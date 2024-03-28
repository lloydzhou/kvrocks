/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <algorithm>

#include "command_parser.h"
#include "commander.h"
#include "error_constants.h"
#include "event_util.h"
#include "server/server.h"
#include "time_util.h"
#include "types/redis_stream.h"
#include "types/redis_zset.h"
#include "types/redis_set.h"
#include "types/redis_hash.h"
#include "cmd_stream.cc"


namespace redis {

class CommandIMRecive: public CommandXRead {
 public:
  Status Parse(const std::vector<std::string> &args) {
    std::string stream_name_;
    stream_name_.append("s:");
    stream_name_.append(args[1]);
    streams_.push_back(stream_name_);

    for (size_t i = 2; i < args.size();) {
      auto arg = util::ToLower(args[i]);

      if (arg == "start") {
        const auto &id_str = args[i + 1];
        StreamEntryID id;
        bool get_latest = id_str == "$";
        if (!get_latest) {
          auto s = ParseStreamEntryID(id_str, &id);
          if (!s.IsOK()) return s;
        }
        ids_.push_back(id);
        latest_marks_.push_back(get_latest);
        ids_.push_back(id);
      }

      if (arg == "count") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        with_count_ = true;

        auto parse_result = ParseInt<uint64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
        i += 2;
        continue;
      }

      if (arg == "block") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        block_ = true;

        auto parse_result = ParseInt<int64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        if (*parse_result < 0) {
          return {Status::RedisParseErr, errTimeoutIsNegative};
        }

        block_timeout_ = *parse_result;
        i += 2;
        continue;
      }

      ++i;
    }
    if (ids_.empty()) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    return Status::OK();
  }
};

class CommandIMSend : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    user_id_ = args[1];
    to_user_id_ = args[2];
    for (size_t i = 3; i < args.size(); ++i) {
      name_value_pairs_.push_back(args[i]);
    }

    if (name_value_pairs_.empty() || name_value_pairs_.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    return ExecuteSend(srv, conn, output);
  }

  Status ExecuteSend(Server *srv, Connection *conn, std::string *output) {
    std::unique_ptr<redis::NextStreamEntryIDGenerationStrategy> next_id_strategy_;
    {
      auto result = ParseNextStreamEntryIDStrategy("*");
      next_id_strategy_ = std::move(*result);
    }
    redis::StreamAddOptions options;
    options.nomkstream = false;
    options.next_id_strategy = std::move(next_id_strategy_);

    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    StreamEntryID entry_id;
    std::string s_to_user_id;
    s_to_user_id.append("s:");
    s_to_user_id.append(to_user_id_);
    name_value_pairs_.push_back("uid");
    name_value_pairs_.push_back(user_id_);
    // XADD s_to_user_id * uid user_id unpack(ARGV)
    auto s = stream_db.Add(s_to_user_id, options, name_value_pairs_, &entry_id);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
      return Status::OK();
    }
    // XADD s_user_id mid FW s_to_user_id
    StreamEntryID entry_id_fw;
    std::string s_user_id;
    s_user_id.append("s:");
    s_user_id.append(user_id_);
    std::vector<std::string> name_value_pairs_fw_;
    name_value_pairs_fw_.push_back("FW");
    name_value_pairs_fw_.push_back(s_to_user_id);
    {
      redis::StreamAddOptions options;
      options.nomkstream = false;
      options.next_id_strategy = std::make_unique<FullySpecifiedEntryID>(entry_id);
      auto s = stream_db.Add(s_user_id, options, name_value_pairs_fw_, &entry_id_fw);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = conn->NilString();
        return Status::OK();
      }
    }
    // ZADD r_to_user_id incr 1 s_user_id
    {
      uint64_t ret = 0;
      float score = 1.0;
      ZAddFlags flags_{kZSetIncr};
      std::string r_to_user_id;
      std::vector<MemberScore> member_scores_;
      r_to_user_id.append("r:");
      r_to_user_id.append(to_user_id_);
      member_scores_.emplace_back(MemberScore{s_user_id, score});
      auto s = zset_db.Add(r_to_user_id, flags_, &member_scores_, &ret);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
      srv->WakeupBlockingConns(r_to_user_id, member_scores_.size());
    }

    *output = redis::BulkString(entry_id.ToString());

    srv->OnEntryAddedToStream(conn->GetNamespace(), s_to_user_id, entry_id);
    srv->OnEntryAddedToStream(conn->GetNamespace(), s_user_id, entry_id_fw);

    return Status::OK();
  }

 protected:
  std::string user_id_;
  std::string to_user_id_;
  std::vector<std::string> name_value_pairs_;
};


class CommandIMGroupSend : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    user_id_ = args[1];
    to_group_id_ = args[2];
    for (size_t i = 3; i < args.size(); ++i) {
      name_value_pairs_.push_back(args[i]);
    }

    if (name_value_pairs_.empty() || name_value_pairs_.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    return ExecuteSend(srv, conn, output);
  }

  Status ExecuteSend(Server *srv, Connection *conn, std::string *output) {
    std::unique_ptr<redis::NextStreamEntryIDGenerationStrategy> next_id_strategy_;
    {
      auto result = ParseNextStreamEntryIDStrategy("*");
      next_id_strategy_ = std::move(*result);
    }
    redis::StreamAddOptions options;
    options.nomkstream = false;
    options.next_id_strategy = std::move(next_id_strategy_);

    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    redis::Set set_db(srv->storage, conn->GetNamespace());

    StreamEntryID entry_id;
    std::string s_to_group_id;
    s_to_group_id.append("gs:");
    s_to_group_id.append(to_group_id_);
    name_value_pairs_.push_back("uid");
    name_value_pairs_.push_back(user_id_);
    // XADD s_to_group_id * uid user_id unpack(ARGV)
    auto s = stream_db.Add(s_to_group_id, options, name_value_pairs_, &entry_id);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
      return Status::OK();
    }
    srv->OnEntryAddedToStream(conn->GetNamespace(), s_to_group_id, entry_id);

    // SMEMBERS m_group_id
    {
      std::vector<std::string> members;
      std::string m_group_id;
      m_group_id.append("gm:");
      m_group_id.append(to_group_id_);
      auto s = set_db.Members(m_group_id, &members);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      // xadd params
      std::vector<std::string> name_value_pairs_fw_;
      name_value_pairs_fw_.push_back("FW");
      name_value_pairs_fw_.push_back(s_to_group_id);
      redis::StreamAddOptions options;
      options.nomkstream = false;
      options.next_id_strategy = std::make_unique<FullySpecifiedEntryID>(entry_id);
      // zset params
      float score = 1.0;
      ZAddFlags flags_{kZSetIncr};
      std::vector<MemberScore> member_scores_;
      member_scores_.emplace_back(MemberScore{s_to_group_id, score});
      for (const auto &member : members) {
        // XADD s_member_id mid FW s_to_group_id
        std::string s_member_id;
        s_member_id.append("s:");
        s_member_id.append(member);
        StreamEntryID entry_id_fw;
        auto s = stream_db.Add(s_member_id, options, name_value_pairs_fw_, &entry_id_fw);
        if (!s.ok() && !s.IsNotFound()) {
          return {Status::RedisExecErr, s.ToString()};
        }
        srv->OnEntryAddedToStream(conn->GetNamespace(), s_member_id, entry_id_fw);
        // ZADD r_member_id s_to_group_id
        if (user_id_ != member) {
          uint64_t ret = 0;
          std::string r_member_id;
          r_member_id.append("r:");
          r_member_id.append(member);
          auto s = zset_db.Add(r_member_id, flags_, &member_scores_, &ret);
          if (!s.ok()) {
            return {Status::RedisExecErr, s.ToString()};
          }
          srv->WakeupBlockingConns(r_member_id, member_scores_.size());
        }
      }
    }

    *output = redis::BulkString(entry_id.ToString());

    return Status::OK();
  }

 protected:
  std::string user_id_;
  std::string to_group_id_;
  std::vector<std::string> name_value_pairs_;
};


class CommandIMMessage : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // im.message GROUP/USER group_id/user_id message_id
    if ("group" == util::ToLower(args[1])) {
      stream_name_.append("gs:");
    } else {
      stream_name_.append("s:");
    }
    stream_name_.append(args[2]);
    {
      auto s = ParseRangeStart(args[3], &start_);
      if (!s.IsOK()) return s;
    }
    {
      auto s = ParseRangeEnd(args[3], &end_);
      if (!s.IsOK()) return s;
    }
    LOG(INFO) << "stream_name " << stream_name_;
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    redis::StreamRangeOptions options;
    options.reverse = false;
    options.start = start_;
    options.end = end_;
    options.with_count = true;
    options.count = 1;
    options.exclude_start = false;
    options.exclude_end = false;

    std::vector<StreamEntry> result;
    auto s = stream_db.Range(stream_name_, options, &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (result.empty()) {
      *output = conn->NilString();
      return Status::OK();
    }

    StreamEntry entry = result.front(); 
    output->append(redis::MultiLen(2));
    output->append(redis::BulkString(entry.key));
    output->append(conn->MultiBulkString(entry.values));
    return Status::OK();
  }
 private:
  std::string stream_name_;
  StreamEntryID start_;
  StreamEntryID end_;
};

class CommandIMGroup : public CommandIMGroupSend {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // im.group group_id
    // im.group group_id user_id field value
    to_group_id_ = args[1];
    if (args.size() > 2) {
      user_id_ = args[2];
      if (args.size() % 2 != 1) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
      for (size_t i = 3; i < args_.size(); i += 2) {
        field_values_.emplace_back(args_[i], args_[i + 1]);
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    std::string m_group_id;
    std::string i_group_id;
    m_group_id.append("gm:");
    m_group_id.append(to_group_id_);
    i_group_id.append("gi:");
    i_group_id.append(to_group_id_);
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    redis::Set set_db(srv->storage, conn->GetNamespace());
    if (args_.size() > 2) {
      field_values_.emplace_back("master", user_id_);
      // HMSET i_group_id master user_id unpack(ARGV)
      auto s = hash_db.MSet(i_group_id, field_values_, true, &ret);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      *output = redis::Integer(ret);
      // ZADD c_user_id timestamp to_group_id
      {
        float score = (float)util::GetTimeStampMS();
        ZAddFlags flags_{0};
        std::string c_user_id;
        std::vector<MemberScore> member_scores_;

        c_user_id.append("c:");
        c_user_id.append(user_id_);
        member_scores_.emplace_back(MemberScore{to_group_id_, score});
        auto s = zset_db.Add(c_user_id, flags_, &member_scores_, &ret);
        if (!s.ok()) {
          return {Status::RedisExecErr, s.ToString()};
        }
        srv->WakeupBlockingConns(c_user_id, member_scores_.size());
      }
      // SADD m_group_id user_id
      {
        uint64_t ret = 0;
        std::vector<Slice> members;
        members.emplace_back(user_id_);
        auto s = set_db.Add(m_group_id, members, &ret);
        if (!s.ok()) {
          return {Status::RedisExecErr, s.ToString()};
        }
      }
      // send join group message(only for current_user)
      std::vector<std::string> name_value_pairs_;
      name_value_pairs_.push_back("action");
      name_value_pairs_.push_back("join");
      name_value_pairs_.push_back("uid");
      name_value_pairs_.push_back(user_id_);
      name_value_pairs_.push_back("gid");
      name_value_pairs_.push_back(to_group_id_);
      auto status = ExecuteSend(srv, conn, output);
      if (status) {
        *output = redis::Integer(ret);
      }
      return status;
    } else {
      // HGETALL i_group_id
      std::vector<FieldValue> field_values;
      {
        auto s = hash_db.GetAll(i_group_id, &field_values);
        if (!s.ok()) {
          return {Status::RedisExecErr, s.ToString()};
        }
      }
      // SMSMBERS m_group_id
      std::vector<std::string> members;
      std::string m_group_id;
      m_group_id.append("gm:");
      m_group_id.append(to_group_id_);
      {
        auto s = set_db.Members(m_group_id, &members);
        if (!s.ok()) {
          return {Status::RedisExecErr, s.ToString()};
        }
      }
      // output
      std::vector<std::string> kv_pairs;
      kv_pairs.reserve(field_values.size());
      for (const auto &p : field_values) {
        kv_pairs.emplace_back(p.field);
        kv_pairs.emplace_back(p.value);
      }
      output->append(redis::MultiLen(2));
      output->append(conn->MapOfBulkStrings(kv_pairs));
      output->append(conn->SetOfBulkStrings(members));
      return Status::OK();
    }
  }
 private:
  std::vector<FieldValue> field_values_;
};

class CommandIMJoin : public CommandIMGroupSend {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // im.join user_id to_group_id_
    user_id_ = args[1];
    to_group_id_ = args[2];
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    // ZADD c_user_id timestamp to_group_id_
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    redis::Set set_db(srv->storage, conn->GetNamespace());
    uint64_t ret = 0;
    {
      float score = (float)util::GetTimeStampMS();
      ZAddFlags flags_{0};
      std::string c_user_id;
      std::vector<MemberScore> member_scores_;

      c_user_id.append("c:");
      c_user_id.append(user_id_);
      member_scores_.emplace_back(MemberScore{to_group_id_, score});
      auto s = zset_db.Add(c_user_id, flags_, &member_scores_, &ret);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
      srv->WakeupBlockingConns(c_user_id, member_scores_.size());
    }
    // SADD m_group_id user_id_
    {
      uint64_t ret = 0;
      std::string m_group_id;
      m_group_id.append("gm:");
      m_group_id.append(to_group_id_);
      std::vector<Slice> members;
      members.emplace_back(user_id_);
      auto s = set_db.Add(m_group_id, members, &ret);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }

    // send join group message
    name_value_pairs_.push_back("action");
    name_value_pairs_.push_back("join");
    name_value_pairs_.push_back("uid");
    name_value_pairs_.push_back(user_id_);
    name_value_pairs_.push_back("gid");
    name_value_pairs_.push_back(to_group_id_);
    auto status = ExecuteSend(srv, conn, output);
    if (status) {
      *output = redis::Integer(ret);
    }
    return status;
  }
};

class CommandIMQuit : public CommandIMGroupSend {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // im.link user_id to_user_id
    user_id_ = args[1];
    to_group_id_ = args[2];
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    redis::Set set_db(srv->storage, conn->GetNamespace());
    uint64_t size = 0;
    // ZREM c_user_id group_id 
    {
      std::string c_user_id;
      c_user_id.append("c:");
      c_user_id.append(user_id_);
      std::vector<rocksdb::Slice> members;
      members.emplace_back(to_group_id_);
      auto s = zset_db.Remove(c_user_id, members, &size);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }
    // ZREM r_user_id s_to_group_id 
    {
      std::string r_user_id;
      std::string s_to_group_id;
      r_user_id.append("r:");
      r_user_id.append(user_id_);
      s_to_group_id.append("gs:");
      s_to_group_id.append(to_group_id_);
      std::vector<rocksdb::Slice> members;
      members.emplace_back(s_to_group_id);
      auto s = zset_db.Remove(r_user_id, members, &size);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }
    // SREM m_group_id user_id_
    {
      std::string m_group_id;
      m_group_id.append("gm:");
      m_group_id.append(to_group_id_);
      std::vector<rocksdb::Slice> members;
      members.emplace_back(user_id_);
      auto s = set_db.Remove(m_group_id, members, &size);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }
    // send quit group message
    name_value_pairs_.push_back("action");
    name_value_pairs_.push_back("quit");
    name_value_pairs_.push_back("uid");
    name_value_pairs_.push_back(user_id_);
    name_value_pairs_.push_back("gid");
    name_value_pairs_.push_back(to_group_id_);
    auto status = ExecuteSend(srv, conn, output);
    if (status) {
      *output = redis::Integer(size);
    }
    return status;
  }
};

class CommandIMUser : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // im.user user_id field value
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    if (!field_values_.empty()) {
      auto s = hash_db.MSet(args_[1], field_values_, true, &ret);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      *output = redis::Integer(ret);
      return Status::OK();
    } else {
      std::vector<FieldValue> field_values;
      auto s = hash_db.GetAll(args_[1], &field_values);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      std::vector<std::string> kv_pairs;
      kv_pairs.reserve(field_values.size());
      for (const auto &p : field_values) {
        kv_pairs.emplace_back(p.field);
        kv_pairs.emplace_back(p.value);
      }
      *output = conn->MapOfBulkStrings(kv_pairs);

      return Status::OK();
    }
  }
 private:
  std::vector<FieldValue> field_values_;
};

class CommandIMLink : public CommandIMSend {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // im.link user_id to_user_id
    user_id_ = args[1];
    to_user_id_ = args[2];
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    // ZADD c_user_id timestamp to_user_id
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    uint64_t ret = 0;
    {
      float score = (float)util::GetTimeStampMS();
      ZAddFlags flags_{0};
      std::string c_user_id;
      std::vector<MemberScore> member_scores_;

      c_user_id.append("c:");
      c_user_id.append(user_id_);
      member_scores_.emplace_back(MemberScore{to_user_id_, score});
      auto s = zset_db.Add(c_user_id, flags_, &member_scores_, &ret);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
      srv->WakeupBlockingConns(c_user_id, member_scores_.size());
      // LOG(INFO) << "success run zadd " << c_user_id << "  " << s_to_user_id;
    }

    // send link user message
    name_value_pairs_.push_back("action");
    name_value_pairs_.push_back("link");
    name_value_pairs_.push_back("uid");
    name_value_pairs_.push_back(user_id_);
    name_value_pairs_.push_back("tuid");
    name_value_pairs_.push_back(to_user_id_);
    auto status = ExecuteSend(srv, conn, output);
    if (status) {
      *output = redis::Integer(ret);
    }
    return status;
  }
};

class CommandIMUnLink : public CommandIMSend {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    // im.link user_id to_user_id
    user_id_ = args[1];
    to_user_id_ = args[2];
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    uint64_t size = 0;
    // ZREM r_user_id s_to_user_id
    {
      std::string r_user_id;
      std::string s_to_user_id;
      r_user_id.append("r:");
      r_user_id.append(user_id_);
      s_to_user_id.append("s:");
      s_to_user_id.append(to_user_id_);
      std::vector<rocksdb::Slice> members;
      members.emplace_back(s_to_user_id);
      auto s = zset_db.Remove(r_user_id, members, &size);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }
    // ZREM c_user_id to_user_id
    {
      std::string c_user_id;
      c_user_id.append("c:");
      c_user_id.append(user_id_);
      std::vector<rocksdb::Slice> members;
      members.emplace_back(to_user_id_);
      auto s = zset_db.Remove(c_user_id, members, &size);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }
    *output = redis::Integer(size);
    return Status::OK();
  }
};


REDIS_REGISTER_COMMANDS(
  MakeCmdAttr<CommandIMRecive>("im.recive", -2, "read-only", 0, 0, 0),
  MakeCmdAttr<CommandIMSend>("im.send", -3, "write", 1, 1, 1),
  MakeCmdAttr<CommandIMGroupSend>("im.gsend", -3, "write", 1, 1, 1),
  MakeCmdAttr<CommandIMMessage>("im.message", 4, "read-only", 1, 1, 1),
  MakeCmdAttr<CommandIMGroup>("im.group", -2, "write", 1, 1, 1),
  MakeCmdAttr<CommandIMJoin>("im.join", 3, "write", 1, 1, 1),
  MakeCmdAttr<CommandIMQuit>("im.quit", 3, "write", 1, 1, 1),
  MakeCmdAttr<CommandIMUser>("im.user", -2, "write", 1, 1, 1),
  MakeCmdAttr<CommandIMLink>("im.link", 3, "write", 1, 1, 1),
  MakeCmdAttr<CommandIMUnLink>("im.unlink", 3, "write", 1, 1, 1)
);

}  // namespace redis
