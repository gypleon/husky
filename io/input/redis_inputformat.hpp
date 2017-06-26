// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <map>
#include <string>
#include <sys/time.h>
#include <utility>
#include <vector>

#include "hiredis/hiredis.h"

#include "io/input/inputformat_base.hpp"
#include "io/input/redis_split.hpp"

#define ITER_STEP 1
#define SEP '/'

#define redisCmd(context, ...) static_cast<redisReply*>(redisCommand(context, __VA_ARGS__))

namespace husky {
namespace io {

class RedisInputFormat final : public InputFormatBase {
public:
    typedef enum RedisDataType {
        String,
        List,
        Hash,
        Set,
        ZSet,
        Null
    } RedisDataType;
    typedef std::pair<RedisDataType, std::string> RecordT;

public:
    RedisInputFormat();
    virtual ~RedisInputFormat();

    virtual bool is_setup() const;
    virtual bool next(RecordT& ref);

    void set_server();
    void set_auth(const std::string&);
    void reset_auth();

private:
    void ask_redis_splits_info();
    void create_redis_cons();
    int ask_best_keys();
    void fetch_split_records(int split_i, const std::vector<RedisRangeKey>& keys);
    void send_end();

    uint16_t gen_slot_crc16(const char *buf, int len);
    std::string parse_host(const std::string& hostname);

private:
    std::string ip_;
    int port_;
    struct timeval timeout_ = {1, 500000};
    bool need_auth_ = false;
    std::string password_;
    std::map<std::string, redisContext *> cons_;
    std::map<std::string, RedisSplit> splits_;
    std::vector<std::string> split_i_id_;

    int is_setup_ = 0;

    std::vector<RecordT> records_vector_;
    bool if_pop_record_ = false;
    bool if_all_assigned_ = false;
    std::vector<std::vector<RedisRangeKey> > best_keys_;
};

}  // namespace io
}  // namespace husky
