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

#include <string>
#include <vector>
#include <sys/time.h>
#include <utility>

#include "hiredis/hiredis.h"

#include "io/input/inputformat_base.hpp"
#include "io/input/redis_split.hpp"

#define ITER_STEP 1

#define redisCmd(context, ...) static_cast<redisReply*>(redisCommand(context, __VA_ARGS__))

namespace husky {
namespace io {

class RedisInputFormat final : public InputFormatBase {
public:
    typedef std::pair<std::string, std::string> RecordT;
public:
    RedisInputFormat();
    virtual ~RedisInputFormat();
    
    virtual bool is_setup() const;
    virtual bool next(RecordT& ref);

    void set_auth(const std::string&);
    void reset_auth();

    bool ask_best_keys();
    void fetch_split_records(const RedisSplit& split, const std::vector<RedisRangeKey>& keys);
    void send_end(RedisBestKeys& best_keys);

protected:
    std::string ip_;
    int port_;
    struct timeval timeout_ = { 1, 500000};

    bool need_auth_ = false;
    std::string password_;

    int is_setup_ = 0;

    std::vector<RecordT> records_vector_;
    bool if_pop_record_ = false;
    RedisBestKeys best_keys_;

    std::map<std::string, redisContext *> cons_;
};

}  // namespace io
}  // namespace husky
