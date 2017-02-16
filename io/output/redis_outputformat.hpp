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
#include "io/output/outputformat_base.hpp"
#include "base/serialization.hpp"

namespace husky {
namespace io {

class RedisOutputFormat final : public OutputFormatBase {
public:
    // typedef enum RedisOutputFormatDatatype{
    //     RedisString,
    //     RedisList,
    //     RedisHash,
    //     RedisSet,
    //     RedisZset,
    //     RedisOther
    // }OutputDatatype;
    typedef std::pair<OutputDatatype, std::string> RecordT;
public:
    RedisOutputFormat();
    ~RedisOutputFormat();
    void set_auth(const std::string& password);
    void set_server(const std::string ip, const std::string port);

    // TODO: BinStream
    bool commit(OutputDatatype datatype, const std::string& jsonstring);
    void flush_all();

protected:
    bool need_auth_ = false;
    std::string password_;
    std::string ip_;
    std::vector<RecordT> records_vector_;
    // for Redis
    int port_;
    struct timeval timeout_ = { 1, 500000};
    int records_bytes_ = 0;
};

}  // namespace io
}  // namespace husky
