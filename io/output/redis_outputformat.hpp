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

#include <array>
#include <map>
#include <string>
#include <sys/time.h>
#include <utility>
#include <vector>

// #include "hiredis/adapters/libevent.h"
#include "hiredis/hiredis.h"

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/worker_info.hpp"
#include "io/input/redis_split.hpp"
#include "io/output/outputformat_base.hpp"

#define redisCmd(context, ...) static_cast<redisReply*>(redisCommand(context, __VA_ARGS__))

namespace husky {
namespace io {

class RedisOutputFormat final : public OutputFormatBase {
private:
    typedef enum DataType {
        RedisString,
        RedisList,
        RedisHash,
        RedisSet,
        RedisZSet
    } DataType;
    typedef enum InnerDataType {
        Other,
        Char,
        Short,
        Int,
        Long,
        Bool,
        Float,
        Double,
        String
    } InnerDataType;

public:
    explicit RedisOutputFormat(int number_clients = 10, int flush_buffer_size = 102400);
    ~RedisOutputFormat();
    virtual bool is_setup() const;

    void set_auth(const std::string& password);
    void set_server();

    inline bool commit(const std::string& key, const std::string& result_string);
    template <class DataT>
    inline bool commit(const std::string& key, const std::vector<DataT>& result_list);
    template <class DataT>
    inline bool commit(const std::string& key, const std::map<std::string, DataT>& result_hash);
    int flush_all();

private:
    void ask_redis_masters_info();
    void create_redis_con_pool();

    uint16_t gen_slot_crc16(const char *buf, int len);
    std::string parse_host(const std::string& hostname);
    template <class DataT>
    inline int get_template_type(DataT sample);

private:
    int num_split_groups_;
    int num_slots_per_group_;
    std::vector<std::string> sorted_split_group_name_;

    bool need_auth_ = false;
    std::string password_;
    // mixed-type data waited to be flushed
    std::map<std::string, std::pair<DataType, BinStream> > records_map_;
    struct timeval timeout_ = {1, 500000};
    int records_bytes_ = 0;
    // number of connections for each redis master
    int number_clients_;
    int flush_buffer_size_;
    std::map<std::string, std::pair<redisContext *, int> > cons_;
    std::map<std::string, RedisSplit> splits_;
};

// commit template implementation
bool RedisOutputFormat::commit(const std::string& key, const std::string& result_string) {
    if (!is_setup())
        return false;
    if (result_string.empty())
        return false;

    const RedisOutputFormat::DataType data_type = RedisOutputFormat::DataType::RedisString;
    BinStream result_stream;
    result_stream << result_string;

    records_map_[key] = std::pair<DataType, BinStream>(data_type, result_stream);
    records_bytes_ += result_string.length();

    if (records_bytes_ >= flush_buffer_size_) {
        flush_all();
        return true;
    } else {
        return false;
    }
}

template <class DataT>
bool RedisOutputFormat::commit(const std::string& key, const std::vector<DataT>& result_list) {
    if (!is_setup())
        return false;
    if (result_list.empty())
        return false;

    RedisOutputFormat::DataType data_type = RedisOutputFormat::DataType::RedisList;
    BinStream result_stream;
    int inner_data_type = get_template_type(result_list[0]);
    result_stream << inner_data_type;
    result_stream << result_list;
    const std::string result_stream_buffer = result_stream.to_string();

    records_map_[key] = std::pair<DataType, BinStream>(data_type, result_stream);
    records_bytes_ += result_stream_buffer.length();

    if (records_bytes_ >= flush_buffer_size_) {
        flush_all();
        return true;
    } else {
        return false;
    }
}

template <class DataT>
bool RedisOutputFormat::commit(const std::string& key, const std::map<std::string, DataT>& result_hash) {
    if (!is_setup())
        return false;
    if (result_hash.empty())
        return false;

    RedisOutputFormat::DataType data_type = RedisOutputFormat::DataType::RedisHash;
    BinStream result_stream;
    int inner_data_type = get_template_type(result_hash.begin()->second);
    result_stream << inner_data_type;
    result_stream << result_hash;
    const std::string result_stream_buffer = result_stream.to_string();

    records_map_[key] = std::pair<DataType, BinStream>(data_type, result_stream);
    records_bytes_ += result_stream_buffer.length();

    if (records_bytes_ >= flush_buffer_size_) {
        flush_all();
        return true;
    } else {
        return false;
    }
}

template <class DataT>
int RedisOutputFormat::get_template_type(DataT sample) {
    const char * sample_type = typeid(sample).name();
    char test_char;
    int16_t test_short;
    int test_int;
    int64_t test_long;
    bool test_bool;
    float test_float;
    double test_double;
    std::string test_string;
    if (!strcmp(typeid(test_string).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::String;
    } else if (!strcmp(typeid(test_short).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Short;
    } else if (!strcmp(typeid(test_int).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Int;
    } else if (!strcmp(typeid(test_long).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Long;
    } else if (!strcmp(typeid(test_bool).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Bool;
    } else if (!strcmp(typeid(test_float).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Float;
    } else if (!strcmp(typeid(test_double).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Double;
    } else if (!strcmp(typeid(test_char).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Char;
    } else {
        return RedisOutputFormat::InnerDataType::Other;
    }
}

}  // namespace io
}  // namespace husky
