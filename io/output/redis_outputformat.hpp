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
#include <array>
#include <map>

#include <sys/time.h>
#include <utility>

#include "hiredis/hiredis.h"
#include "hiredis/adapters/libevent.h"

#include "io/output/outputformat_base.hpp"
#include "io/input/redis_split.hpp"
#include "core/worker_info.hpp"
#include "core/context.hpp"
#include "core/constants.hpp"
#include "base/serialization.hpp"

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
    RedisOutputFormat(int number_clients = 10, int flush_buffer_size = 102400);
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
    const uint16_t crc16tab_[256]= {
        0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
        0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
        0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
        0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
        0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
        0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
        0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
        0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
        0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
        0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
        0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
        0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
        0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
        0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
        0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
        0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
        0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
        0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
        0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
        0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
        0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
        0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
        0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
        0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
        0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
        0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
        0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
        0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
        0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
        0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
        0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
        0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
    };
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
    short int test_short;
    int test_int;
    long int test_long;
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
