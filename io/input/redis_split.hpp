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
#include <map>

#include <assert.h>

#include "base/serialization.hpp"

namespace husky {
namespace io {

using base::BinStream;

struct RedisRangeKey;

class RedisSplit {
    public:
        RedisSplit();
        RedisSplit(const RedisSplit& other);

        // Set-functions
        void set_valid(bool valid);

        void set_id(const std::string& id);
        void set_ip(const std::string& ip);
        void set_master(const std::string& master);

        void set_port(int port);
        void set_sstart(int start);
        void set_send(int end);

        // Get-functions
        inline const std::string& get_id() const { return id_; }
        inline const std::string& get_ip() const { return ip_; }
        inline const std::string& get_master() const { return master_; }

        inline int get_port() const { return port_; }
        inline int get_sstart() const { return slots_start_; }
        inline int get_send() const { return slots_end_; }

        // Miscellaneous
        inline bool is_valid() const { return is_valid_; }

        // For std::map's key
        bool operator<(const RedisSplit &rs)const{
            return slots_start_ < rs.get_sstart();
        }

        RedisSplit& operator=(const RedisSplit &other){
            is_valid_ = other.is_valid_;
            id_ = other.id_;
            ip_ = other.ip_;
            master_ = other.master_;
            port_ = other.port_;
            slots_start_ = other.slots_start_;
            slots_end_ = other.slots_end_;
            return *this;
        }

    private:
        bool is_valid_;

        std::string id_;
        std::string ip_;
        // '-' - master, id - slave
        std::string master_;
        int port_;
        // TODO: slaves slave should also have slots info for load balance
        int slots_start_;
        int slots_end_;
};

class RedisBestKeys {
    public:
        RedisBestKeys();
        RedisBestKeys(const RedisBestKeys& other);
        ~RedisBestKeys();

        // Set / Add
        void add_key(RedisSplit rs, RedisRangeKey key);
        void set_keys(std::map<RedisSplit, std::vector<RedisRangeKey> > best_keys);

        // Get
        const std::map<RedisSplit, std::vector<RedisRangeKey> > &get_keys();

        // Del
        int del_split(const RedisSplit& split);
        void clear() { best_keys_.clear(); }

    private:
        std::map<RedisSplit, std::vector<RedisRangeKey> > best_keys_;
};

// for master-slaves load balance
class RedisSplitGroup {
    public:
        RedisSplitGroup() = default;
        RedisSplitGroup(RedisSplit master);
        ~RedisSplitGroup();

        // Set / Add
        void add_member(std::string split_id);
        void update_priority();

        // Get
        const std::vector<std::string>& get_members() { return members_; }
        int get_priority(std::string member_id, bool if_balance = false); 
        int get_num_members();
        const std::vector<std::string>& get_sorted_members();

    private:
        std::vector<std::string> members_;
        std::vector<std::string> sorted_members_;
        // the smallest value, the highest priority
        std::map<std::string, int> priority_;
};

// for heavy-keys load balance
struct RedisRangeKey {
    std::string str_;
    int start_ = 0;
    int end_ = -1;
};

BinStream& operator<<(BinStream& stream, const RedisSplit& split);
BinStream& operator>>(BinStream& stream, RedisSplit& split);

BinStream& operator<<(BinStream& stream, RedisBestKeys& keys);
BinStream& operator>>(BinStream& stream, RedisBestKeys& keys);

BinStream& operator<<(BinStream& stream, RedisRangeKey& key);
BinStream& operator>>(BinStream& stream, RedisRangeKey& key);

}  // namespace io
}  // namespace husky
