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

#include "io/input/redis_split.hpp"

#include <string>
#include <map>

#include "base/serialization.hpp"
#include "base/log.hpp"

namespace husky {
namespace io {

// RedisSplit
RedisSplit::RedisSplit() : is_valid_(false), slots_start_(-1), slots_end_(-1) {}

RedisSplit::RedisSplit(const RedisSplit& other) {
    is_valid_ = other.is_valid_;
    id_ = other.id_;
    ip_ = other.ip_;
    master_ = other.master_;
    port_ = other.port_;
    slots_start_ = other.slots_start_;
    slots_end_ = other.slots_end_;
}

void RedisSplit::set_valid(bool valid) { is_valid_ = valid; }
void RedisSplit::set_id(const std::string& id) { id_ = id; }
void RedisSplit::set_ip(const std::string& ip) { ip_ = ip; }
void RedisSplit::set_master(const std::string& master) { master_ = master; }

void RedisSplit::set_port(int port) { port_ = port; }
void RedisSplit::set_sstart(int start) { slots_start_ = start; }
void RedisSplit::set_send(int end) { slots_end_ = end; }

// RedisBestKeys
RedisBestKeys::RedisBestKeys() {
    best_keys_.clear();
}

RedisBestKeys::RedisBestKeys(const RedisBestKeys& other) {
    best_keys_ = other.best_keys_;
}

RedisBestKeys::~RedisBestKeys() {
    best_keys_.clear();
}

void RedisBestKeys::add_key(RedisSplit rs, RedisRangeKey key) {
    best_keys_[rs].push_back(key);
}

void RedisBestKeys::set_keys(std::map<RedisSplit, std::vector<RedisRangeKey> > best_keys) {
    best_keys_ = best_keys;
}

const std::map<RedisSplit, std::vector<RedisRangeKey> >& RedisBestKeys::get_keys() {
    return best_keys_;
}

int RedisBestKeys::del_split(const RedisSplit& split) {
    int pre_size = best_keys_.size();
    auto it = best_keys_.find(split);
    best_keys_.erase(it);
    return best_keys_.size() - pre_size;
}

// RedisSplitGroup
RedisSplitGroup::RedisSplitGroup(RedisSplit master) { 
    add_member(master.get_id());
}

RedisSplitGroup::~RedisSplitGroup() { 
    priority_.clear();
    sorted_members_.clear();
    members_.clear(); 
}

void RedisSplitGroup::add_member(std::string split_id) { 
    priority_[split_id] = members_.size();
    members_.push_back(split_id); 
}

void RedisSplitGroup::update_priority() {
    assert(members_.size() == priority_.size());
    for ( auto& pr : priority_ ) {
        pr.second = (pr.second + 1) % members_.size();
    }
}

int RedisSplitGroup::get_priority(std::string member_id, bool if_balance) { 
    int priority = priority_[member_id];
    if ( if_balance) {
        update_priority();
    }
    return priority; 
}

int RedisSplitGroup::get_num_members() { 
    assert(members_.size() == priority_.size());
    return members_.size(); 
}

const std::vector<std::string>& RedisSplitGroup::get_sorted_members() {
    sorted_members_.clear();
    int cur_priority = members_.size();
    while ( cur_priority > 0 ) {
        for ( auto& member : members_ ) {
            if ( get_priority(member) == cur_priority-1 ) {
                sorted_members_.insert(sorted_members_.begin(), member);
                cur_priority--;
            }
        }
    }
    return sorted_members_;
}

// BinStream series
BinStream& operator<<(BinStream& stream, RedisSplit& split) {
    stream << split.is_valid();
    stream << split.get_id();
    stream << split.get_ip();
    stream << split.get_port();
    stream << split.get_master();
    stream << split.get_sstart();
    stream << split.get_send();
    return stream;
}

BinStream& operator>>(BinStream& stream, RedisSplit& split) {
    bool is_valid;
    std::string id;
    std::string ip;
    int port;
    std::string master;
    int slots_start;
    int slots_end;
    stream >> is_valid;
    stream >> id;
    stream >> ip;
    stream >> port;
    stream >> master;
    stream >> slots_start;
    stream >> slots_end;
    split.set_valid(is_valid);
    split.set_id(id);
    split.set_ip(ip);
    split.set_port(port);
    split.set_master(master);
    split.set_sstart(slots_start);
    split.set_send(slots_end);
    return stream;
}

BinStream& operator<<(BinStream& stream, RedisBestKeys& keys) {
    std::map<RedisSplit, std::vector<RedisRangeKey> > best_keys = keys.get_keys();
    // stream << best_keys;
    stream << best_keys.size();
    RedisSplit split;
    for (auto& b_keys : best_keys){
        split = b_keys.first;
        stream << split;
        stream << b_keys.second.size();
        for (auto& key : b_keys.second){
            stream << key;
        }
    }
    return stream;
}

BinStream& operator>>(BinStream& stream, RedisBestKeys& keys) {
    std::map<RedisSplit, std::vector<RedisRangeKey> > best_keys;
    // stream >> best_keys;
    size_t num_splits;
    RedisSplit b_keys;
    stream >> num_splits;
    for ( int i=0; i<num_splits; i++){
        stream >> b_keys;
        size_t num_keys;
        stream >> num_keys;
        for ( int j=0; j<num_keys; j++){
            RedisRangeKey key;
            stream >> key;
            best_keys[b_keys].push_back(key);
        }
    }
    keys.set_keys(best_keys);
    return stream;
}

BinStream& operator<<(BinStream& stream, RedisRangeKey& key) {
    stream << key.str_ << key.start_ << key.end_;
    return stream;
}

BinStream& operator>>(BinStream& stream, RedisRangeKey& key) {
    stream >> key.str_ >> key.start_ >> key.end_;
    return stream;
}

}  // namespace io
}  // namespace husky
