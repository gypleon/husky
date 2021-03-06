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

#include <map>
#include <string>
#include <vector>

#include "base/log.hpp"
#include "base/serialization.hpp"

namespace husky {
namespace io {

// RedisSplit
RedisSplit::RedisSplit() : is_valid_(false), slots_start_(-1), slots_end_(-1) {}

RedisSplit::RedisSplit(const RedisSplit& other) {
    is_valid_ = other.is_valid_;
    sn_ = other.sn_;
    id_ = other.id_;
    ip_ = other.ip_;
    master_ = other.master_;
    port_ = other.port_;
    slots_start_ = other.slots_start_;
    slots_end_ = other.slots_end_;
}

void RedisSplit::set_valid(bool valid) { is_valid_ = valid; }
void RedisSplit::set_sn(int sn) { sn_ = sn; }
void RedisSplit::set_id(const std::string& id) { id_ = id; }
void RedisSplit::set_ip(const std::string& ip) { ip_ = ip; }
void RedisSplit::set_master(const std::string& master) { master_ = master; }

void RedisSplit::set_port(int port) { port_ = port; }
void RedisSplit::set_sstart(int start) { slots_start_ = start; }
void RedisSplit::set_send(int end) { slots_end_ = end; }

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

void RedisSplitGroup::sort_members() {
    sorted_members_.clear();
    int lowest_priority = members_.size();
    for ( int higher_priority = 0; higher_priority < lowest_priority; higher_priority++ ) {
        for ( auto& member : members_ ) {
            if ( get_priority(member) == higher_priority ) {
                sorted_members_.push_back(member);
                // no identical priority
                break;
            }
        }
    }
}

int RedisSplitGroup::get_priority(std::string member_id, bool if_balance) {
    int priority = priority_[member_id];
    if (if_balance) {
        update_priority();
    }
    return priority;
}

int RedisSplitGroup::get_num_members() {
    assert(members_.size() == priority_.size());
    return members_.size();
}

const std::vector<std::string>& RedisSplitGroup::get_sorted_members() {
    return sorted_members_;
}

// BinStream series
// for RedisOutputFormat::redis_masters_info
BinStream& operator<<(BinStream& stream, const RedisSplit& split) {
    stream << split.is_valid();
    stream << split.get_sn();
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
    int sn;
    std::string id;
    std::string ip;
    int port;
    std::string master;
    int slots_start;
    int slots_end;
    stream >> is_valid;
    stream >> sn;
    stream >> id;
    stream >> ip;
    stream >> port;
    stream >> master;
    stream >> slots_start;
    stream >> slots_end;
    split.set_valid(is_valid);
    split.set_sn(sn);
    split.set_id(id);
    split.set_ip(ip);
    split.set_port(port);
    split.set_master(master);
    split.set_sstart(slots_start);
    split.set_send(slots_end);
    return stream;
}

BinStream& operator<<(BinStream& stream, const RedisRangeKey& key) {
    stream << key.str_ << key.start_ << key.end_;
    return stream;
}

BinStream& operator>>(BinStream& stream, RedisRangeKey& key) {
    stream >> key.str_ >> key.start_ >> key.end_;
    return stream;
}

}  // namespace io
}  // namespace husky
