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

#ifdef WITH_REDIS

#include "master/redis_assigner.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <map>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/param.h>

#include <chrono>
#include <ctime>

#include "boost/tokenizer.hpp"
#include "hiredis/hiredis.h"

#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/zmq_helpers.hpp"
#include "master/master.hpp"

#define CHECK_REP(X) if ( !X || X->type == REDIS_REPLY_ERROR ) { LOG_E << "Error"; exit(-1); }

namespace husky {

static RedisSplitAssigner redis_split_assigner;

RedisSplitAssigner::RedisSplitAssigner() {
    Master::get_instance().register_main_handler(TYPE_REDIS_REQ,
                                                 std::bind(&RedisSplitAssigner::master_redis_req_handler, this));
    Master::get_instance().register_main_handler(TYPE_REDIS_QRY_REQ,
                                                 std::bind(&RedisSplitAssigner::master_redis_qry_req_handler, this));
    Master::get_instance().register_main_handler(TYPE_REDIS_END_REQ,
                                                 std::bind(&RedisSplitAssigner::master_redis_req_end_handler, this));
    Master::get_instance().register_setup_handler(std::bind(&RedisSplitAssigner::master_setup_handler, this));
}

bool RedisSplitAssigner::load_parameters() {
    if ("" == Context::get_param("redis_hostname") || "" == Context::get_param("redis_port")) {
        LOG_E << "Redis required parameters missed:";
        LOG_E << "  --redis_hostname HOSTNAME";
        LOG_E << "  --redis_port     PORT";
        return false;
    } else {
        ip_ = Context::get_param("redis_hostname");
        port_ = atoi(Context::get_param("redis_port").c_str());
        if (Context::get_param("redis_key_split_threshold").compare("")) {
            key_split_size_ = stoi(Context::get_param("redis_key_split_threshold"));
        } else {
            key_split_size_ = 0;
            LOG_I << "split heavy List: \033[1;32m[OFF]\033[0m";
        }
        if ((keys_path_ = Context::get_param("redis_keys_file")).compare("")) {
            keys_file_.open(keys_path_, std::ios::in);
        } else {
            LOG_I << "load keys from file: \033[1;32m[OFF]\033[0m";
        }
        if (Context::get_param("redis_keys_pattern").compare("")) {
            keys_pattern_ = Context::get_param("redis_keys_pattern");
        } else {
            keys_pattern_ = "";
            LOG_I << "load keys from pattern: \033[1;32m[OFF]\033[0m";
        }
        if (Context::get_param("redis_keys_list").compare("")) {
            keys_list_ = Context::get_param("redis_keys_list");
        } else {
            keys_list_ = "";
            LOG_I << "load keys from list: \033[1;32m[OFF]\033[0m";
        }
        if (Context::get_param("redis_local_latency").compare("")) {
            local_served_latency_ = atoi(Context::get_param("redis_local_latency").c_str());
        } else {
            local_served_latency_ = 100;
            LOG_I << "local latency: \033[1;32m[DEFAULT 100 microseconds]\033[0m";
        }
        if (Context::get_param("redis_non_local_latency").compare("")) {
            non_local_served_latency_ = atoi(Context::get_param("redis_non_local_latency").c_str());
        } else {
            non_local_served_latency_ = 100;
            LOG_I << "remote latency: \033[1;32m[DEFAULT 100 microseconds]\033[0m";
        }
        return true;
    }
}

void RedisSplitAssigner::master_redis_qry_req_handler() {
    auto& master = Master::get_instance();
    auto master_socket = master.get_socket();
    BinStream stream = zmq_recv_binstream(master_socket.get());

    int query_type = -1;
    stream >> query_type;

    stream.clear();

    std::map<std::string, RedisSplit> redis_splits_info;
    switch (query_type) {
        case 0:
            {
                answer_masters_info(redis_splits_info);
                break;
            }
        case 1:
            {
                answer_splits_info(redis_splits_info);
                break;
            }
        default:
            {
                LOG_E << "Undefined query type: " << query_type;
                break;
            }
    }
    stream << redis_splits_info;

    zmq_sendmore_string(master_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_socket.get());
    zmq_send_binstream(master_socket.get(), stream);
}

void RedisSplitAssigner::master_redis_req_handler() {
    auto& master = Master::get_instance();
    auto master_socket = master.get_socket();
    BinStream stream = zmq_recv_binstream(master_socket.get());

    int global_tid = -1;
    stream >> global_tid;

    stream.clear();

    // deliver keys to the incoming worker
    std::vector<std::vector<RedisRangeKey> > ret;
    answer_tid_best_keys(global_tid, ret);

    /* visualize delivered keys
    for (int split_i=0; split_i<ret.size(); split_i++) {
        for (auto& split : splits_) {
            if (split.second.get_sn() == split_i) {
                std::vector<RedisRangeKey> keys = ret[split_i];
                for (auto& key : keys) {
                    LOG_I << split_i << " " << split.second.get_ip() << ":" << split.second.get_port() << " <- " << gen_slot_crc16(key.str_.c_str(), key.str_.length()) << " " << key.str_;
                }
                break;
            }
        }
    }
    */

    stream << worker_task_status_[global_tid];
    stream << ret;

    zmq_sendmore_string(master_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_socket.get());
    zmq_send_binstream(master_socket.get(), stream);
}

void RedisSplitAssigner::master_redis_req_end_handler() {
    auto& master = Master::get_instance();
    auto master_socket = master.get_socket();
    BinStream stream = zmq_recv_binstream(master_socket.get());
    int global_tid = -1;
    int num_received_keys = -1;

    stream >> global_tid;
    stream >> num_received_keys;
    receive_end(global_tid, num_received_keys);

    if (if_all_keys_fetched_) {
        reset_default_states();
    }

    stream.clear();
    zmq_sendmore_string(master_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_socket.get());
    zmq_send_binstream(master_socket.get(), stream);
}

void RedisSplitAssigner::master_setup_handler() {
    if (!load_parameters()) return;

    seed_ = std::chrono::system_clock::now().time_since_epoch().count();
    srand(seed_);

    create_redis_info();
    create_husky_info();
    create_split_proc_map();
    create_redis_con_pool();
    import_all_pattern_keys();
    create_schedule_thread();
}

RedisSplitAssigner::~RedisSplitAssigner() {
    splits_.clear();
    split_groups_.clear();

    all_keys_.clear();
    batch_keys_.clear();
    non_local_served_keys_.clear();
    worker_keys_pools_.clear();
    worker_num_fetched_keys_.clear();
    worker_num_keys_assigned_.clear();

    split_proc_map_.clear();
    procs_load_.clear();
    keys_latency_map_.clear();
    proc_keys_stat_.clear();

    if (keys_file_.is_open()) {
        keys_file_.close();
    }

    // release Redis connection pool
    for (auto& con : cons_) {
        if (con.second) {
            redisFree(con.second);
        }
    }
    cons_.clear();

    scheduler_.join();
}

void RedisSplitAssigner::set_auth(const std::string& password) {
    password_ = password;
    need_auth_ = true;
}

void RedisSplitAssigner::reset_auth() { need_auth_ = false; }

bool RedisSplitAssigner::refresh_splits_info() {
    redisContext *c = NULL;
    redisReply *reply = NULL;
    c = redisConnectWithTimeout(ip_.c_str(), port_, timeout_);
    if (NULL == c || c->err) {
        if (c) {
            LOG_E << "Connection error: " << std::string(c->errstr);
            redisFree(c);
        } else {
            LOG_E << "Connection error: can't allocate redis context";
        }
        return 0;
    }

    // to be tested
    if (need_auth_) {
        reply = redisCmd(c, "AUTH %s", password_.c_str());
        CHECK_REP(reply);
    }

    int serial_number = 0;
    // get the cluster nodes list
    reply = redisCmd(c, "CLUSTER NODES");
    std::istringstream rep_lines(reply->str);
    char line_buf[256] = "";
    // parse each redis server
    while (rep_lines.getline(line_buf, sizeof(line_buf))) {
        RedisSplit split;
        split.set_sn(serial_number++);
        std::istringstream line(line_buf);
        std::vector<std::string> split_info;
        // parse a line of server info
        while (line) {
            std::string field;
            line >> field;
            if ("" != field) {
                split_info.push_back(field);
            }
        }
        // parse id
        split.set_id(split_info[0]);
        // parse ip
        std::istringstream ip_port(split_info[1]);
        ip_port.getline(line_buf, sizeof(line_buf), ':');
        split.set_ip(line_buf);
        // parse port
        ip_port.getline(line_buf, sizeof(line_buf), ':');
        std::istringstream ports(line_buf);
        ports.getline(line_buf, sizeof(line_buf), '@');
        split.set_port(atoi(line_buf));
        // parse role
        split.set_master(split_info[3]);
        // parse node state
        // if healthy
        if (split_info[2].substr(split_info[2].size()-4, 4).compare("fail")) {
            // if master
            if (!split_info[3].compare("-")) {
                // slots start
                std::istringstream slots(split_info[8]);
                slots.getline(line_buf, sizeof(line_buf), '-');
                split.set_sstart(atoi(line_buf));
                // slots end
                slots.getline(line_buf, sizeof(line_buf), '-');
                split.set_send(atoi(line_buf));
                slots.getline(line_buf, sizeof(line_buf), '-');
                // init load balance
                RedisSplitGroup split_group(split);
                split_groups_[split.get_id()] = split_group;
            }
            split.set_valid(true);
        // if down
        } else {
            split.set_valid(false);
        }
        splits_[split.get_id()] = split;
    }

    // set slaves' slots range for load balancing
    for (auto& split : splits_) {
        std::string my_master;
        if ((my_master = split.second.get_master()).compare("-")) {
            split.second.set_sstart(splits_[my_master].get_sstart());
            split.second.set_send(splits_[my_master].get_send());
            split_groups_[my_master].add_member(split.first);
        }
    }

    // sort split_groups for efficient query
    sorted_split_group_name_.clear();
    for ( auto& split_group : split_groups_ ) {
        sorted_split_group_name_.push_back(split_group.first);
    }
    std::sort(sorted_split_group_name_.begin(), sorted_split_group_name_.end(),
            [&](std::string& a, std::string& b){
            return splits_[a].get_sstart() < splits_[b].get_sstart();
            });
    num_slots_per_group_ = 16384 / split_groups_.size();

    if (reply) {
        freeReplyObject(reply);
    }
    if (c) {
        redisFree(c);
    }
}

void RedisSplitAssigner::reset_default_states() {
    num_keys_amount_ = 0;
    num_keys_batched_ = 0;
    num_keys_scheduled_ = 0;
    num_keys_fetched_ = 0;
    cur_start_ = 0;
    non_local_offset_ = 0;

    is_dynamic_imported_ = false;
    is_pattern_imported_ = false;
    is_pattern_batched_ = false;
    is_file_imported_ = false;
    if_all_keys_scheduled_ = false;
    if_all_keys_fetched_ = false;

    refresh_splits_info();
    create_redis_con_pool();

    all_keys_.clear();
    batch_keys_.clear();
    non_local_served_keys_.clear();
    keys_latency_map_.clear();

    procs_load_.clear();
    proc_keys_stat_.clear();
    proc_worker_offset_.clear();
    proc_worker_map_.clear();

    worker_num_keys_assigned_.clear();
    worker_num_fetched_keys_.clear();
    worker_task_status_.clear();
    worker_keys_pools_.clear();

    create_husky_info();

    if (if_scheduler_stopped_) {
        scheduler_.join();
        if_scheduler_stopped_ = false;
        create_schedule_thread();
    }
}

bool RedisSplitAssigner::create_redis_info() {
    refresh_splits_info();

    for (auto& split_group : split_groups_) {
        const std::vector<std::string> members = split_group.second.get_members();
        LOG_I << "\033[1;32m====================================================\033[0m";
        LOG_I << "redis group id: " << split_group.first;
        LOG_I << "slots start: " << std::to_string(splits_[split_group.first].get_sstart());
        LOG_I << "slots end: " << std::to_string(splits_[split_group.first].get_send());
        for (auto& member_id : members) {
            LOG_I << "serial number:" << splits_[member_id].get_sn() << " member id:" << member_id << " ip:" << splits_[member_id].get_ip() << " port:" << std::to_string(splits_[member_id].get_port()) << " priority:" << std::to_string(split_group.second.get_priority(member_id));
        }
    }
    LOG_I << "\033[1;32m====================================================\033[0m";
}

void RedisSplitAssigner::answer_tid_best_keys(int global_tid, std::vector<std::vector<RedisRangeKey> >& ret) {
    std::unique_lock<std::mutex> pools_lock(worker_pools_mutex_);

    int num_answered = 0;
    int num_splits = splits_.size();
    int num_remained_keys = 0;
    for (int split_i=0; split_i < num_splits; split_i++) {
        num_remained_keys += worker_keys_pools_[global_tid][split_i].size();
        std::vector<RedisRangeKey> split_keys;
        ret.push_back(split_keys);
    }
    int num_to_be_answered = worker_num_keys_assigned_[global_tid].front() < num_remained_keys ? worker_num_keys_assigned_[global_tid].front() : num_remained_keys;

    if (num_to_be_answered > 0) {
        int i = 0;
        while (num_answered < num_to_be_answered) {
            int split_i = i++ % num_splits;
            if (!worker_keys_pools_[global_tid][split_i].empty()) {
                ret[split_i].push_back(worker_keys_pools_[global_tid][split_i].back());
                worker_keys_pools_[global_tid][split_i].pop_back();
                num_answered++;
            }
        }
    }

    if (0 == num_remained_keys - num_answered) {
        if (is_dynamic_imported_ && is_pattern_batched_ && is_file_imported_ && if_all_keys_scheduled_) {
            worker_task_status_[global_tid] = io::RedisTaskStatus::NoMoreTask;
        } else if (!is_dynamic_imported_ || !is_pattern_batched_ || !is_file_imported_ || !if_all_keys_scheduled_) {
            worker_task_status_[global_tid] = io::RedisTaskStatus::WaitTasks;
        } else {
            worker_task_status_[global_tid] = io::RedisTaskStatus::Abnormal;
        }
    } else {
        worker_task_status_[global_tid] = io::RedisTaskStatus::WaitTasks;
    }
}

void RedisSplitAssigner::answer_masters_info(std::map<std::string, RedisSplit>& redis_masters_info) {
    for (auto& split_group : split_groups_) {
        RedisSplit& master = splits_[split_group.first];
        redis_masters_info[master.get_id()] = master;
    }
}

void RedisSplitAssigner::answer_splits_info(std::map<std::string, RedisSplit>& redis_splits_info) {
    for (auto& split : splits_) {
        redis_splits_info[split.first] = split.second;
    }
}

void RedisSplitAssigner::receive_end(int global_tid, int num_received_keys) {
    std::unique_lock<std::mutex> pools_lock(worker_pools_mutex_);
    worker_num_fetched_keys_[global_tid] += num_received_keys;
    num_keys_fetched_ += num_received_keys;
    if (worker_num_keys_assigned_[global_tid].front() == num_received_keys) {
        worker_num_keys_assigned_[global_tid].pop();
    } else {
        LOG_E << "end:worker_" << global_tid << "[" << num_received_keys << "/" << worker_num_keys_assigned_[global_tid].front() << "]";
        worker_num_keys_assigned_[global_tid].pop();
    }
    LOG_I << "fetched [" << num_keys_fetched_ << "," << num_keys_scheduled_ << "," << num_keys_batched_ << "/" << num_keys_amount_ << "]";
    if (num_keys_fetched_ == num_keys_amount_) {
        if_all_keys_fetched_ = true;
    }
}

uint16_t RedisSplitAssigner::gen_slot_crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc << 8) ^ io::crc16tab_[((crc >> 8) ^ *buf++)&0x00FF];
    return crc % 16384;
}

std::string RedisSplitAssigner::parse_host(const std::string& hostname) {
    hostent * record = gethostbyname(hostname.c_str());
    if (record == NULL) {
        LOG_E << "Hostname parse failed:" << hostname;
        return "failed";
    }
    in_addr * address = (in_addr *)record->h_addr;
    std::string ip_address = inet_ntoa(*address);
    return ip_address;
}

void RedisSplitAssigner::create_split_proc_map() {
    for (auto& split : splits_) {
        for (int proc_id=0; proc_id < num_procs_; proc_id++) {
            std::string proc_host = work_info_.get_hostname(proc_id);
            if (!(parse_host(proc_host)).compare(split.second.get_ip())) {
                split_proc_map_[split.first] = proc_id;
                break;
            }
        }
    }
}

void RedisSplitAssigner::create_husky_info() {
    work_info_ = Context::get_worker_info();
    num_procs_ = work_info_.get_num_processes();
    num_workers_ = work_info_.get_num_workers();
    batch_size_ = num_workers_ * 10000;
    for (int proc_id=0; proc_id < num_procs_; proc_id++) {
        // initialize process-level keys statistics
        std::vector<int> keys_stat{0, 0};
        proc_keys_stat_.push_back(keys_stat);
        // initialize process-level workload
        procs_load_.push_back(0);
        // mapping processes and workers
        int num_local_workers = work_info_.get_num_local_workers(proc_id);
        std::vector<int> local_workers;
        for (int worker_id=0; worker_id < num_local_workers; worker_id++) {
            local_workers.push_back(work_info_.local_to_global_id(proc_id, worker_id));
        }
        proc_worker_map_.push_back(local_workers);
        // initialize process' worker offset
        proc_worker_offset_.push_back(0);
    }
    for (int worker_id=0; worker_id < num_workers_; worker_id++) {
        // create worker-level keys pools
        std::vector<std::vector<RedisRangeKey> > best_keys_pool;
        for (int split_i=0; split_i < splits_.size(); split_i++) {
            std::vector<RedisRangeKey> split_keys;
            best_keys_pool.push_back(split_keys);
        }
        worker_keys_pools_.push_back(best_keys_pool);
        // create worker-level fetched keys statistics
        worker_num_fetched_keys_.push_back(0);
        // create worker-level task status
        worker_task_status_.push_back(io::RedisTaskStatus::WaitTasks);
        // create worker-level batch keys statistics
        std::queue<int> worker_num_batch_keys;
        worker_num_keys_assigned_.push_back(worker_num_batch_keys);
    }
}

void RedisSplitAssigner::create_redis_con_pool() {
    // create Redis connection pool
    redisReply *reply = NULL;
    for (auto& split_group : split_groups_) {
        RedisSplit master = splits_[split_group.first];
        redisContext * c = redisConnectWithTimeout(master.get_ip().c_str(), master.get_port(), timeout_);
        if (NULL == c || c->err) {
            if (c) {
                LOG_E << "Connection error: " + std::string(c->errstr);
                redisFree(c);
            } else {
                LOG_E << "Connection error: can't allocate redis context";
            }
            return;
        }

        // to be tested
        if (need_auth_) {
            reply = redisCmd(c, "AUTH %s", password_.c_str());
        }

        cons_[split_group.first] = c;
    }
    if (reply) {
        freeReplyObject(reply);
    }
}

void RedisSplitAssigner::create_schedule_thread() {
    scheduler_ = std::thread(&RedisSplitAssigner::load_schedule, this);
}

void RedisSplitAssigner::load_schedule() {
    if (if_streaming_mode_) {
        while (true) {
            std::unique_lock<std::mutex> pools_lock(worker_pools_mutex_, std::defer_lock);
            load_batch_keys();
            schedule_batch_keys(pools_lock.mutex());
        }
    } else {
        while (!is_dynamic_imported_ || !is_pattern_batched_ || !is_file_imported_ || !if_all_keys_scheduled_) {
            std::unique_lock<std::mutex> pools_lock(worker_pools_mutex_, std::defer_lock);
            load_batch_keys();
            schedule_batch_keys(pools_lock.mutex());
        }
    }
    if_scheduler_stopped_ = true;
    LOG_I << "scheduling thread finished.";
}

void RedisSplitAssigner::import_all_pattern_keys() {
    redisReply *reply = NULL;

    if (keys_pattern_.compare("")) {
        LOG_I << "keys from pattern [" << keys_pattern_ << "] ...";
        for (auto& split_group : split_groups_) {
            RedisSplit master = splits_[split_group.first];
            reply = redisCmd(cons_[master.get_id()], "KEYS %s", keys_pattern_.c_str());
            LOG_I << "[" << reply->elements << "] matched on [" << master.get_ip() << ":" << master.get_port() << "]";
            // no record matches this pattern on this master
            if (reply->elements <= 0) {
                continue;
            }
            std::vector<std::string> split_all_keys;
            int i = 0;
            for (; i < reply->elements; i++) {
                std::string key = std::string(reply->element[i]->str);
                split_all_keys.push_back(key);
            }
            all_keys_.push_back(split_all_keys);
            num_keys_amount_ += split_all_keys.size();
        }
        LOG_I << "[" << num_keys_amount_ << "] keys from pattern DONE";
    }

    is_pattern_imported_ = true;

    if (reply) {
        freeReplyObject(reply);
    }
}

// load a batch of keys
void RedisSplitAssigner::load_batch_keys() {
    redisReply *reply = NULL;

    // mode 1: load keys from Redis LIST, a batch at a time
    if (!is_dynamic_imported_) {
        if (keys_list_.compare("")) {
            LOG_I << "keys from list [" << keys_list_ << "] ...";
            if (!if_found_keys_list_) {
                uint16_t slot = gen_slot_crc16(keys_list_.c_str(), keys_list_.length());
                int split_group_id = slot / num_slots_per_group_;
                split_group_id = split_group_id > sorted_split_group_name_.size()-1 ? --split_group_id : split_group_id;
                if (slot < splits_[sorted_split_group_name_[split_group_id]].get_sstart()) {
                    split_group_id--;
                } else if (slot > splits_[sorted_split_group_name_[split_group_id]].get_send()) {
                    split_group_id++;
                }
                keys_list_master_ = splits_[sorted_split_group_name_[split_group_id]];
                // check if the list exists
                reply = redisCmd(cons_[keys_list_master_.get_id()], "EXISTS %s", keys_list_.c_str());
                if (0 == reply->integer) {
                    is_dynamic_imported_ = true;
                    LOG_E << "didn't find keys-list:" << keys_list_;
                } else {
                    if_found_keys_list_ = true;
                }
            }
            if (if_found_keys_list_) {
                reply = redisCmd(cons_[keys_list_master_.get_id()], "LRANGE %s %d %d", keys_list_.c_str(), cur_start_, cur_start_ + batch_size_ - 1);
                cur_start_ += batch_size_;
                for (int i=0; i < reply->elements; i++) {
                    std::string key = std::string(reply->element[i]->str);
                    batch_keys_.push_back(key);
                }
                if (0 == reply->elements) {
                    is_dynamic_imported_ = true;
                    LOG_I << "keys from list DONE";
                }
            }
        } else {
            is_dynamic_imported_ = true;
        }
    }

    // mode 2 (step 1): load keys from pattern, load all keys at once
    if (!is_pattern_imported_) {
        import_all_pattern_keys();
    }
    // mode 2 (step 2): load keys from pattern, generate a batch
    int num_remained = num_keys_amount_ - num_keys_batched_;
    if (!is_pattern_batched_) {
        if (num_keys_batched_ != num_keys_amount_) {
            int load_size = num_remained < batch_size_ ? num_remained : batch_size_;
            int num_loaded = 0;
            int i = 0;
            while (num_loaded < load_size) {
                std::vector<std::string>& split_all_keys = all_keys_[i++ % all_keys_.size()];
                if (!split_all_keys.empty()) {
                    batch_keys_.push_back(split_all_keys.back());
                    split_all_keys.pop_back();
                    num_loaded++;
                }
            }
            num_keys_batched_ += load_size;
        } else {
            is_pattern_batched_ = true;
        }
    }

    // mode 3: load keys from file, a batch at a time
    std::string raw_key;
    if (!is_file_imported_) {
        if (keys_file_.is_open()) {
            LOG_I << "keys from file [" << keys_path_ << "] ...";
            keys_file_.seekg(cur_pos_);
            // import keys for current batch
            for (int num_cur_line = 0; num_cur_line < batch_size_; num_cur_line++) {
                std::getline(keys_file_, raw_key);
                if (is_file_imported_ = keys_file_.eof()) {
                    is_file_imported_ = true;
                    break;
                }
                // eliminate '\r'
                std::string key = raw_key.erase(raw_key.find_last_not_of(" \r\n")+1);
                batch_keys_.push_back(key);
            }
            if (!is_file_imported_)
                cur_pos_ = keys_file_.tellg();
            LOG_I << "keys from file DONE";
        } else {
            is_file_imported_ = true;
        }
    }

    if (reply) {
        freeReplyObject(reply);
    }
}

uint64_t RedisSplitAssigner::reduce_max_workload(PROC_KEYS_POOLS& proc_new_keys_pools, std::vector<uint64_t>& workers_load) {
    // update process-level workload
    for (int proc_id=0; proc_id < num_procs_; proc_id++) {
        procs_load_[proc_id] = proc_keys_stat_[proc_id][0] * local_served_latency_ + proc_keys_stat_[proc_id][1] * non_local_served_latency_;
        // generate worker-level workload
        for (int worker_id : proc_worker_map_[proc_id]) {
            workers_load[worker_id] = procs_load_[proc_id] / proc_worker_map_[proc_id].size();
        }
    }
    auto minmax_it = std::minmax_element(workers_load.begin(), workers_load.end());
    int max_worker_id = minmax_it.second - workers_load.begin();
    int min_worker_id = minmax_it.first - workers_load.begin();
    int max_proc_id = work_info_.get_process_id(max_worker_id);
    int min_proc_id = work_info_.get_process_id(min_worker_id);
    int num_workers_max = proc_worker_map_[max_proc_id].size();
    int num_workers_min = proc_worker_map_[min_proc_id].size();
    uint64_t max_load = workers_load[max_worker_id];
    bool if_equal = false;
    // transfer non-local/local workload
    for (int key_type=1; key_type >= 0; key_type--) {
        for (auto& split_keys : proc_new_keys_pools[max_proc_id][key_type]) {
            if (if_equal) break;
            int num_split_keys = split_keys.second.size();
            for (int i=0; i < num_split_keys; i++) {
                const RedisRangeKey& range_key = split_keys.second.back();
                procs_load_[max_proc_id] -= keys_latency_map_[range_key.str_][max_proc_id];
                procs_load_[min_proc_id] += keys_latency_map_[range_key.str_][min_proc_id];
                if (local_served_latency_ == keys_latency_map_[range_key.str_][min_proc_id]) {
                    proc_new_keys_pools[min_proc_id][0][split_keys.first].push_back(split_keys.second.back());
                    proc_keys_stat_[min_proc_id][0]++;
                } else {
                    proc_new_keys_pools[min_proc_id][1][split_keys.first].push_back(split_keys.second.back());
                    proc_keys_stat_[min_proc_id][1]++;
                }
                split_keys.second.pop_back();
                proc_keys_stat_[max_proc_id][key_type]--;
                if (procs_load_[max_proc_id]/num_workers_max-non_local_served_latency_ <= procs_load_[min_proc_id]/num_workers_min) {
                    if_equal = true;
                    break;
                }
            }
        }
    }

    // return estimated reduced time at worker-level
    return max_load - procs_load_[max_proc_id]/num_workers_max;
}

void RedisSplitAssigner::schedule_batch_keys(std::mutex * pools_lock) {
    redisReply *reply = NULL;

    // step 1: initialize incoming keys pools
    PROC_KEYS_POOLS proc_new_keys_pools;
    for (int proc_id=0; proc_id < num_procs_; proc_id++) {
        std::map<std::string, std::vector<RedisRangeKey> > local_keys_pool;
        std::map<std::string, std::vector<RedisRangeKey> > non_local_keys_pool;
        std::vector<std::map<std::string, std::vector<RedisRangeKey> > > best_keys_pool{local_keys_pool, non_local_keys_pool};
        proc_new_keys_pools.push_back(best_keys_pool);
    }

    // step 2: assign local-served keys to process-level, retain non-local-served keys
    for (auto& key : batch_keys_) {
        RedisRangeKey range_key;
        range_key.str_ = key;
        uint16_t slot = gen_slot_crc16(key.c_str(), key.length());

        int split_group_id = slot / num_slots_per_group_;
        split_group_id = split_group_id > sorted_split_group_name_.size()-1 ? --split_group_id : split_group_id;
        if (slot < splits_[sorted_split_group_name_[split_group_id]].get_sstart()) {
            split_group_id--;
        } else if (slot > splits_[sorted_split_group_name_[split_group_id]].get_send()) {
            split_group_id++;
        }
        RedisSplitGroup& split_group = split_groups_[sorted_split_group_name_[split_group_id]];
        split_group.sort_members();
        for (int proc_id = 0; proc_id < num_procs_; proc_id++) {
            keys_latency_map_[key].push_back(non_local_served_latency_);
        }
        std::string selected_split_id = split_group.get_sorted_members()[0];
        std::map<std::string, int>::iterator it;
        if ((it = split_proc_map_.find(selected_split_id)) != split_proc_map_.end()) {
            int proc_id = it->second;
            proc_new_keys_pools[proc_id][0][selected_split_id].push_back(range_key);
            proc_keys_stat_[proc_id][0]++;
            keys_latency_map_[key][proc_id] = local_served_latency_;
        } else {
            // retain non-local-served keys (this kind of keys rarely exist)
            if (key_split_size_) {
                RedisSplit master = splits_[sorted_split_group_name_[split_group_id]];
                reply = redisCmd(cons_[master.get_id()], "TYPE %s", key.c_str());
                if (!strcmp(reply->str, "list")) {
                    reply = redisCmd(cons_[master.get_id()], "LLEN %s", key.c_str());
                    int llen = reply->integer;
                    // heavy list
                    if (llen >= key_split_size_) {
                        int range_start = 0;
                        for (int range_start=0; range_start < llen; range_start+=key_split_size_) {
                            range_key.start_ = range_start;
                            range_key.end_ = range_start+key_split_size_-1;
                            non_local_served_keys_[selected_split_id].push_back(range_key);
                        }
                    } else {
                        non_local_served_keys_[selected_split_id].push_back(range_key);
                    }
                }
            } else {
                non_local_served_keys_[selected_split_id].push_back(range_key);
            }
        }
        split_group.update_priority();
    }

    // step 3: optimize process-level workload
    int reduced_time = 0;
    int consumed_time = 0;
    int optimize_step = 1;
    std::chrono::time_point<std::chrono::system_clock> start;
    std::vector<uint64_t> workers_load;
    for (int worker_id=0; worker_id < num_workers_; worker_id++) {
        workers_load.push_back(0);
    }
    while (true) {
        start = std::chrono::system_clock::now();
        reduced_time = 0;
        for (int i=0; i < optimize_step; i++) {
            reduced_time += reduce_max_workload(proc_new_keys_pools, workers_load);
        }
        std::chrono::duration<double, std::micro> interval = std::chrono::system_clock::now() - start;
        consumed_time = int(interval.count());
        if (reduced_time <= consumed_time) break;
    }

    pools_lock->lock();
    // step 4: distribute local-served keys to workers
    std::vector<std::vector<int> > worker_keys_stat;
    for (int worker_id=0; worker_id < num_workers_; worker_id++) {
        std::vector<int> keys_stat{0, 0};
        worker_keys_stat.push_back(keys_stat);
        worker_num_keys_assigned_[worker_id].push(0);
    }
    int local_worker_id = 0;
    int worker_id = 0;
    for (int proc_id=0; proc_id < num_procs_; proc_id++) {
        int num_local_workers = proc_worker_map_[proc_id].size();
        for (int key_type=0; key_type < 2; key_type++) {
            for (auto& split_keys : proc_new_keys_pools[proc_id][key_type]) {
                int num_keys = split_keys.second.size();
                for (int i=0; i < num_keys; i++) {
                    local_worker_id = (i + proc_worker_offset_[proc_id]) % num_local_workers;
                    worker_id = proc_worker_map_[proc_id][local_worker_id];
                    worker_keys_pools_[worker_id][splits_[split_keys.first].get_sn()].push_back(split_keys.second.back());
                    worker_keys_stat[worker_id][key_type]++;
                    split_keys.second.pop_back();
                }
                proc_worker_offset_[proc_id] = local_worker_id + 1;
            }
        }
    }

    // step 5: distribute non-local-served keys
    for (auto& split_keys : non_local_served_keys_) {
        int num_keys = split_keys.second.size();
        for (int i=0; i < num_keys; i++) {
            worker_id = (i + non_local_offset_) % num_workers_;
            worker_keys_pools_[worker_id][splits_[split_keys.first].get_sn()].push_back(split_keys.second.back());
            worker_keys_stat[worker_id][1]++;
            proc_keys_stat_[work_info_.get_process_id(worker_id)][1]++;
            split_keys.second.pop_back();
        }
        non_local_offset_ = worker_id + 1;
    }

    // step 6: count worker-level assigned keys
    for (int worker_id; worker_id < num_workers_; worker_id++) {
        for (int key_type=0; key_type < 2; key_type++) {
            worker_num_keys_assigned_[worker_id].back() += worker_keys_stat[worker_id][key_type];
        }
    }

    /* visualize worker stat
    for (int worker_id; worker_id<num_workers_; worker_id++) {
        workers_load[worker_id] = worker_keys_stat[worker_id][0] * local_served_latency_ + worker_keys_stat[worker_id][1] * non_local_served_latency_;
        LOG_I << "worker_" << worker_id << " load: " << workers_load[worker_id] << ", \tkeys: " << worker_keys_stat[worker_id][0] + worker_keys_stat[worker_id][1] << "[" << float(worker_keys_stat[worker_id][0]) / (worker_keys_stat[worker_id][0]+worker_keys_stat[worker_id][1]) * 100 << "%]";
    }
    */
    worker_keys_stat.clear();

    num_keys_scheduled_ += batch_keys_.size();
    if (num_keys_scheduled_ == num_keys_amount_) {
        if_all_keys_scheduled_ = true;
    }
    pools_lock->unlock();
    batch_keys_.clear();

    if (reply) {
        freeReplyObject(reply);
    }
}

}  // namespace husky

#endif
