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

#include <string>
#include <utility>
#include <sstream>
#include <cstdlib>
#include <cstring>
#include <map>

#include "hiredis/hiredis.h"
#include "boost/tokenizer.hpp"

#include "core/constants.hpp"
#include "core/zmq_helpers.hpp"
#include "core/context.hpp"
#include "master/master.hpp"

#include <netdb.h>
#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fstream>

// for experiments
#include <ctime>
#include <chrono>

namespace husky {

static RedisSplitAssigner redis_split_assigner;

RedisSplitAssigner::RedisSplitAssigner() : end_count_(0), split_num_(0), num_proc_keys_(0), num_workers_assigned_(0), key_split_threshold_(0), num_batch_load_(10000), num_max_answer_(10000), is_file_imported_(false), is_pattern_imported_(false), is_dynamic_imported_(false) {
    Master::get_instance().register_main_handler(TYPE_REDIS_REQ,
                                                 std::bind(&RedisSplitAssigner::master_redis_req_handler, this));
    Master::get_instance().register_main_handler(TYPE_REDIS_END_REQ,
                                                 std::bind(&RedisSplitAssigner::master_redis_req_end_handler, this));
    Master::get_instance().register_setup_handler(std::bind(&RedisSplitAssigner::master_setup_handler, this));
}

void RedisSplitAssigner::master_redis_req_handler() {
    auto& master = Master::get_instance();
    auto master_socket = master.get_socket();
    BinStream stream = zmq_recv_binstream(master_socket.get());

    int global_tid = -1;
    stream >> global_tid;

    WorkerInfo work_info = Context::get_worker_info();
    stream.clear();
    if ( work_info.get_largest_tid() < global_tid ) {
        std::map<std::string, RedisSplit> redis_masters_info; 
        answer_masters_info(redis_masters_info); 
        stream << redis_masters_info;
    } else {
        int proc_id = work_info.get_process_id(global_tid);
        // load a batch of keys
        // TODO: reload keys
        if ( !is_dynamic_imported_ || !is_pattern_imported_ || !is_file_imported_ ) {
            load_keys();
        }
        RedisBestKeys ret = answer_tid_best_keys(global_tid);
        stream << ret;
    }

    zmq_sendmore_string(master_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_socket.get());
    zmq_send_binstream(master_socket.get(), stream);
}

void RedisSplitAssigner::master_redis_req_end_handler() {
    auto& master = Master::get_instance();
    auto master_socket = master.get_socket();
    BinStream stream = zmq_recv_binstream(master_socket.get());
    RedisBestKeys best_keys;
    int global_tid = -1;
    stream >> best_keys;
    stream >> global_tid;
    receive_end(best_keys);

    stream.clear();
    zmq_sendmore_string(master_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_socket.get());
    zmq_send_binstream(master_socket.get(), stream);
}

void RedisSplitAssigner::master_setup_handler() {
    ip_ = Context::get_param("redis_ip");
    port_ = atoi(Context::get_param("redis_port").c_str()); 
    key_split_threshold_ = stoi(Context::get_param("redis_key_split_threshold"));
    if ( (keys_path_ = Context::get_param("redis_keys_file")).compare("") ) {
        keys_file_.open(keys_path_, std::ios::in);
    }
    keys_pattern_ = Context::get_param("redis_keys_pattern").c_str(); 
    keys_list_ = Context::get_param("redis_keys_list").c_str(); 
    cache_splits_info();
    create_best_keys_pools();
    create_redis_con_pool();
}

RedisSplitAssigner::~RedisSplitAssigner() {
    splits_.clear();
    split_groups_.clear();
    waited_keys_.clear();
    proc_keys_pools_.clear();
    fetched_keys_.clear();
    if ( keys_file_.is_open() ) {
        keys_file_.close();
    }
    // release Redis connection pool
    for ( auto& con : cons_ ) {
        if (con.second) {
            redisFree(con.second);
        }
    }
    cons_.clear();
    batch_keys_.clear();
}

void RedisSplitAssigner::set_auth(const std::string& password) {
    password_ = password;
    need_auth_ = true;
}

void RedisSplitAssigner::reset_auth() { need_auth_ = false; }

bool RedisSplitAssigner::cache_splits_info() {
    redisContext *c = NULL;
    redisReply *reply = NULL;
    c = redisConnectWithTimeout( ip_.c_str(), port_, timeout_);
    if (NULL == c || c->err) {
        if (c){
            LOG_I << "Connection error: " << std::string(c->errstr);
            redisFree(c);
        }else{
            LOG_I << "Connection error: can't allocate redis context";
        }
        return 0;
    }

    // TODO: to be tested
    if (need_auth_) {
        reply = redisCmd(c, "AUTH %s", password_.c_str());
    }

    // get the cluster nodes list
    reply = redisCmd(c, "CLUSTER NODES");
    std::istringstream rep_lines(reply->str);
    char line_buf[256] = "";
    // parse each redis server
    while(rep_lines.getline(line_buf, sizeof(line_buf))){
        RedisSplit split;
        std::istringstream line(line_buf);
        std::vector<std::string> split_info;
        // parse a line of server info
        while(line){
            std::string field;
            line >> field;
            if( "" != field){
                split_info.push_back(field);   
            }
        }
        // parse id
        // TODO: seems too many spaces
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
        if (split_info[2].substr(split_info[2].size()-4, 4).compare("fail")){
            // if master
            if (!split_info[3].compare("-")){
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
        }else{
            split.set_valid(false);
        }
        splits_[split.get_id()] = split;
    }

    // set slaves' slots range for load balancing
    for ( auto& split : splits_) {
        std::string my_master;
        if ((my_master = split.second.get_master()).compare("-")) {
            split.second.set_sstart(splits_[my_master].get_sstart());
            split.second.set_send(splits_[my_master].get_send());
            split_groups_[my_master].add_member(split.first);
        }
    }
    for ( auto& split_group : split_groups_) {
        const std::vector<std::string> members = split_group.second.get_members();
        LOG_I << "\033[1;32m====================================================\033[0m";
        LOG_I << "redis group id: " << split_group.first;
        LOG_I << "slots start: " << std::to_string(splits_[split_group.first].get_sstart());
        LOG_I << "slots end: " << std::to_string(splits_[split_group.first].get_send());
        for ( auto& member_id : members) {
            LOG_I << "member id:" << member_id << " ip:" << splits_[member_id].get_ip() << " port:" << std::to_string(splits_[member_id].get_port()) << " priority:" << std::to_string(split_group.second.get_priority(member_id));
        }
    }
    LOG_I << "\033[1;32m====================================================\033[0m";

    if (reply) {
        freeReplyObject(reply);
    }
    if (c) {
        redisFree(c);
    }

    split_num_ = splits_.size();
}

// only answer at most num_max_answer_ keys a time
RedisBestKeys RedisSplitAssigner::answer_tid_best_keys(int global_tid) {

    RedisBestKeys ret;
    WorkerInfo work_info = Context::get_worker_info();

    int proc_id = work_info.get_process_id(global_tid);
    int num_global_workers = work_info.get_num_workers();
    num_workers_assigned_ %= num_global_workers;
    int num_keys_remain = waited_keys_.size() + num_proc_keys_;
    int num_keys_this_worker = (num_keys_remain / (num_global_workers-num_workers_assigned_) + 1);
    num_keys_this_worker = num_keys_this_worker < num_max_answer_ ? num_keys_this_worker : num_max_answer_;

    // local keys
    int num_proc_keys = proc_keys_pools_[proc_id].size();
    if ( num_proc_keys ) {
        int num_proc_workers = work_info.get_num_local_workers(proc_id);
        int num_local_keys_this_worker = num_proc_keys / num_proc_workers + 1;
        num_local_keys_this_worker = num_local_keys_this_worker < num_max_answer_ ? num_local_keys_this_worker : num_max_answer_;
        for ( int i=0; i<num_local_keys_this_worker && !proc_keys_pools_[proc_id].empty(); i++ ) {
            RedisRangeKey key = proc_keys_pools_[proc_id].back();
            std::string proc_ip = parse_host(work_info.get_hostname(proc_id));
            for ( auto& split : splits_ ) {
                if ( !split.second.get_ip().compare(proc_ip) ) {
                    ret.add_key(split.second, key);
                    proc_keys_pools_[proc_id].pop_back();
                    num_proc_keys_--;
                    break;
                }
            }
        }
        num_keys_this_worker -= num_local_keys_this_worker;
    }

    // normal keys
    for (int i=0; i<num_keys_this_worker && !waited_keys_.empty(); i++){
        RedisRangeKey key = waited_keys_.back();
        uint16_t slot = gen_slot_crc16(key.str_.c_str(), key.str_.length());
        for (auto& split : splits_){
            if (slot >= split.second.get_sstart() && slot <= split.second.get_send()) {
                ret.add_key(split.second, key);
                waited_keys_.pop_back();
                break;
            }
        }
    }

    num_workers_assigned_++;

    return ret;
}

void RedisSplitAssigner::answer_masters_info(std::map<std::string, RedisSplit>& redis_masters_info) {
    for ( auto& split_group : split_groups_ ) {
        RedisSplit& master = splits_[split_group.first];
        redis_masters_info[master.get_id()] = master;
    }
}

void RedisSplitAssigner::receive_end(RedisBestKeys& best_keys) {
    std::map<RedisSplit, std::vector<RedisRangeKey> > rs_keys = best_keys.get_keys();
    for ( auto it=rs_keys.begin(); it!=rs_keys.end(); it++){
        for ( auto& key : it->second){
            fetched_keys_.push_back(key);
        }
        end_count_ += (it->second).size();
    }
}

uint16_t RedisSplitAssigner::gen_slot_crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc<<8) ^ crc16tab_[((crc>>8) ^ *buf++)&0x00FF];
    return crc % 16384;
}

// TODO: to be replaced by Husky built-in function
std::string RedisSplitAssigner::parse_host(const std::string& hostname){
    hostent * record = gethostbyname(hostname.c_str());
    if(record == NULL){
        LOG_I << "Hostname parse failed:" << hostname;
        return "failed";
    }
    in_addr * address = (in_addr * )record->h_addr;
    std::string ip_address = inet_ntoa(*address);
    return ip_address;
}

void RedisSplitAssigner::create_best_keys_pools (){
    // create best keys pools for each process
    WorkerInfo work_info = Context::get_worker_info();
    int num_procs = work_info.get_num_processes(); 
    for ( int proc_id = 0; proc_id < num_procs; proc_id++){
        std::vector<RedisRangeKey> best_keys_pool;
        proc_keys_pools_.push_back(best_keys_pool);
    }
}

void RedisSplitAssigner::create_redis_con_pool (){
    // create Redis connection pool
    redisReply *reply = NULL;
    for ( auto& split_group : split_groups_ ) {
        RedisSplit master = splits_[split_group.first];
        redisContext * c = redisConnectWithTimeout( master.get_ip().c_str(), master.get_port(), timeout_);
        if (NULL == c || c->err) {
            if (c){
                LOG_I << "Connection error: " + std::string(c->errstr);
                redisFree(c);
            }else{
                LOG_I << "Connection error: can't allocate redis context";
            }
            return;
        }

        // TODO: to be tested
        if (need_auth_) {
            reply = redisCmd(c, "AUTH %s", password_.c_str());
        }

        cons_[split_group.first] = c;
    } 
    if (reply) {
        freeReplyObject(reply);
    }
}

// TODO: separate load & schedule
// load a batch & schedule
void RedisSplitAssigner::load_keys(){

    batch_keys_.clear();

    WorkerInfo work_info = Context::get_worker_info();
    int num_procs = work_info.get_num_processes(); 
    redisReply *reply = NULL;

    // load keys from Redis LIST
    if ( !is_dynamic_imported_ && keys_list_.compare("") ) {
        for ( auto& split_group : split_groups_ ) {
            RedisSplit master = splits_[split_group.first];
            reply = redisCmd(cons_[master.get_id()], "EXISTS %s", keys_list_.c_str());
            // dynamic list doesn't exist on this master
            if ( 0 == reply->integer )
                continue;
            reply = redisCmd(cons_[master.get_id()], "LRANGE %s %d %d", keys_list_.c_str(), cur_start_, cur_start_+num_batch_load_-1);
            cur_start_ += num_batch_load_;
            for ( int i=0; i<reply->elements; i++ ) {
                std::string key = std::string(reply->element[i]->str);
                batch_keys_.push_back(key);
            }
            if ( reply->elements < num_batch_load_ )
                is_dynamic_imported_ = true;
            if ( reply->elements > 0 )
                break;
        }
    }

    // load keys according to a pattern, only load once
    if ( !is_pattern_imported_ && keys_pattern_.compare("") ) {
        for ( auto& split_group : split_groups_ ) {
            RedisSplit master = splits_[split_group.first];
            reply = redisCmd(cons_[master.get_id()], "KEYS %s", keys_pattern_.c_str());
            // no record matches this pattern on this master
            if ( reply->elements <= 0 )
                continue;
            for ( int i=0; i<reply->elements; i++ ) {
                std::string key = std::string(reply->element[i]->str);
                batch_keys_.push_back(key);
            }
        }
        is_pattern_imported_ = true;
    }

    // load keys from file
    std::string raw_key;
    if ( !is_file_imported_ && keys_file_.is_open() ) {
        keys_file_.seekg(cur_pos_);
        // import keys for current batch
        for ( int num_cur_line = 0; num_cur_line<num_batch_load_; num_cur_line++) {
            std::getline(keys_file_, raw_key);
            if ( is_file_imported_ = keys_file_.eof() ) {
                // TODO: reload without shutdown
                // is_file_imported_ = true;
                break;
            }
            // eliminate '\r'
            std::string key = raw_key.erase(raw_key.find_last_not_of(" \r\n")+1);
            batch_keys_.push_back(key);
        }
        if ( !is_file_imported_ )
            cur_pos_ = keys_file_.tellg();
    }

    // for test, time
    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

    // local/heavy task assignment
    for ( auto& key : batch_keys_ ) {
        RedisRangeKey range_key;
        range_key.str_ = key;
        bool is_locally_assigned = false;
        uint16_t slot = gen_slot_crc16(key.c_str(), key.length());
        RedisSplit master;
        // local-first key
        for (auto& split_group : split_groups_){
            master = splits_[split_group.first];
            if (slot >= master.get_sstart() && slot <= master.get_send()) {
                // master-slaves load balance, priority from the highest to the lowest
                for ( auto& candidate_id : split_group.second.get_sorted_members()) {
                    bool is_chosen = false;
                    for (int proc_id = 0; proc_id < num_procs; proc_id++){
                        std::string proc_host = work_info.get_hostname(proc_id);
                        if (!(parse_host(proc_host)).compare(splits_[candidate_id].get_ip())){
                            proc_keys_pools_[proc_id].push_back(range_key);
                            num_proc_keys_++;
                            is_locally_assigned = true;
                            is_chosen = true;
                            // if turn on master-slaves load balance
                            if ( true )
                                split_group.second.update_priority();
                            break;
                        }
                    }
                    if ( is_chosen )
                        break;
                }
            }
            if (is_locally_assigned)
                break;
        }
        // normal key
        if (!is_locally_assigned) {
            // TODO: if heavy key, currently only split heavy LIST, only query from master
            bool is_heavy = false;

            reply = redisCmd(cons_[master.get_id()], "TYPE %s", key.c_str());
            if (!strcmp(reply->str, "list")) {
                reply = redisCmd(cons_[master.get_id()], "LLEN %s", key.c_str());
                int llen = reply->integer;
                // heavy list
                if ( key_split_threshold_ && llen >= key_split_threshold_ ) {
                    is_heavy = true;
                    WorkerInfo work_info = Context::get_worker_info();
                    int num_key_split = work_info.get_num_workers();
                    int num_range_per_worker = llen / (num_key_split) + 1;
                    for ( int range_start=0; range_start<llen; range_start+=num_range_per_worker ) {
                        range_key.start_ = range_start;
                        range_key.end_ = range_start+num_range_per_worker-1;
                        waited_keys_.push_back(range_key);
                    }
                }
            }

            if ( !is_heavy ) {
                waited_keys_.push_back(range_key);
            }
        }
    }

    // even process keys pools, local optimal
    int avg_amount = num_proc_keys_ / proc_keys_pools_.size() + 1;
    std::vector<int> proc_num_diffs;
    int diff = 0;
    std::vector<RedisRangeKey> waited_to_balance;
    for ( int i=0; i<proc_keys_pools_.size(); i++ ) {
        if ( (diff = proc_keys_pools_[i].size() - avg_amount) > 0 ) {
            for ( int j=0; j<diff; j++ ) {
                waited_to_balance.push_back( proc_keys_pools_[i].back() );
                proc_keys_pools_[i].pop_back();
            }
        }
        proc_num_diffs.push_back( proc_keys_pools_[i].size() - avg_amount );
    }
    while ( !waited_to_balance.empty() ) {
        waited_keys_.push_back( waited_to_balance.back() );
        waited_to_balance.pop_back();
    }

    // for test, time
    std::chrono::duration<double> interval = std::chrono::system_clock::now() - start;
    LOG_I << "schedule time: " + std::to_string(interval.count());
    start = std::chrono::system_clock::now();
    
    if (reply) {
        freeReplyObject(reply);
    }
}

}  // namespace husky

#endif
