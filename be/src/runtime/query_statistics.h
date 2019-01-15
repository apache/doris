// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_EXEC_QUERY_STATISTICS_H
#define DORIS_BE_EXEC_QUERY_STATISTICS_H

#include "common/atomic.h"
#include "gen_cpp/data.pb.h"
#include "util/spinlock.h"

namespace doris {

// This is responsible for collecting query statistics, usually it consist of 
// two parts, one is current fragment or plan's statistics, the other is sub fragment
// or plan's statistics.
class QueryStatistics {
public:

    class Statistics {
    public:

        Statistics() : process_rows(0), scan_bytes(0) {
        }

        void add(const Statistics& other) {
            process_rows += other.process_rows;
            scan_bytes += other.scan_bytes;
        }
    
        void add(const PQueryStatistics& other) {
            process_rows += other.process_rows();
            scan_bytes += other.scan_bytes();
        }    

        void reset() {
            process_rows = 0;
            scan_bytes = 0;
        }

        void set_scan_bytes(int64_t scan_bytes) {
            this->scan_bytes = scan_bytes;
        }

        void add_scan_bytes(int64_t scan_bytes) {
            this->scan_bytes += scan_bytes;
        }

        long get_scan_bytes() {
            return scan_bytes;        
        }

        void set_process_rows(int64_t process_rows) {
            this->process_rows = process_rows;
        }

        void add_process_rows(int64_t process_rows) {
            this->process_rows += process_rows;
        }

        long get_process_rows() {
            return process_rows;
        }

        void serialize(PQueryStatistics* statistics) {
            DCHECK(statistics != nullptr);
            statistics->set_process_rows(process_rows);
            statistics->set_scan_bytes(scan_bytes);
        }

        void deserialize(const PQueryStatistics& statistics) {
            process_rows = statistics.process_rows();
            scan_bytes = statistics.scan_bytes();
         }

    private:

        long process_rows;
        long scan_bytes;
    };

    QueryStatistics() {
    }

    // It can't be called by this and other at the same time in different threads.
    // Otherwise it will cause dead lock.
    void add(QueryStatistics* other) {
        boost::lock_guard<SpinLock> l(_lock);
        boost::lock_guard<SpinLock> other_l(other->_lock);
        auto other_iter = other->_statistics_map.begin();
        while (other_iter != other->_statistics_map.end()) {
            auto iter = _statistics_map.find(other_iter->first);
            Statistics* statistics = nullptr;          
            if (iter == _statistics_map.end()) {
                statistics = new Statistics();
                _statistics_map[other_iter->first] = statistics;
            } else {
                statistics = iter->second;
            }
            Statistics* other_statistics = other_iter->second;
            statistics->add(*other_statistics); 
            other_iter++;
        }
    }

    void add_process_rows(long process_rows) {
       boost::lock_guard<SpinLock> l(_lock);
       auto statistics = find(DEFAULT_SENDER_ID);
       statistics->add_process_rows(process_rows);
    }   

    void add_scan_bytes(long scan_bytes) {
       boost::lock_guard<SpinLock> l(_lock);
       auto statistics = find(DEFAULT_SENDER_ID);
       statistics->add_scan_bytes(scan_bytes);
    }

    void deserialize(const PQueryStatistics& other, int sender_id) {
        boost::lock_guard<SpinLock> l(_lock);
        auto statistics = find(sender_id);
        statistics->deserialize(other);
    }

    void serialize(PQueryStatistics* statistics) { 
        DCHECK(statistics != nullptr);
        boost::lock_guard<SpinLock> l(_lock);
        Statistics total_statistics = get_total_statistics();
        total_statistics.serialize(statistics);
    }

    long get_process_rows() {
        boost::lock_guard<SpinLock> l(_lock);
        Statistics statistics = get_total_statistics();
        return statistics.get_process_rows();
    }

    long get_scan_bytes() {
        boost::lock_guard<SpinLock> l(_lock);
        Statistics statistics = get_total_statistics();
        return statistics.get_scan_bytes();
    }

    long get_process_rows(int sender_id) {
        boost::lock_guard<SpinLock> l(_lock);
        auto statistics = find(sender_id);
        return statistics->get_process_rows();
    }

    long get_scan_bytes(int sender_id) {
        boost::lock_guard<SpinLock> l(_lock);
        auto statistics = find(sender_id);
        return statistics->get_scan_bytes();
    }

    void clear() {
        boost::lock_guard<SpinLock> l(_lock);
        auto iter = _statistics_map.begin();
        while (iter != _statistics_map.end()) {
            iter->second->reset();
            iter++;
        }
    }

    ~QueryStatistics() {
        boost::lock_guard<SpinLock> l(_lock);
        auto iter = _statistics_map.begin();
        while (iter != _statistics_map.end()) {
            delete iter->second;
            iter++; 
       }   
    }

private:

    Statistics* find(int sender_id) {
       auto iter = _statistics_map.find(sender_id);
       Statistics* statistics = nullptr;
       if (iter == _statistics_map.end()) {
           statistics = new Statistics();
           _statistics_map[sender_id] = statistics;
       } else {
           statistics = iter->second;
       }
       return statistics; 
    }

    Statistics get_total_statistics() {
        Statistics total_statistics;
        auto iter = _statistics_map.begin();
        while (iter != _statistics_map.end()) {
           total_statistics.add(*(iter->second));
           iter++;
        }   
        return total_statistics;
    }  

    // Map lock.
    SpinLock _lock;
    // Sender id to statistics.
    std::map<int, Statistics*> _statistics_map;
    // To index current plan. 
    static const int DEFAULT_SENDER_ID = -1;
};

}

#endif
