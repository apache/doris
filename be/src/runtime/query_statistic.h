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

#ifndef DORIS_BE_EXEC_QUERY_STATISTIC_H
#define DORIS_BE_EXEC_QUERY_STATISTIC_H

#include "common/atomic.h"
#include "gen_cpp/data.pb.h"
#include "util/spinlock.h"

namespace doris {

// This is responsible for collecting query statistic, usually it consist of 
// two parts, one is current fragment or plan's statistic, the other is sub fragment
// or plan's statistic.Now cpu is measured by number of rows, io is measured by byte.
class QueryStatistic {
public:

    class Statistic {
    public:

        Statistic() : cpu_by_row(0), io_by_byte(0) {
        }

        void add(const Statistic& other) {
            cpu_by_row += other.cpu_by_row;
            io_by_byte += other.io_by_byte;
        }
    
        void add(const PQueryStatistic& other) {
            cpu_by_row += other.cpu_by_row();
            io_by_byte += other.io_by_byte();
        }    

        void reset() {
            cpu_by_row = 0;
            io_by_byte = 0;
        }

        void set_io_by_byte(int64_t io_by_byte) {
            this->io_by_byte = io_by_byte;
        }

        void add_io_by_byte(int64_t io_by_byte) {
            this->io_by_byte += io_by_byte;
        }

        long get_io_by_byte() {
            return io_by_byte;        
        }

        void set_cpu_by_row(int64_t cpu_by_row) {
            this->cpu_by_row = cpu_by_row;
        }

        void add_cpu_by_row(int64_t cpu_by_row) {
            this->cpu_by_row += cpu_by_row;
        }

        long get_cpu_by_row() {
            return cpu_by_row;
        }

        void serialize(PQueryStatistic* statistic) {
             DCHECK(statistic != nullptr);
            statistic->set_cpu_by_row(cpu_by_row);
            statistic->set_io_by_byte(io_by_byte);
        }

        void deserialize(const PQueryStatistic& statistic) {
            cpu_by_row = statistic.cpu_by_row();
            io_by_byte = statistic.io_by_byte();
         }

        private:

        long cpu_by_row;
        long io_by_byte;
    };

    QueryStatistic() {
    }

    void add(const QueryStatistic& other) {
        boost::lock_guard<SpinLock> l(_lock);
        auto other_iter = other._statistics.begin();
        while (other_iter != other._statistics.end()) {
            auto iter = _statistics.find(other_iter->first);
            Statistic* statistic = nullptr;          
            if (iter == _statistics.end()) {
                statistic = new Statistic();
                _statistics[other_iter->first] = statistic;
            } else {
                statistic = iter->second;
            }
            Statistic* other_statistic = other_iter->second;
            statistic->add(*other_statistic); 
            other_iter++;
        }
    }

    void add_cpu_by_row(long cpu_by_row) {
       boost::lock_guard<SpinLock> l(_lock);
       auto statistic = find(DEFAULT_SENDER_ID);
       statistic->add_cpu_by_row(cpu_by_row);
    }   

    void add_io_by_byte(long io_by_byte) {
       boost::lock_guard<SpinLock> l(_lock);
       auto statistic = find(DEFAULT_SENDER_ID);
       statistic->add_io_by_byte(io_by_byte);
    }

    void deserialize(const PQueryStatistic& other, int sender_id) {
        boost::lock_guard<SpinLock> l(_lock);
        auto statistic = find(sender_id);
        statistic->deserialize(other);
    }

    void serialize(PQueryStatistic* statistic) { 
        boost::lock_guard<SpinLock> l(_lock);
        DCHECK(statistic != nullptr);
        Statistic total_statistic = get_total_statistic();
        total_statistic.serialize(statistic);
    }

    long get_cpu_by_row() {
        boost::lock_guard<SpinLock> l(_lock);
        Statistic statistic = get_total_statistic();
        return statistic.get_cpu_by_row();
    }

    long get_io_by_byte() {
        boost::lock_guard<SpinLock> l(_lock);
        Statistic statistic = get_total_statistic();
        return statistic.get_io_by_byte();
    }

    long get_cpu_by_row(int sender_id) {
        boost::lock_guard<SpinLock> l(_lock);
        auto statistic = find(sender_id);
        return statistic->get_cpu_by_row();
    }

    long get_io_by_byte(int sender_id) {
        boost::lock_guard<SpinLock> l(_lock);
        auto statistic = find(sender_id);
        return statistic->get_io_by_byte();
    }

    void clear() {
        boost::lock_guard<SpinLock> l(_lock);
        auto iter = _statistics.begin();
        while (iter != _statistics.end()) {
            iter->second->reset();
            iter++;
        }
    }

    ~QueryStatistic() {
        boost::lock_guard<SpinLock> l(_lock);
        auto iter = _statistics.begin();
        while (iter != _statistics.end()) {
            delete iter->second;
            iter++; 
       }   
    }

private:

    Statistic* find(int sender_id) {
       auto iter = _statistics.find(sender_id);
       Statistic* statistic = nullptr;
       if (iter == _statistics.end()) {
           statistic = new Statistic();
           _statistics[sender_id] = statistic;
       } else {
           statistic = iter->second;
       }
       return statistic; 
    }

    Statistic get_total_statistic() {
        Statistic total_statistic;
        auto iter = _statistics.begin();
        while (iter != _statistics.end()) {
           total_statistic.add(*(iter->second));
           iter++;
        }   
        return total_statistic;
    }  

    // Map lock.
    SpinLock _lock;
    // Sender id to statistic.
    std::map<int, Statistic*> _statistics;
    // To index current plan. 
    static const int DEFAULT_SENDER_ID = -1;
};

}

#endif
