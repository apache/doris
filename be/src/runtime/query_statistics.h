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

#include "gen_cpp/data.pb.h"
#include "util/spinlock.h"

namespace doris {

// This is responsible for collecting query statistics, usually it consists of 
// two parts, one is current fragment or plan's statistics, the other is sub fragment
// or plan's statistics and QueryStatisticsRecvr is responsible for collecting it.
class QueryStatistics {
public:

    QueryStatistics() : scan_rows(0), scan_bytes(0) {
    }

    void add(const QueryStatistics& other) {
        scan_rows += other.scan_rows;
        scan_bytes += other.scan_bytes;
    }

    void add_scan_rows(long scan_rows) {
        this->scan_rows += scan_rows;
    }

    void add_scan_bytes(long scan_bytes) {
        this->scan_bytes += scan_bytes;
    }

    void clear() {
        scan_rows = 0;
        scan_bytes = 0;
    }

    void serialize(PQueryStatistics* statistics) {
        DCHECK(statistics != nullptr);
        statistics->set_scan_rows(scan_rows);
        statistics->set_scan_bytes(scan_bytes);
    }

    void deserialize(const PQueryStatistics& statistics) {
        scan_rows = statistics.scan_rows();
        scan_bytes = statistics.scan_bytes();
    }

private:

    long scan_rows;
    long scan_bytes;
};

// It is used for collecting sub plan query statistics in DataStreamRecvr.
class QueryStatisticsRecvr {
public:

    void insert(const PQueryStatistics& statistics, int sender_id) {
        boost::lock_guard<SpinLock> l(_lock);
        QueryStatistics* query_statistics = nullptr;
        auto iter = _query_statistics.find(sender_id);
        if (iter == _query_statistics.end()) {
            query_statistics = new QueryStatistics;
            _query_statistics[sender_id] = query_statistics;
        } else {
            query_statistics = iter->second;
        }
        query_statistics->deserialize(statistics);
    }

    void add_to(QueryStatistics* statistics) {
        boost::lock_guard<SpinLock> l(_lock);
        auto iter = _query_statistics.begin();
        while (iter != _query_statistics.end()) {
            statistics->add(*(iter->second));
            iter++;
        }
    }
   
    ~QueryStatisticsRecvr() {
        // It is unnecessary to lock here, because the destructor will be
        // called alter DataStreamRecvr's close in ExchangeNode.
        auto iter = _query_statistics.begin();
        while (iter != _query_statistics.end()) {
            delete iter->second;
            iter++;
        }
        _query_statistics.clear();
   }
 
private:
    
    std::map<int, QueryStatistics*> _query_statistics;
    SpinLock _lock;
};

}

#endif
