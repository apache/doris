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

#ifndef DORIS_BE_SRC_QUERY_EXEC_SCAN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_SCAN_NODE_H

#include <string>
#include "exec/exec_node.h"
#include "util/runtime_profile.h"
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

class TScanRange;

// Abstract base class of all scan nodes; introduces set_scan_range().
//
// Includes ScanNode common counters:
//   BytesRead - total bytes read by this scan node
//
//   TotalRawHdfsReadTime - it measures the total time spent in the disk-io-mgr's reading
//     threads for this node. For example, if we have 3 reading threads and each spent
//     1 sec, this counter will report 3 sec.
//
//   TotalReadThroughput - BytesRead divided by the total time spent in this node
//     (from Open to Close). For IO bounded queries, this should be very close to the
//     total throughput of all the disks.
//
//   PerDiskRawHdfsThroughput - the read throughput for each disk. If all the data reside
//     on disk, this should be the read throughput the disk, regardless of whether the
//     query is IO bounded or not.
//
//   NumDisksAccessed - number of disks accessed.
//
//   AverageIoMgrQueueCapcity - the average queue capacity in the io mgr for this node.
//   AverageIoMgrQueueSize - the average queue size (for ready buffers) in the io mgr
//     for this node.
//
//   AverageScannerThreadConcurrency - the average number of active scanner threads. A
//     scanner thread is considered active if it is not blocked by IO. This number would
//     be low (less than 1) for IO bounded queries. For cpu bounded queries, this number
//     would be close to the max scanner threads allowed.
//
//   AverageHdfsReadThreadConcurrency - the average number of active hdfs reading threads
//     reading for this scan node. For IO bound queries, this should be close to the
//     number of disk.
//
//     HdfsReadThreadConcurrencyCount=<i> - the number of samples taken when the hdfs read
//       thread concurrency is <i>.
//
//   ScanRangesComplete - number of scan ranges completed
//
//   MaterializeTupleTime - time spent in creating in-memory tuple format
//
//   ScannerThreadsTotalWallClockTime - total time spent in all scanner threads.
//
//   ScannerThreadsUserTime, ScannerThreadsSysTime,
//   ScannerThreadsVoluntaryContextSwitches, ScannerThreadsInvoluntaryContextSwitches -
//     these are aggregated counters across all scanner threads of this scan node. They
//     are taken from getrusage. See RuntimeProfile::ThreadCounters for details.
//
class ScanNode : public ExecNode {
public:
    ScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}
    virtual ~ScanNode() { }

    // Set up counters
    virtual Status prepare(RuntimeState* state);

    // Convert scan_ranges into node-specific scan restrictions.  This should be
    // called after prepare()
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) = 0;

    virtual bool is_scan_node() const {
        return true;
    }

    RuntimeProfile::Counter* bytes_read_counter() const {
        return _bytes_read_counter;
    }
    RuntimeProfile::Counter* rows_read_counter() const {
        return _rows_read_counter;
    }
    RuntimeProfile::Counter* read_timer() const {
        return _read_timer;
    }
    RuntimeProfile::Counter* total_throughput_counter() const {
        return _total_throughput_counter;
    }
    RuntimeProfile::Counter* per_read_thread_throughput_counter() const {
        return _per_read_thread_throughput_counter;
    }
    RuntimeProfile::Counter* materialize_tuple_timer() const {
        return _materialize_tuple_timer;
    }
    RuntimeProfile::ThreadCounters* scanner_thread_counters() const {
        return _scanner_thread_counters;
    }

    // names of ScanNode common counters
    static const std::string _s_bytes_read_counter;
    static const std::string _s_rows_read_counter;
    static const std::string _s_total_read_timer;
    static const std::string _s_total_throughput_counter;
    static const std::string _s_per_read_thread_throughput_counter;
    static const std::string _s_num_disks_accessed_counter;
    static const std::string _s_materialize_tuple_timer;
    static const std::string _s_scanner_thread_counters_prefix;
    static const std::string _s_scanner_thread_total_wallclock_time;
    static const std::string _s_average_io_mgr_queue_capacity;
    static const std::string _s_num_scanner_threads_started;

protected:
    RuntimeProfile::Counter* _bytes_read_counter; // # bytes read from the scanner
    // # rows/tuples read from the scanner (including those discarded by eval_conjucts())
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer; // total read time
    // Wall based aggregate read throughput [bytes/sec]
    RuntimeProfile::Counter* _total_throughput_counter;
    // Per thread read throughput [bytes/sec]
    RuntimeProfile::Counter* _per_read_thread_throughput_counter;
    RuntimeProfile::Counter* _num_disks_accessed_counter;
    RuntimeProfile::Counter* _materialize_tuple_timer;  // time writing tuple slots
    // Aggregated scanner thread counters
    RuntimeProfile::ThreadCounters* _scanner_thread_counters;
    RuntimeProfile::Counter* _num_scanner_threads_started_counter;
};

}

#endif
