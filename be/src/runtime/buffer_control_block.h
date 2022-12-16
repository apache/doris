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

#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/query_statistics.h"

namespace google {
namespace protobuf {
class Closure;
}
} // namespace google

namespace brpc {
class Controller;
}

namespace doris {

class TFetchDataResult;
class PFetchDataResult;

struct GetResultBatchCtx {
    brpc::Controller* cntl = nullptr;
    PFetchDataResult* result = nullptr;
    google::protobuf::Closure* done = nullptr;

    GetResultBatchCtx(brpc::Controller* cntl_, PFetchDataResult* result_,
                      google::protobuf::Closure* done_)
            : cntl(cntl_), result(result_), done(done_) {}

    void on_failure(const Status& status);
    void on_close(int64_t packet_seq, QueryStatistics* statistics = nullptr);
    void on_data(const std::unique_ptr<TFetchDataResult>& t_result, int64_t packet_seq,
                 bool eos = false);
};

// buffer used for result customer and producer
class BufferControlBlock {
public:
    BufferControlBlock(const TUniqueId& id, int buffer_size);
    virtual ~BufferControlBlock();

    Status init();
    virtual bool can_sink(); // 只有一个fragment写入，因此can_sink返回true，则一定可以执行sink
    Status add_batch(std::unique_ptr<TFetchDataResult>& result);

    // get result from batch, use timeout?
    Status get_batch(TFetchDataResult* result);

    void get_batch(GetResultBatchCtx* ctx);

    // close buffer block, set _status to exec_status and set _is_close to true;
    // called because data has been read or error happened.
    Status close(Status exec_status);
    // this is called by RPC, called from coordinator
    Status cancel();

    const TUniqueId& fragment_id() const { return _fragment_id; }

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
        _query_statistics = statistics;
    }

    void update_num_written_rows(int64_t num_rows) {
        // _query_statistics may be null when the result sink init failed
        // or some other failure.
        // and the number of written rows is only needed when all things go well.
        if (_query_statistics != nullptr) {
            _query_statistics->set_returned_rows(num_rows);
        }
    }

    // TODO: The value of query peak mem usage in fe.audit.log comes from a random BE,
    // not the BE with the largest peak mem usage
    void update_max_peak_memory_bytes() {
        if (_query_statistics != nullptr) {
            int64_t max_peak_memory_bytes = _query_statistics->calculate_max_peak_memory_bytes();
            _query_statistics->set_max_peak_memory_bytes(max_peak_memory_bytes);
        }
    }

protected:
    virtual bool _get_batch_queue_empty() { return _batch_queue.empty(); }
    virtual void _update_batch_queue_empty() {}

    using ResultQueue = std::list<std::unique_ptr<TFetchDataResult>>;

    // result's query id
    TUniqueId _fragment_id;
    bool _is_close;
    std::atomic_bool _is_cancelled;
    Status _status;
    std::atomic_int _buffer_rows;
    const int _buffer_limit;
    int64_t _packet_num;

    // blocking queue for batch
    ResultQueue _batch_queue;
    // protects all subsequent data in this block
    std::mutex _lock;
    // signal arrival of new batch or the eos/cancelled condition
    std::condition_variable _data_arrival;
    // signal removal of data by stream consumer
    std::condition_variable _data_removal;

    std::deque<GetResultBatchCtx*> _waiting_rpc;

    // It is shared with PlanFragmentExecutor and will be called in two different
    // threads. But their calls are all at different time, there is no problem of
    // multithreading access.
    std::shared_ptr<QueryStatistics> _query_statistics;
};

class PipBufferControlBlock : public BufferControlBlock {
public:
    PipBufferControlBlock(const TUniqueId& id, int buffer_size)
            : BufferControlBlock(id, buffer_size) {}

    bool can_sink() override {
        return _get_batch_queue_empty() || _buffer_rows < _buffer_limit || _is_cancelled;
    }

private:
    bool _get_batch_queue_empty() override { return _batch_queue_empty; }
    void _update_batch_queue_empty() override { _batch_queue_empty = _batch_queue.empty(); }

    std::atomic_bool _batch_queue_empty = false;
};

} // namespace doris
