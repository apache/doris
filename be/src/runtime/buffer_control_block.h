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

#ifndef DORIS_BE_RUNTIME_BUFFER_CONTROL_BLOCK_H
#define DORIS_BE_RUNTIME_BUFFER_CONTROL_BLOCK_H

#include <list>
#include <deque>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_node_consumption_provider.h"

namespace google {
namespace protobuf {
class Closure;
}
}

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

    GetResultBatchCtx(brpc::Controller* cntl_,
                      PFetchDataResult* result_,
                      google::protobuf::Closure* done_)
        : cntl(cntl_), result(result_), done(done_) {
    }

    void on_failure(const Status& status);
    void on_close(int64_t packet_seq, ExecNodeConsumptionProvider::Consumption* consumption = nullptr);
    void on_data(TFetchDataResult* t_result, int64_t packet_seq, bool eos = false);
};

// buffer used for result customer and productor
class BufferControlBlock {
public:
    BufferControlBlock(const TUniqueId& id, int buffer_size);
    ~BufferControlBlock();

    Status init();
    Status add_batch(TFetchDataResult* result);

    // get result from batch, use timeout?
    Status get_batch(TFetchDataResult* result);

    void get_batch(GetResultBatchCtx* ctx);

    // close buffer block, set _status to exec_status and set _is_close to true;
    // called because data has been read or error happend.
    Status close(Status exec_status);
    // this is called by RPC, called from coordinator
    Status cancel();

    const TUniqueId& fragment_id() const {
        return _fragment_id;
    }

    void set_query_consumption(const ExecNodeConsumptionProvider::Consumption& consumption) {
        _consumption = consumption;
    }
private:
    typedef std::list<TFetchDataResult*> ResultQueue;

    // result's query id
    TUniqueId _fragment_id;
    bool _is_close;
    bool _is_cancelled;
    Status _status;
    int _buffer_rows;
    int _buffer_limit;
    int64_t _packet_num;

    // blocking queue for batch
    ResultQueue _batch_queue;
    // protects all subsequent data in this block
    boost::mutex _lock;
    // signal arrival of new batch or the eos/cancelled condition
    boost::condition_variable _data_arriaval;
    // signal removal of data by stream consumer
    boost::condition_variable _data_removal;
   
    std::deque<GetResultBatchCtx*> _waiting_rpc;

    ExecNodeConsumptionProvider::Consumption _consumption;
};

}

#endif
