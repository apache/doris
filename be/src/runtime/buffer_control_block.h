// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_RUNTIME_BUFFER_CONTROL_BLOCK_H
#define BDG_PALO_BE_RUNTIME_BUFFER_CONTROL_BLOCK_H

#include <list>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include "common/status.h"
#include "gen_cpp/Types_types.h"

namespace palo {

class TFetchDataResult;

// buffer used for result customer and productor
class BufferControlBlock {
public:
    BufferControlBlock(const TUniqueId& id, int buffer_size);
    ~BufferControlBlock();

    Status init();
    Status add_batch(TFetchDataResult* result);
    // get result from batch, use timeout?
    Status get_batch(TFetchDataResult* result);
    // close buffer block, set _status to exec_status and set _is_close to true;
    // called because data has been read or error happend.
    Status close(Status exec_status);
    // this is called by RPC, called from coordinator
    Status cancel();

    const TUniqueId& fragment_id() const {
        return _fragment_id;
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
    int _packet_num;

    // blocking queue for batch
    ResultQueue _batch_queue;
    // protects all subsequent data in this block
    boost::mutex _lock;
    // signal arrival of new batch or the eos/cancelled condition
    boost::condition_variable _data_arriaval;
    // signal removal of data by stream consumer
    boost::condition_variable _data_removal;
};

}

#endif
