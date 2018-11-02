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

#ifndef DORIS_BE_SRC_QUERY_BE_RUNTIME_DATA_STREAM_RECVR_H
#define DORIS_BE_SRC_QUERY_BE_RUNTIME_DATA_STREAM_RECVR_H

#include "runtime/data_stream_mgr.h"

namespace doris {

class DataStreamMgr;

// Single receiver of an m:n data stream.
// Incoming row batches are routed to destinations based on the provided
// partitioning specification.
// Receivers are created via DataStreamMgr::CreateRecvr().
class DataStreamRecvr {
public:
    // deregister from _mgr
    ~DataStreamRecvr() {
        // TODO: log error msg
        _mgr->deregister_recvr(_cb->fragment_instance_id(), _cb->dest_node_id());
    }

    // Returns next row batch in data stream; blocks if there aren't any.
    // Returns NULL if eos (subsequent calls will not return any more batches).
    // Sets 'is_cancelled' to true if receiver fragment got cancelled, otherwise false.
    // The caller owns the batch.
    // TODO: error handling
    RowBatch* get_batch(bool* is_cancelled) {
        return _cb->get_batch(is_cancelled);
    }

    RuntimeProfile* profile() {
        return _cb->profile();
    }

private:
    friend class DataStreamMgr;
    DataStreamMgr* _mgr;
    boost::shared_ptr<DataStreamMgr::StreamControlBlock> _cb;

    DataStreamRecvr(DataStreamMgr* mgr,
                    boost::shared_ptr<DataStreamMgr::StreamControlBlock> cb)
        : _mgr(mgr), _cb(cb) {}
};

}

#endif
