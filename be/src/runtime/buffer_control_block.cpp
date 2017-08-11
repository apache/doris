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

#include "runtime/buffer_control_block.h"
#include "runtime/raw_value.h"
#include "gen_cpp/PaloInternalService_types.h"

namespace palo {

BufferControlBlock::BufferControlBlock(const TUniqueId& id, int buffer_size)
    : _fragment_id(id),
      _is_close(false),
      _is_cancelled(false),
      _buffer_rows(0),
      _buffer_limit(buffer_size),
      _packet_num(0) {
}

BufferControlBlock::~BufferControlBlock() {
    cancel();

    for (ResultQueue::iterator iter = _batch_queue.begin(); _batch_queue.end() != iter; ++iter) {
        delete *iter;
        *iter = NULL;
    }
}

Status BufferControlBlock::init() {
    return Status::OK;
}

Status BufferControlBlock::add_batch(TFetchDataResult* result) {
    boost::unique_lock<boost::mutex> l(_lock);

    if (_is_cancelled) {
        return Status::CANCELLED;
    }

    int num_rows = result->result_batch.rows.size();

    while ((!_batch_queue.empty() && (num_rows + _buffer_rows) > _buffer_limit)
            && !_is_cancelled) {
        _data_removal.wait(l);
    }

    if (_is_cancelled) {
        return Status::CANCELLED;
    }

    _buffer_rows += num_rows;
    _batch_queue.push_back(result);
    _data_arriaval.notify_one();
    return Status::OK;
}

Status BufferControlBlock::get_batch(TFetchDataResult* result) {
    TFetchDataResult* item = NULL;
    {
        boost::unique_lock<boost::mutex> l(_lock);

        while (_batch_queue.empty() && !_is_close && !_is_cancelled) {
            _data_arriaval.wait(l);
        }

        // if Status has been set, return fail;
        RETURN_IF_ERROR(_status);

        // cancelled
        if (_is_cancelled) {
            return Status::CANCELLED;
        }

        if (_batch_queue.empty()) {
            if (_is_close) {
                // no result, normal end
                result->eos = true;
                result->__set_packet_num(_packet_num);
                _packet_num++;
                return Status::OK;
            } else {
                // can not get here
                return Status("Internal error, can not Get here!");
            }
        }

        // get result
        item = _batch_queue.front();
        _batch_queue.pop_front();
        _buffer_rows -= item->result_batch.rows.size();
        _data_removal.notify_one();
    }
    *result = *item;
    result->__set_packet_num(_packet_num);
    _packet_num++;
    // destruct item new from Result writer
    delete item;
    item = NULL;

    return Status::OK;
}

Status BufferControlBlock::close(Status exec_status) {
    boost::unique_lock<boost::mutex> l(_lock);
    _is_close = true;
    _status = exec_status;

    // notify blocked get thread
    _data_arriaval.notify_all();
    return Status::OK;
}

Status BufferControlBlock::cancel() {
    boost::unique_lock<boost::mutex> l(_lock);
    _is_cancelled = true;
    _data_removal.notify_all();
    _data_arriaval.notify_all();
    return Status::OK;
}

}
/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */
