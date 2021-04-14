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

#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/raw_value.h"
#include "service/brpc.h"
#include "util/thrift_util.h"

namespace doris {

void GetResultBatchCtx::on_failure(const Status& status) {
    DCHECK(!status.ok()) << "status is ok, errmsg=" << status.get_error_msg();
    status.to_protobuf(result->mutable_status());
    done->Run();
    delete this;
}

void GetResultBatchCtx::on_close(int64_t packet_seq, QueryStatistics* statistics) {
    Status status;
    status.to_protobuf(result->mutable_status());
    if (statistics != nullptr) {
        statistics->to_pb(result->mutable_query_statistics());
    }
    result->set_packet_seq(packet_seq);
    result->set_eos(true);
    done->Run();
    delete this;
}

void GetResultBatchCtx::on_data(TFetchDataResult* t_result, int64_t packet_seq, bool eos) {
    Status st = Status::OK();
    if (t_result != nullptr) {
        uint8_t* buf = nullptr;
        uint32_t len = 0;
        ThriftSerializer ser(false, 4096);
        st = ser.serialize(&t_result->result_batch, &len, &buf);
        if (st.ok()) {
            if (resp_in_attachment) {
                // TODO(yangzhengguo) this is just for compatible with old version, this should be removed in the release 0.15
                cntl->response_attachment().append(buf, len);
            } else {
                result->set_row_batch(std::string((const char*)buf, len));
            }
            result->set_packet_seq(packet_seq);
            result->set_eos(eos);
        } else {
            LOG(WARNING) << "TFetchDataResult serialize failed, errmsg=" << st.get_error_msg();
        }
    } else {
        result->set_empty_batch(true);
        result->set_packet_seq(packet_seq);
        result->set_eos(eos);
    }
    st.to_protobuf(result->mutable_status());
    done->Run();
    delete this;
}

BufferControlBlock::BufferControlBlock(const TUniqueId& id, int buffer_size)
        : _fragment_id(id),
          _is_close(false),
          _is_cancelled(false),
          _buffer_rows(0),
          _buffer_limit(buffer_size),
          _packet_num(0) {}

BufferControlBlock::~BufferControlBlock() {
    cancel();

    for (ResultQueue::iterator iter = _batch_queue.begin(); _batch_queue.end() != iter; ++iter) {
        delete *iter;
        *iter = NULL;
    }
}

Status BufferControlBlock::init() {
    return Status::OK();
}

Status BufferControlBlock::add_batch(TFetchDataResult* result) {
    boost::unique_lock<boost::mutex> l(_lock);

    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    int num_rows = result->result_batch.rows.size();

    while ((!_batch_queue.empty() && (num_rows + _buffer_rows) > _buffer_limit) && !_is_cancelled) {
        _data_removal.wait(l);
    }

    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    if (_waiting_rpc.empty()) {
        _buffer_rows += num_rows;
        _batch_queue.push_back(result);
        _data_arrival.notify_one();
    } else {
        auto ctx = _waiting_rpc.front();
        _waiting_rpc.pop_front();
        ctx->on_data(result, _packet_num);
        delete result;
        _packet_num++;
    }
    return Status::OK();
}

Status BufferControlBlock::get_batch(TFetchDataResult* result) {
    TFetchDataResult* item = NULL;
    {
        boost::unique_lock<boost::mutex> l(_lock);

        while (_batch_queue.empty() && !_is_close && !_is_cancelled) {
            _data_arrival.wait(l);
        }

        // if Status has been set, return fail;
        RETURN_IF_ERROR(_status);

        // cancelled
        if (_is_cancelled) {
            return Status::Cancelled("Cancelled");
        }

        if (_batch_queue.empty()) {
            if (_is_close) {
                // no result, normal end
                result->eos = true;
                result->__set_packet_num(_packet_num);
                _packet_num++;
                return Status::OK();
            } else {
                // can not get here
                return Status::InternalError("Internal error, can not Get here!");
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

    return Status::OK();
}

void BufferControlBlock::get_batch(GetResultBatchCtx* ctx) {
    boost::lock_guard<boost::mutex> l(_lock);
    if (!_status.ok()) {
        ctx->on_failure(_status);
        return;
    }
    if (_is_cancelled) {
        ctx->on_failure(Status::Cancelled("Cancelled"));
        return;
    }
    if (!_batch_queue.empty()) {
        // get result
		TFetchDataResult* result = _batch_queue.front();
		_batch_queue.pop_front();
		_buffer_rows -= result->result_batch.rows.size();
		_data_removal.notify_one();

		ctx->on_data(result, _packet_num);
		_packet_num++;

		delete result;
		result = nullptr;

        return;
    }
    if (_is_close) {
        ctx->on_close(_packet_num, _query_statistics.get());
        return;
    }
    // no ready data, push ctx to waiting list
    _waiting_rpc.push_back(ctx);
}

Status BufferControlBlock::close(Status exec_status) {
    boost::unique_lock<boost::mutex> l(_lock);
    _is_close = true;
    _status = exec_status;

    // notify blocked get thread
    _data_arrival.notify_all();
    if (!_waiting_rpc.empty()) {
        if (_status.ok()) {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_close(_packet_num, _query_statistics.get());
            }
        } else {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_failure(_status);
            }
        }
        _waiting_rpc.clear();
    }
    return Status::OK();
}

Status BufferControlBlock::cancel() {
    boost::unique_lock<boost::mutex> l(_lock);
    _is_cancelled = true;
    _data_removal.notify_all();
    _data_arrival.notify_all();
    for (auto& ctx : _waiting_rpc) {
        ctx->on_failure(Status::Cancelled("Cancelled"));
    }
    _waiting_rpc.clear();
    return Status::OK();
}

} // namespace doris
