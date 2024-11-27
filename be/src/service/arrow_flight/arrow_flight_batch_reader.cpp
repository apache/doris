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

#include "service/arrow_flight/arrow_flight_batch_reader.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gen_cpp/internal_service.pb.h>

#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"
#include "util/brpc_client_cache.h"
#include "util/ref_count_closure.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris::flight {

ArrowFlightBatchReaderBase::ArrowFlightBatchReaderBase(
        const std::shared_ptr<QueryStatement>& statement)
        : _statement(statement) {}

std::shared_ptr<arrow::Schema> ArrowFlightBatchReaderBase::schema() const {
    return _schema;
}

arrow::Status ArrowFlightBatchReaderBase::_return_invalid_status(const std::string& msg) {
    std::string status_msg =
            fmt::format("ArrowFlightBatchReader {}, packet_seq={}, result={}:{}, finistId={}", msg,
                        _packet_seq, _statement->result_addr.hostname, _statement->result_addr.port,
                        print_id(_statement->query_id));
    LOG(WARNING) << status_msg;
    return arrow::Status::Invalid(status_msg);
}

ArrowFlightBatchReaderBase::~ArrowFlightBatchReaderBase() {
    VLOG_NOTICE << fmt::format(
            "ArrowFlightBatchReader finished, packet_seq={}, result_addr={}:{}, finistId={}, "
            "convert_arrow_batch_timer={}, deserialize_block_timer={}, peak_memory_usage={}",
            _packet_seq, _statement->result_addr.hostname, _statement->result_addr.port,
            print_id(_statement->query_id), _convert_arrow_batch_timer, _deserialize_block_timer,
            _mem_tracker->peak_consumption());
}

ArrowFlightBatchLocalReader::ArrowFlightBatchLocalReader(
        const std::shared_ptr<QueryStatement>& statement,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker)
        : ArrowFlightBatchReaderBase(statement) {
    _schema = schema;
    _mem_tracker = mem_tracker;
}

arrow::Result<std::shared_ptr<ArrowFlightBatchLocalReader>> ArrowFlightBatchLocalReader::Create(
        const std::shared_ptr<QueryStatement>& statement) {
    DCHECK(statement->result_addr.hostname == BackendOptions::get_localhost());
    // Make sure that FE send the fragment to BE and creates the BufferControlBlock before returning ticket
    // to the ADBC client, so that the schema and control block can be found.
    std::shared_ptr<arrow::Schema> schema;
    RETURN_ARROW_STATUS_IF_ERROR(
            ExecEnv::GetInstance()->result_mgr()->find_arrow_schema(statement->query_id, &schema));
    std::shared_ptr<MemTrackerLimiter> mem_tracker;
    RETURN_ARROW_STATUS_IF_ERROR(ExecEnv::GetInstance()->result_mgr()->find_mem_tracker(
            statement->query_id, &mem_tracker));

    std::shared_ptr<ArrowFlightBatchLocalReader> result(
            new ArrowFlightBatchLocalReader(statement, schema, mem_tracker));
    return result;
}

arrow::Status ArrowFlightBatchLocalReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    // parameter *out not nullptr
    *out = nullptr;
    SCOPED_ATTACH_TASK(_mem_tracker);
    std::shared_ptr<vectorized::Block> result;
    auto st = ExecEnv::GetInstance()->result_mgr()->fetch_arrow_data(_statement->query_id, &result,
                                                                     _timezone_obj);
    st.prepend("ArrowFlightBatchLocalReader fetch arrow data failed");
    ARROW_RETURN_NOT_OK(to_arrow_status(st));
    if (result == nullptr) {
        // eof, normal path end
        return arrow::Status::OK();
    }

    {
        // convert one batch
        SCOPED_ATOMIC_TIMER(&_convert_arrow_batch_timer);
        st = convert_to_arrow_batch(*result, _schema, arrow::default_memory_pool(), out,
                                    _timezone_obj);
        st.prepend("ArrowFlightBatchLocalReader convert block to arrow batch failed");
        ARROW_RETURN_NOT_OK(to_arrow_status(st));
    }

    _packet_seq++;
    if (*out != nullptr) {
        VLOG_NOTICE << "ArrowFlightBatchLocalReader read next: " << (*out)->num_rows() << ", "
                    << (*out)->num_columns() << ", packet_seq: " << _packet_seq;
    }
    return arrow::Status::OK();
}

ArrowFlightBatchRemoteReader::ArrowFlightBatchRemoteReader(
        const std::shared_ptr<QueryStatement>& statement,
        const std::shared_ptr<PBackendService_Stub>& stub)
        : ArrowFlightBatchReaderBase(statement), _brpc_stub(stub), _block(nullptr) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::QUERY,
            fmt::format("ArrowFlightBatchRemoteReader#QueryId={}", print_id(_statement->query_id)));
}

arrow::Result<std::shared_ptr<ArrowFlightBatchRemoteReader>> ArrowFlightBatchRemoteReader::Create(
        const std::shared_ptr<QueryStatement>& statement) {
    std::shared_ptr<PBackendService_Stub> stub =
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                    statement->result_addr);
    if (!stub) {
        std::string msg = fmt::format(
                "ArrowFlightBatchRemoteReader get rpc stub failed, result_addr={}:{}, finistId={}",
                statement->result_addr.hostname, statement->result_addr.port,
                print_id(statement->query_id));
        LOG(WARNING) << msg;
        return arrow::Status::Invalid(msg);
    }

    std::shared_ptr<ArrowFlightBatchRemoteReader> result(
            new ArrowFlightBatchRemoteReader(statement, stub));
    ARROW_RETURN_NOT_OK(result->init_schema());
    return result;
}

arrow::Status ArrowFlightBatchRemoteReader::_fetch_schema() {
    Status st;
    auto request = std::make_shared<PFetchArrowFlightSchemaRequest>();
    auto* pfinst_id = request->mutable_finst_id();
    pfinst_id->set_hi(_statement->query_id.hi);
    pfinst_id->set_lo(_statement->query_id.lo);
    auto callback = DummyBrpcCallback<PFetchArrowFlightSchemaResult>::create_shared();
    auto closure = AutoReleaseClosure<
            PFetchArrowFlightSchemaRequest,
            DummyBrpcCallback<PFetchArrowFlightSchemaResult>>::create_unique(request, callback);
    callback->cntl_->set_timeout_ms(config::arrow_flight_reader_brpc_controller_timeout_ms);
    callback->cntl_->ignore_eovercrowded();

    _brpc_stub->fetch_arrow_flight_schema(closure->cntl_.get(), closure->request_.get(),
                                          closure->response_.get(), closure.get());
    closure.release();
    callback->join();

    if (callback->cntl_->Failed()) {
        if (!ExecEnv::GetInstance()->brpc_internal_client_cache()->available(
                    _brpc_stub, _statement->result_addr.hostname, _statement->result_addr.port)) {
            ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                    callback->cntl_->remote_side());
        }
        auto error_code = callback->cntl_->ErrorCode();
        auto error_text = callback->cntl_->ErrorText();
        return _return_invalid_status(fmt::format("fetch schema error: {}, error_text: {}",
                                                  berror(error_code), error_text));
    }
    st = Status::create(callback->response_->status());
    ARROW_RETURN_NOT_OK(to_arrow_status(st));

    if (callback->response_->has_schema() && !callback->response_->schema().empty()) {
        auto input =
                arrow::io::BufferReader::FromString(std::string(callback->response_->schema()));
        ARROW_ASSIGN_OR_RAISE(auto reader,
                              arrow::ipc::RecordBatchStreamReader::Open(
                                      input.get(), arrow::ipc::IpcReadOptions::Defaults()));
        _schema = reader->schema();
    } else {
        return _return_invalid_status(fmt::format("fetch schema error: not find schema"));
    }
    return arrow::Status::OK();
}

arrow::Status ArrowFlightBatchRemoteReader::_fetch_data() {
    DCHECK(_block == nullptr);
    while (true) {
        // if `continue` occurs, data is invalid, continue fetch, block is nullptr.
        // if `break` occurs, fetch data successfully (block is not nullptr) or fetch eos.
        Status st;
        auto request = std::make_shared<PFetchArrowDataRequest>();
        auto* pfinst_id = request->mutable_finst_id();
        pfinst_id->set_hi(_statement->query_id.hi);
        pfinst_id->set_lo(_statement->query_id.lo);
        auto callback = DummyBrpcCallback<PFetchArrowDataResult>::create_shared();
        auto closure = AutoReleaseClosure<
                PFetchArrowDataRequest,
                DummyBrpcCallback<PFetchArrowDataResult>>::create_unique(request, callback);
        callback->cntl_->set_timeout_ms(config::arrow_flight_reader_brpc_controller_timeout_ms);
        callback->cntl_->ignore_eovercrowded();

        _brpc_stub->fetch_arrow_data(closure->cntl_.get(), closure->request_.get(),
                                     closure->response_.get(), closure.get());
        closure.release();
        callback->join();

        if (callback->cntl_->Failed()) {
            if (!ExecEnv::GetInstance()->brpc_internal_client_cache()->available(
                        _brpc_stub, _statement->result_addr.hostname,
                        _statement->result_addr.port)) {
                ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                        callback->cntl_->remote_side());
            }
            auto error_code = callback->cntl_->ErrorCode();
            auto error_text = callback->cntl_->ErrorText();
            return _return_invalid_status(fmt::format("fetch data error={}, error_text: {}",
                                                      berror(error_code), error_text));
        }
        st = Status::create(callback->response_->status());
        ARROW_RETURN_NOT_OK(to_arrow_status(st));

        DCHECK(callback->response_->has_packet_seq());
        if (_packet_seq != callback->response_->packet_seq()) {
            return _return_invalid_status(
                    fmt::format("fetch data receive packet failed, expect: {}, receive: {}",
                                _packet_seq, callback->response_->packet_seq()));
        }
        _packet_seq++;

        if (callback->response_->has_eos() && callback->response_->eos()) {
            break;
        }

        if (callback->response_->has_empty_batch() && callback->response_->empty_batch()) {
            continue;
        }

        DCHECK(callback->response_->has_block());
        if (callback->response_->block().ByteSizeLong() == 0) {
            continue;
        }

        std::call_once(_timezone_once_flag, [this, callback] {
            DCHECK(callback->response_->has_timezone());
            TimezoneUtils::find_cctz_time_zone(callback->response_->timezone(), _timezone_obj);
        });

        {
            SCOPED_ATOMIC_TIMER(&_deserialize_block_timer);
            _block = vectorized::Block::create_shared();
            st = _block->deserialize(callback->response_->block());
            ARROW_RETURN_NOT_OK(to_arrow_status(st));
            break;
        }

        const auto rows = _block->rows();
        if (rows == 0) {
            _block = nullptr;
            continue;
        }
    }
    return arrow::Status::OK();
}

arrow::Status ArrowFlightBatchRemoteReader::init_schema() {
    ARROW_RETURN_NOT_OK(_fetch_schema());
    DCHECK(_schema != nullptr);
    return arrow::Status::OK();
}

arrow::Status ArrowFlightBatchRemoteReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    // parameter *out not nullptr
    *out = nullptr;
    SCOPED_ATTACH_TASK(_mem_tracker);
    ARROW_RETURN_NOT_OK(_fetch_data());
    if (_block == nullptr) {
        // eof, normal path end, last _fetch_data return block is nullptr
        return arrow::Status::OK();
    }
    {
        // convert one batch
        SCOPED_ATOMIC_TIMER(&_convert_arrow_batch_timer);
        auto st = convert_to_arrow_batch(*_block, _schema, arrow::default_memory_pool(), out,
                                         _timezone_obj);
        st.prepend("ArrowFlightBatchRemoteReader convert block to arrow batch failed");
        ARROW_RETURN_NOT_OK(to_arrow_status(st));
    }
    _block = nullptr;

    if (*out != nullptr) {
        VLOG_NOTICE << "ArrowFlightBatchRemoteReader read next: " << (*out)->num_rows() << ", "
                    << (*out)->num_columns() << ", packet_seq: " << _packet_seq;
    }
    return arrow::Status::OK();
}

} // namespace doris::flight
