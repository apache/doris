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

#include <condition_variable>

#include "core/block/block.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"

namespace doris::flight {

namespace {
// Poll cancellation on the RPC thread: no background thread may retain ServerCallContext.
template <typename Response>
class FlightReadCallback : public DummyBrpcCallback<Response> {
public:
    void call() override {
        std::lock_guard lock(_mutex);
        _done = true;
        _cv.notify_all();
    }

    void wait(const std::function<bool()>& is_cancelled) {
        std::unique_lock lock(_mutex);
        while (!_done) {
            if (is_cancelled()) {
                lock.unlock();
                brpc::StartCancel(this->call_id_);
                this->join();
                return;
            }
            _cv.wait_for(lock, std::chrono::milliseconds(20));
        }
        lock.unlock();
        this->join();
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _done = false;
};
} // namespace

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

bool ArrowFlightBatchReaderBase::is_cancelled() const {
    return _closed.load() || (_is_cancelled && _is_cancelled());
}

void ArrowFlightBatchReaderBase::close(const Status& reason) {
    if (!_closed.exchange(true) && !_eof.load() && _cancel_query) {
        _cancel_query(reason);
    }
}

arrow::Status ArrowFlightBatchReaderBase::Close() {
    close(Status::Cancelled("Arrow Flight result stream closed before EOF"));
    return arrow::Status::OK();
}

arrow::Status ArrowFlightBatchReaderBase::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    *out = nullptr;
    if (is_cancelled()) {
        (void)Close();
        return arrow::Status::Cancelled("Arrow Flight fetch cancelled");
    }
    auto status = [&]() -> arrow::Status {
        RETURN_ARROW_STATUS_IF_CATCH_EXCEPTION(ReadNextImpl(out));
    }();
    if (!status.ok()) {
        close(to_doris_status(status));
    } else if (!*out) {
        _eof = true;
    }
    return status;
}

ArrowFlightBatchReaderBase::~ArrowFlightBatchReaderBase() {
    // Transport errors can destroy a stream without another ReadNext/Close call.
    (void)Close();
    LOG(INFO) << fmt::format(
            "ArrowFlightBatchReader finished, packet_seq={}, result_addr={}:{}, finistId={}, "
            "convert_arrow_batch_timer={}, deserialize_block_timer={}, peak_memory_usage={}",
            _packet_seq, _statement->result_addr.hostname, _statement->result_addr.port,
            print_id(_statement->query_id), _convert_arrow_batch_timer, _deserialize_block_timer,
            _mem_tracker ? _mem_tracker->peak_consumption() : 0);
}

ArrowFlightBatchLocalReader::ArrowFlightBatchLocalReader(
        const std::shared_ptr<QueryStatement>& statement,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker)
        : ArrowFlightBatchReaderBase(statement) {
    _schema = schema;
    _mem_tracker = mem_tracker;
    _cancel_query = [id = statement->query_id](const Status& reason) {
        ExecEnv::GetInstance()->result_mgr()->cancel_arrow_flight_query(id, reason);
    };
}

arrow::Result<std::shared_ptr<ArrowFlightBatchLocalReader>> ArrowFlightBatchLocalReader::Create(
        const std::shared_ptr<QueryStatement>& statement, std::function<bool()> is_cancelled) {
    DCHECK(statement->result_addr.hostname == BackendOptions::get_localhost());
    std::shared_ptr<ArrowFlightResultBlockBuffer> arrow_buffer;
    RETURN_ARROW_STATUS_IF_ERROR(
            ExecEnv::GetInstance()->result_mgr()->find_buffer(statement->query_id, arrow_buffer));
    // Make sure that FE send the fragment to BE and creates the BufferControlBlock before returning ticket
    // to the ADBC client, so that the schema and control block can be found.
    std::shared_ptr<arrow::Schema> schema;
    RETURN_ARROW_STATUS_IF_ERROR(arrow_buffer->get_schema(&schema));
    std::shared_ptr<MemTrackerLimiter> mem_tracker = arrow_buffer->mem_tracker();
    std::shared_ptr<ArrowFlightBatchLocalReader> result(
            new ArrowFlightBatchLocalReader(statement, schema, mem_tracker));
    result->_is_cancelled = std::move(is_cancelled);
    arrow_buffer->get_timezone(result->_timezone_obj);
    return result;
}

arrow::Status ArrowFlightBatchLocalReader::ReadNextImpl(std::shared_ptr<arrow::RecordBatch>* out) {
    // parameter *out not nullptr
    *out = nullptr;
    SCOPED_ATTACH_TASK(_mem_tracker);
    TUniqueId tid = UniqueId(_statement->query_id).to_thrift();
    std::shared_ptr<ArrowFlightResultBlockBuffer> arrow_buffer;
    RETURN_ARROW_STATUS_IF_ERROR(
            ExecEnv::GetInstance()->result_mgr()->find_buffer(tid, arrow_buffer));
    std::shared_ptr<Block> result;
    bool eos = false;
    while (!result && !eos) {
        if (is_cancelled()) {
            return arrow::Status::Cancelled("Arrow Flight fetch cancelled");
        }
        auto st = arrow_buffer->get_arrow_batch(&result, &eos);
        st.prepend("ArrowFlightBatchLocalReader fetch arrow data failed");
        ARROW_RETURN_NOT_OK(to_arrow_status(st));
    }
    if (eos) {
        // eof, normal path end
        return arrow::Status::OK();
    }

    {
        // convert one batch
        SCOPED_ATOMIC_TIMER(&_convert_arrow_batch_timer);
        auto st = ArrowFlightArrowBlockConvertor(_schema, _timezone_obj)
                          .convert_to_arrow(*result, arrow::default_memory_pool(), out);
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
    _cancel_query = [stub, id = statement->query_id](const Status&) {
        auto request = std::make_shared<PFetchArrowDataRequest>();
        request->mutable_finst_id()->set_hi(id.hi);
        request->mutable_finst_id()->set_lo(id.lo);
        request->set_cancel(true);
        auto callback = DummyBrpcCallback<PFetchArrowDataResult>::create_shared();
        auto closure = AutoReleaseClosure<
                PFetchArrowDataRequest,
                DummyBrpcCallback<PFetchArrowDataResult>>::create_unique(request, callback);
        // Bound teardown even if the result BE is unavailable.
        callback->cntl_->set_timeout_ms(1000);
        stub->fetch_arrow_data(closure->cntl_.get(), closure->request_.get(),
                               closure->response_.get(), closure.get());
        closure.release();
        callback->join();
        if (callback->cntl_->Failed()) {
            LOG(WARNING) << "Failed to cancel Arrow Flight result " << print_id(id) << ": "
                         << callback->cntl_->ErrorText();
        }
    };
}

arrow::Result<std::shared_ptr<ArrowFlightBatchRemoteReader>> ArrowFlightBatchRemoteReader::Create(
        const std::shared_ptr<QueryStatement>& statement, std::function<bool()> is_cancelled) {
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
    result->_is_cancelled = std::move(is_cancelled);
    ARROW_RETURN_NOT_OK(result->init_schema());
    return result;
}

arrow::Status ArrowFlightBatchRemoteReader::_fetch_schema() {
    Status st;
    auto request = std::make_shared<PFetchArrowFlightSchemaRequest>();
    auto* pfinst_id = request->mutable_finst_id();
    pfinst_id->set_hi(_statement->query_id.hi);
    pfinst_id->set_lo(_statement->query_id.lo);
    auto callback = std::make_shared<FlightReadCallback<PFetchArrowFlightSchemaResult>>();
    auto closure = AutoReleaseClosure<
            PFetchArrowFlightSchemaRequest,
            FlightReadCallback<PFetchArrowFlightSchemaResult>>::create_unique(request, callback);
    callback->cntl_->set_timeout_ms(config::arrow_flight_reader_brpc_controller_timeout_ms);
    callback->cntl_->ignore_eovercrowded();

    _brpc_stub->fetch_arrow_flight_schema(closure->cntl_.get(), closure->request_.get(),
                                          closure->response_.get(), closure.get());
    closure.release();
    callback->wait([this] { return is_cancelled(); });
    if (is_cancelled()) {
        return arrow::Status::Cancelled("Arrow Flight fetch cancelled");
    }

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
        auto callback = std::make_shared<FlightReadCallback<PFetchArrowDataResult>>();
        auto closure = AutoReleaseClosure<
                PFetchArrowDataRequest,
                FlightReadCallback<PFetchArrowDataResult>>::create_unique(request, callback);
        callback->cntl_->set_timeout_ms(config::arrow_flight_reader_brpc_controller_timeout_ms);
        callback->cntl_->ignore_eovercrowded();

        _brpc_stub->fetch_arrow_data(closure->cntl_.get(), closure->request_.get(),
                                     closure->response_.get(), closure.get());
        closure.release();
        callback->wait([this] { return is_cancelled(); });
        if (is_cancelled()) {
            return arrow::Status::Cancelled("Arrow Flight fetch cancelled");
        }

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
            _block = Block::create_shared();
            [[maybe_unused]] size_t uncompressed_size = 0;
            [[maybe_unused]] int64_t uncompressed_time = 0;
            st = _block->deserialize(callback->response_->block(), &uncompressed_size,
                                     &uncompressed_time);
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

arrow::Status ArrowFlightBatchRemoteReader::ReadNextImpl(std::shared_ptr<arrow::RecordBatch>* out) {
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
        auto st = ArrowFlightArrowBlockConvertor(_schema, _timezone_obj)
                          .convert_to_arrow(*_block, arrow::default_memory_pool(), out);
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
