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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/function_service.pb.h>

#include <cstdint>
#include <memory>

#include "common/status.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"
#include "util/brpc_client_cache.h"
#include "util/jni-util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_rpc.h"
#include "vec/io/io_helper.h"
namespace doris::vectorized {

#define error_default_str "#$@"

constexpr int64_t max_buffered_rows = 4096;

struct AggregateRpcUdafData {
private:
    std::string _update_fn;
    std::string _merge_fn;
    std::string _server_addr;
    std::string _finalize_fn;
    bool _saved_last_result;
    std::shared_ptr<PFunctionService_Stub> _client;
    PFunctionCallResponse _res;
    std::vector<PFunctionCallRequest> _buffer_request;
    bool _error;

public:
    AggregateRpcUdafData() = default;
    AggregateRpcUdafData(int64_t num_args) { set_last_result(false); }

    bool has_last_result() { return _saved_last_result == true; }

    void set_last_result(bool flag) { _saved_last_result = flag; }

    ~AggregateRpcUdafData() {}

    void set_error(bool flag) { _error = flag; }

    bool has_error() { return _error == true; }

    Status merge(AggregateRpcUdafData& rhs) {
        static_cast<void>(send_buffer_to_rpc_server());
        if (has_last_result()) {
            PFunctionCallRequest request;
            PFunctionCallResponse response;
            brpc::Controller cntl;
            PFunctionCallResponse current_res = rhs.get_result();
            request.set_function_name(_merge_fn);
            //last result
            PValues* arg = request.add_args();
            arg->CopyFrom(_res.result(0));
            arg = request.add_args();
            //current result
            arg->CopyFrom(current_res.result(0));
            //send to rpc server  that impl the merge op, the will save the result
            RETURN_IF_ERROR(send_rpc_request(cntl, request, response));
            _res = response;
        } else {
            _res = rhs.get_result();
            set_last_result(true);
        }
        return Status::OK();
    }

    Status init(const TFunction& fn) {
        _update_fn = fn.aggregate_fn.update_fn_symbol;
        _merge_fn = fn.aggregate_fn.merge_fn_symbol;
        _server_addr = fn.hdfs_location;
        _finalize_fn = fn.aggregate_fn.finalize_fn_symbol;
        set_error(false);
        _client = ExecEnv::GetInstance()->brpc_function_client_cache()->get_client(_server_addr);
        if (_client == nullptr) {
            std::string err_msg = "init rpc error, addr:" + _server_addr;
            LOG(ERROR) << err_msg;
            set_error(true);
            return Status::InternalError(err_msg);
        }
        return Status::OK();
    }

    Status send_rpc_request(brpc::Controller& cntl, PFunctionCallRequest& request,
                            PFunctionCallResponse& response) {
        _client->fn_call(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            set_error(true);
            std::stringstream err_msg;
            err_msg << " call rpc function failed";
            err_msg << " _server_addr:" << _server_addr;
            err_msg << " function_name:" << request.function_name();
            err_msg << " err:" << cntl.ErrorText();
            LOG(ERROR) << err_msg.str();
            return Status::InternalError(err_msg.str());
        }
        if (!response.has_status() || response.result_size() == 0) {
            set_error(true);
            std::stringstream err_msg;
            err_msg << " call rpc function failed, status or result is not set";
            err_msg << " _server_addr:" << _server_addr;
            err_msg << " function_name:" << request.function_name();
            LOG(ERROR) << err_msg.str();
            return Status::InternalError(err_msg.str());
        }
        if (response.status().status_code() != 0) {
            set_error(true);
            std::stringstream err_msg;
            err_msg << " call rpc function failed";
            err_msg << " _server_addr:" << _server_addr;
            err_msg << " function_name:" << request.function_name();
            err_msg << " err:" << response.status().DebugString();
            LOG(ERROR) << err_msg.str();
            return Status::InternalError(err_msg.str());
        }
        return Status::OK();
    }

    Status gen_request_data(PFunctionCallRequest& request, const IColumn** columns, int start,
                            int end, const DataTypes& argument_types) {
        for (int i = 0; i < argument_types.size(); i++) {
            PValues* arg = request.add_args();
            auto data_type = argument_types[i];
            if (auto st = data_type->get_serde()->write_column_to_pb(*columns[i], *arg, start, end);
                !st.ok()) {
                return st;
            }
        }
        return Status::OK();
    }

#define ADD_VALUE(TYPEVALUE)                                           \
    if (_buffer_request[j].args(i).TYPEVALUE##_##size() > 0) {         \
        arg->add_##TYPEVALUE(_buffer_request[j].args(i).TYPEVALUE(0)); \
    }

    PFunctionCallRequest merge_buffer_request(PFunctionCallRequest& request) {
        int args_size = _buffer_request[0].args_size();
        request.set_function_name(_update_fn);
        for (int i = 0; i < args_size; i++) {
            PValues* arg = request.add_args();
            arg->mutable_type()->CopyFrom(_buffer_request[0].args(i).type());
            for (int j = 0; j < _buffer_request.size(); j++) {
                ADD_VALUE(double_value);
                ADD_VALUE(float_value);
                ADD_VALUE(int32_value);
                ADD_VALUE(int64_value);
                ADD_VALUE(uint32_value);
                ADD_VALUE(uint64_value);
                ADD_VALUE(bool_value);
                ADD_VALUE(string_value);
                ADD_VALUE(bytes_value);
            }
        }
        return request;
    }

    // called in group agg op
    Status buffer_add(const IColumn** columns, int start, int end,
                      const DataTypes& argument_types) {
        PFunctionCallRequest request;
        static_cast<void>(gen_request_data(request, columns, start, end, argument_types));
        _buffer_request.push_back(request);
        if (_buffer_request.size() >= max_buffered_rows) {
            static_cast<void>(send_buffer_to_rpc_server());
        }
        return Status::OK();
    }

    //clear buffer request
    Status send_buffer_to_rpc_server() {
        if (_buffer_request.size() > 0) {
            PFunctionCallRequest request;
            PFunctionCallResponse response;
            brpc::Controller cntl;
            merge_buffer_request(request);
            if (has_last_result()) {
                request.mutable_context()
                        ->mutable_function_context()
                        ->mutable_args_data()
                        ->CopyFrom(_res.result());
            }
            RETURN_IF_ERROR(send_rpc_request(cntl, request, response));
            _res = response;
            set_last_result(true);
            _buffer_request.clear();
        }
        return Status::OK();
    }

    Status add(const IColumn** columns, int start, int end, const DataTypes& argument_types) {
        PFunctionCallRequest request;
        PFunctionCallResponse response;
        brpc::Controller cntl;
        request.set_function_name(_update_fn);
        static_cast<void>(gen_request_data(request, columns, start, end, argument_types));
        if (has_last_result()) {
            request.mutable_context()->mutable_function_context()->mutable_args_data()->CopyFrom(
                    _res.result());
        }
        RETURN_IF_ERROR(send_rpc_request(cntl, request, response));
        _res = response;
        set_last_result(true);
        return Status::OK();
    }

    void serialize(BufferWritable& buf) {
        static_cast<void>(send_buffer_to_rpc_server());
        std::string serialize_data = error_default_str;
        if (!has_error()) {
            serialize_data = _res.SerializeAsString();
        } else {
            LOG(ERROR) << "serialize empty buf";
        }
        write_binary(serialize_data, buf);
    }

    void deserialize(BufferReadable& buf) {
        static_cast<void>(send_buffer_to_rpc_server());
        std::string serialize_data;
        read_binary(serialize_data, buf);
        if (error_default_str != serialize_data) {
            _res.ParseFromString(serialize_data);
            set_last_result(true);
        } else {
            LOG(ERROR) << "deserialize empty buf";
            set_error(true);
        }
    }

#define GETDATA(LOCATTYPE, TYPEVALUE)                                                          \
    if (response.result_size() > 0 && response.result(0).TYPEVALUE##_##value_size() > 0) {     \
        LOCATTYPE ret = response.result(0).TYPEVALUE##_##value(0);                             \
        to.insert_data((char*)&ret, 0);                                                        \
    } else {                                                                                   \
        LOG(ERROR) << "_server_addr:" << _server_addr << ",_finalize_fn:" << _finalize_fn      \
                   << ",msg: failed to get final result cause return type need " << #TYPEVALUE \
                   << "but result is empty";                                                   \
        to.insert_default();                                                                   \
    }

    //if any unexpected error happen will return NULL
    Status get(IColumn& to, const DataTypePtr& return_type) {
        if (has_error()) {
            to.insert_default();
            return Status::OK();
        }
        static_cast<void>(send_buffer_to_rpc_server());
        PFunctionCallRequest request;
        PFunctionCallResponse response;
        brpc::Controller cntl;
        request.set_function_name(_finalize_fn);
        request.mutable_context()->mutable_function_context()->mutable_args_data()->CopyFrom(
                _res.result());
        static_cast<void>(send_rpc_request(cntl, request, response));
        if (has_error()) {
            to.insert_default();
            return Status::OK();
        }
        DataTypePtr result_type = return_type;
        if (return_type->is_nullable()) {
            result_type =
                    reinterpret_cast<const DataTypeNullable*>(return_type.get())->get_nested_type();
        }
        WhichDataType which(result_type);
        if (which.is_float32()) {
            GETDATA(float, float);
        } else if (which.is_float64()) {
            GETDATA(double, double);
        } else if (which.is_int32()) {
            GETDATA(int32_t, int32);
        } else if (which.is_uint32()) {
            GETDATA(uint32_t, uint32);
        } else if (which.is_int64()) {
            GETDATA(int64_t, int64);
        } else if (which.is_uint64()) {
            GETDATA(uint64_t, uint64);
        } else if (which.is_uint8()) {
            GETDATA(uint8_t, bool);
        } else if (which.is_string()) {
            if (response.result_size() > 0 && response.result(0).string_value_size() > 0) {
                std::string ret = response.result(0).string_value(0);
                to.insert_data(ret.c_str(), ret.size());
            } else {
                LOG(ERROR) << "_server_addr:" << _server_addr << ",_finalize_fn:" << _finalize_fn
                           << ",msg: failed to get final result cause return type need string but "
                              "result is empty";
                to.insert_default();
            }
        } else {
            LOG(ERROR) << "failed to get result cause unkown return type";
            to.insert_default();
        }
        return Status::OK();
    }

    PFunctionCallResponse get_result() {
        static_cast<void>(send_buffer_to_rpc_server());
        return _res;
    }
};

class AggregateRpcUdaf final
        : public IAggregateFunctionDataHelper<AggregateRpcUdafData, AggregateRpcUdaf> {
public:
    AggregateRpcUdaf(const TFunction& fn, const DataTypes& argument_types_,
                     const DataTypePtr& return_type)
            : IAggregateFunctionDataHelper(argument_types_), _fn(fn), _return_type(return_type) {}
    ~AggregateRpcUdaf() = default;

    static AggregateFunctionPtr create(const TFunction& fn, const DataTypes& argument_types_,
                                       const DataTypePtr& return_type) {
        return std::make_shared<AggregateRpcUdaf>(fn, argument_types_, return_type);
    }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data(argument_types.size());
        Status status = Status::OK();
        SAFE_CREATE(RETURN_IF_STATUS_ERROR(status, data(place).init(_fn)),
                    this->data(place).~Data());
    }

    String get_name() const override { return _fn.name.function_name; }

    DataTypePtr get_return_type() const override { return _return_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        static_cast<void>(
                this->data(place).buffer_add(columns, row_num, row_num + 1, argument_types));
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        static_cast<void>(this->data(place).add(columns, 0, batch_size, argument_types));
    }

    void reset(AggregateDataPtr place) const override {}

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        static_cast<void>(this->data(place).merge(this->data(const_cast<AggregateDataPtr>(rhs))));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(const_cast<AggregateDataPtr&>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        static_cast<void>(this->data(const_cast<AggregateDataPtr>(place)).get(to, _return_type));
    }

private:
    TFunction _fn;
    DataTypePtr _return_type;
};

} // namespace doris::vectorized
