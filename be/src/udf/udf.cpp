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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/udf/udf.cpp
// and modified by Doris

#include "udf/udf.h"

#include <assert.h>

#include <iostream>
#include <sstream>

#include "common/logging.h"
#include "gen_cpp/types.pb.h"
#include "olap/hll.h"
#include "runtime/decimalv2_value.h"

// Be careful what this includes since this needs to be linked into the UDF's
// binary. For example, it would be unfortunate if they had a random dependency
// on libhdfs.
#include "udf/udf_internal.h"
#include "util/debug_util.h"

#if DORIS_UDF_SDK_BUILD
// For the SDK build, we are building the .lib that the developers would use to
// write UDFs. They want to link against this to run their UDFs in a test environment.
// Pulling in free-pool is very undesirable since it pulls in many other libraries.
// Instead, we'll implement a dummy version that is not used.
// When they build their library to a .so, they'd use the version of FunctionContext
// in the main binary, which does include FreePool.
namespace doris {
class FreePool {
public:
    FreePool(MemPool*) {}

    uint8_t* allocate(int byte_size) { return reinterpret_cast<uint8_t*>(malloc(byte_size)); }

    uint8_t* reallocate(uint8_t* ptr, int byte_size) {
        return reinterpret_cast<uint8_t*>(realloc(ptr, byte_size));
    }

    void free(uint8_t* ptr) { ::free(ptr); }
};

class RuntimeState {
public:
    void set_process_status(const std::string& error_msg) { assert(false); }

    bool log_error(const std::string& error) {
        assert(false);
        return false;
    }

    const std::string& user() const { return _user; }

private:
    std::string _user = "";
};
} // namespace doris
#else
#include "runtime/free_pool.hpp"
#include "runtime/runtime_state.h"
#endif

namespace doris {

FunctionContextImpl::FunctionContextImpl(doris_udf::FunctionContext* parent)
        : _varargs_buffer(nullptr),
          _varargs_buffer_size(0),
          _num_updates(0),
          _num_removes(0),
          _context(parent),
          _pool(nullptr),
          _state(nullptr),
          _debug(false),
          _version(doris_udf::FunctionContext::V2_0),
          _num_warnings(0),
          _thread_local_fn_state(nullptr),
          _fragment_local_fn_state(nullptr),
          _external_bytes_tracked(0),
          _closed(false) {}

void FunctionContextImpl::close() {
    if (_closed) {
        return;
    }

    // Free local allocations first so we can detect leaks through any remaining allocations
    // (local allocations cannot be leaked, at least not by the UDF)
    free_local_allocations();

    if (_external_bytes_tracked > 0) {
        // This isn't ideal because the memory is still leaked, but don't track it so our
        // accounting stays sane.
        // TODO: we need to modify the memtrackers to allow leaked user-allocated memory.
        _context->free(_external_bytes_tracked);
    }

    free(_varargs_buffer);
    _varargs_buffer = nullptr;

    _closed = true;
}

uint8_t* FunctionContextImpl::allocate_local(int64_t byte_size) {
    uint8_t* buffer = _pool->allocate(byte_size);
    _local_allocations.push_back(buffer);
    return buffer;
}

void FunctionContextImpl::free_local_allocations() {
    for (int i = 0; i < _local_allocations.size(); ++i) {
        _pool->free(_local_allocations[i]);
    }

    _local_allocations.clear();
}

void FunctionContextImpl::set_constant_args(const std::vector<doris_udf::AnyVal*>& constant_args) {
    _constant_args = constant_args;
}

void FunctionContextImpl::set_constant_cols(
        const std::vector<doris::ColumnPtrWrapper*>& constant_cols) {
    _constant_cols = constant_cols;
}

bool FunctionContextImpl::check_allocations_empty() {
    if (_allocations.empty() && _external_bytes_tracked == 0) {
        return true;
    }

    // TODO: fix this
    //if (_debug) _context->set_error("Leaked allocations.");
    return false;
}

bool FunctionContextImpl::check_local_allocations_empty() {
    if (_local_allocations.empty()) {
        return true;
    }

    // TODO: fix this
    //if (_debug) _context->set_error("Leaked local allocations.");
    return false;
}

doris_udf::FunctionContext* FunctionContextImpl::create_context(
        RuntimeState* state, MemPool* pool, const doris_udf::FunctionContext::TypeDesc& return_type,
        const std::vector<doris_udf::FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size,
        bool debug) {
    doris_udf::FunctionContext::TypeDesc invalid_type;
    invalid_type.type = doris_udf::FunctionContext::INVALID_TYPE;
    invalid_type.precision = 0;
    invalid_type.scale = 0;
    return FunctionContextImpl::create_context(state, pool, invalid_type, return_type, arg_types,
                                               varargs_buffer_size, debug);
}

doris_udf::FunctionContext* FunctionContextImpl::create_context(
        RuntimeState* state, MemPool* pool,
        const doris_udf::FunctionContext::TypeDesc& intermediate_type,
        const doris_udf::FunctionContext::TypeDesc& return_type,
        const std::vector<doris_udf::FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size,
        bool debug) {
    auto* ctx = new doris_udf::FunctionContext();
    ctx->_impl->_state = state;
    ctx->_impl->_pool = new FreePool(pool);
    ctx->_impl->_intermediate_type = intermediate_type;
    ctx->_impl->_return_type = return_type;
    ctx->_impl->_arg_types = arg_types;
    ctx->_impl->_varargs_buffer = reinterpret_cast<uint8_t*>(malloc(varargs_buffer_size));
    ctx->_impl->_varargs_buffer_size = varargs_buffer_size;
    ctx->_impl->_debug = debug;
    VLOG_ROW << "Created FunctionContext: " << ctx << " with pool " << ctx->_impl->_pool;
    return ctx;
}

FunctionContext* FunctionContextImpl::clone(MemPool* pool) {
    doris_udf::FunctionContext* new_context =
            create_context(_state, pool, _intermediate_type, _return_type, _arg_types,
                           _varargs_buffer_size, _debug);
    new_context->_impl->_constant_args = _constant_args;
    new_context->_impl->_constant_cols = _constant_cols;
    new_context->_impl->_fragment_local_fn_state = _fragment_local_fn_state;
    return new_context;
}

} // namespace doris

namespace doris_udf {
static const int MAX_WARNINGS = 1000;

FunctionContext* FunctionContext::create_test_context() {
    FunctionContext* context = new FunctionContext();
    context->impl()->_debug = true;
    context->impl()->_state = nullptr;
    context->impl()->_pool = new doris::FreePool(nullptr);
    return context;
}

FunctionContext::FunctionContext() : _impl(new doris::FunctionContextImpl(this)) {}

FunctionContext::~FunctionContext() {
    // TODO: this needs to free local allocations but there's a mem issue
    // in the uda harness now.
    _impl->check_local_allocations_empty();
    _impl->check_allocations_empty();
    delete _impl->_pool;
    delete _impl;
}

FunctionContext::DorisVersion FunctionContext::version() const {
    return _impl->_version;
}

const char* FunctionContext::user() const {
    if (_impl->_state == nullptr) {
        return nullptr;
    }

    return _impl->_state->user().c_str();
}

FunctionContext::UniqueId FunctionContext::query_id() const {
    UniqueId id;
#if DORIS_UDF_SDK_BUILD
    id.hi = id.lo = 0;
#else
    id.hi = _impl->_state->query_id().hi;
    id.lo = _impl->_state->query_id().lo;
#endif
    return id;
}

bool FunctionContext::has_error() const {
    return !_impl->_error_msg.empty();
}

const char* FunctionContext::error_msg() const {
    if (has_error()) {
        return _impl->_error_msg.c_str();
    }

    return nullptr;
}

uint8_t* FunctionContext::allocate(int byte_size) {
    uint8_t* buffer = _impl->_pool->allocate(byte_size);
    _impl->_allocations[buffer] = byte_size;

    if (_impl->_debug) {
        memset(buffer, 0xff, byte_size);
    }

    return buffer;
}

uint8_t* FunctionContext::reallocate(uint8_t* ptr, int byte_size) {
    _impl->_allocations.erase(ptr);
    ptr = _impl->_pool->reallocate(ptr, byte_size);
    _impl->_allocations[ptr] = byte_size;
    return ptr;
}

void FunctionContext::free(uint8_t* buffer) {
    if (buffer == nullptr) {
        return;
    }

    if (_impl->_debug) {
        std::map<uint8_t*, int>::iterator it = _impl->_allocations.find(buffer);

        if (it != _impl->_allocations.end()) {
            // fill in garbage value into the buffer to increase the chance of detecting misuse
            memset(buffer, 0xff, it->second);
            _impl->_allocations.erase(it);
            _impl->_pool->free(buffer);
        } else {
            set_error("FunctionContext::free() on buffer that is not freed or was not allocated.");
        }
    } else {
        _impl->_allocations.erase(buffer);
        _impl->_pool->free(buffer);
    }
}

void FunctionContext::track_allocation(int64_t bytes) {
    _impl->_external_bytes_tracked += bytes;
}

void FunctionContext::free(int64_t bytes) {
    _impl->_external_bytes_tracked -= bytes;
}

void FunctionContext::set_function_state(FunctionStateScope scope, void* ptr) {
    assert(!_impl->_closed);
    switch (scope) {
    case THREAD_LOCAL:
        _impl->_thread_local_fn_state = ptr;
        break;
    case FRAGMENT_LOCAL:
        _impl->_fragment_local_fn_state = ptr;
        break;
    default:
        std::stringstream ss;
        ss << "Unknown FunctionStateScope: " << scope;
        set_error(ss.str().c_str());
    }
}

void FunctionContext::set_error(const char* error_msg) {
    if (_impl->_error_msg.empty()) {
        _impl->_error_msg = error_msg;
        std::stringstream ss;
        ss << "UDF ERROR: " << error_msg;

        if (_impl->_state != nullptr) {
            _impl->_state->set_process_status(ss.str());
        }
    }
}

void FunctionContext::clear_error_msg() {
    _impl->_error_msg.clear();
}

bool FunctionContext::add_warning(const char* warning_msg) {
    if (_impl->_num_warnings++ >= MAX_WARNINGS) {
        return false;
    }

    std::stringstream ss;
    ss << "UDF WARNING: " << warning_msg;

    if (_impl->_state != nullptr) {
        return _impl->_state->log_error(ss.str());
    } else {
        std::cerr << ss.str() << std::endl;
        return true;
    }
}

StringVal::StringVal(FunctionContext* context, int64_t len)
        : len(len), ptr(context->impl()->allocate_local(len)) {}

bool StringVal::resize(FunctionContext* ctx, int64_t new_len) {
    if (new_len <= len) {
        len = new_len;
        return true;
    }
    if (UNLIKELY(new_len > StringVal::MAX_LENGTH)) {
        len = 0;
        is_null = true;
        return false;
    }
    auto* new_ptr = ctx->impl()->allocate_local(new_len);
    if (new_ptr != nullptr) {
        memcpy(new_ptr, ptr, len);
        ptr = new_ptr;
        len = new_len;
        return true;
    }
    return false;
}

StringVal StringVal::copy_from(FunctionContext* ctx, const uint8_t* buf, int64_t len) {
    StringVal result(ctx, len);
    if (!result.is_null) {
        memcpy(result.ptr, buf, len);
    }
    return result;
}

StringVal StringVal::create_temp_string_val(FunctionContext* ctx, int64_t len) {
    ctx->impl()->string_result().resize(len);
    return StringVal((uint8_t*)ctx->impl()->string_result().c_str(), len);
}

void StringVal::append(FunctionContext* ctx, const uint8_t* buf, int64_t buf_len) {
    if (UNLIKELY(len + buf_len > StringVal::MAX_LENGTH)) {
        ctx->set_error(
                "Concatenated string length larger than allowed limit of "
                "1 GB character data.");
        ctx->free(ptr);
        ptr = nullptr;
        len = 0;
        is_null = true;
    } else {
        ptr = ctx->reallocate(ptr, len + buf_len);
        memcpy(ptr + len, buf, buf_len);
        len += buf_len;
    }
}

void StringVal::append(FunctionContext* ctx, const uint8_t* buf, int64_t buf_len,
                       const uint8_t* buf2, int64_t buf2_len) {
    if (UNLIKELY(len + buf_len + buf2_len > StringVal::MAX_LENGTH)) {
        ctx->set_error(
                "Concatenated string length larger than allowed limit of "
                "1 GB character data.");
        ctx->free(ptr);
        ptr = nullptr;
        len = 0;
        is_null = true;
    } else {
        ptr = ctx->reallocate(ptr, len + buf_len + buf2_len);
        memcpy(ptr + len, buf, buf_len);
        memcpy(ptr + len + buf_len, buf2, buf2_len);
        len += buf_len + buf2_len;
    }
}

const FunctionContext::TypeDesc* FunctionContext::get_arg_type(int arg_idx) const {
    if (arg_idx < 0 || arg_idx >= _impl->_arg_types.size()) {
        return nullptr;
    }
    return &_impl->_arg_types[arg_idx];
}

void HllVal::init(FunctionContext* ctx) {
    len = doris::HLL_COLUMN_DEFAULT_LEN;
    ptr = ctx->allocate(len);
    memset(ptr, 0, len);
    // the HLL type is HLL_DATA_FULL in UDF or UDAF
    ptr[0] = doris::HllDataType::HLL_DATA_FULL;

    is_null = false;
}

void HllVal::agg_parse_and_cal(FunctionContext* ctx, const HllVal& other) {
    doris::HllSetResolver resolver;

    // zero size means the src input is a HyperLogLog object
    if (other.len == 0) {
        auto* hll = reinterpret_cast<doris::HyperLogLog*>(other.ptr);
        uint8_t* other_ptr = ctx->allocate(doris::HLL_COLUMN_DEFAULT_LEN);
        int other_len = hll->serialize(ptr);
        resolver.init((char*)other_ptr, other_len);
    } else {
        resolver.init((char*)other.ptr, other.len);
    }

    resolver.parse();

    if (resolver.get_hll_data_type() == doris::HLL_DATA_EMPTY) {
        return;
    }

    uint8_t* pdata = ptr + 1;
    int data_len = doris::HLL_REGISTERS_COUNT;

    if (resolver.get_hll_data_type() == doris::HLL_DATA_EXPLICIT) {
        for (int i = 0; i < resolver.get_explicit_count(); i++) {
            uint64_t hash_value = resolver.get_explicit_value(i);
            int idx = hash_value % data_len;
            uint8_t first_one_bit = __builtin_ctzl(hash_value >> doris::HLL_COLUMN_PRECISION) + 1;
            pdata[idx] = std::max(pdata[idx], first_one_bit);
        }
    } else if (resolver.get_hll_data_type() == doris::HLL_DATA_SPARSE) {
        std::map<doris::HllSetResolver::SparseIndexType, doris::HllSetResolver::SparseValueType>&
                sparse_map = resolver.get_sparse_map();
        for (std::map<doris::HllSetResolver::SparseIndexType,
                      doris::HllSetResolver::SparseValueType>::iterator iter = sparse_map.begin();
             iter != sparse_map.end(); ++iter) {
            pdata[iter->first] = std::max(pdata[iter->first], (uint8_t)iter->second);
        }
    } else if (resolver.get_hll_data_type() == doris::HLL_DATA_FULL) {
        char* full_value = resolver.get_full_value();
        for (int i = 0; i < doris::HLL_REGISTERS_COUNT; i++) {
            pdata[i] = std::max(pdata[i], (uint8_t)full_value[i]);
        }
    }
}

void HllVal::agg_merge(const HllVal& other) {
    uint8_t* pdata = ptr + 1;
    uint8_t* pdata_other = other.ptr + 1;

    for (int i = 0; i < doris::HLL_REGISTERS_COUNT; ++i) {
        pdata[i] = std::max(pdata[i], pdata_other[i]);
    }
}

bool FunctionContext::is_arg_constant(int i) const {
    if (i < 0 || i >= _impl->_constant_args.size()) {
        return false;
    }
    return _impl->_constant_args[i] != nullptr;
}

bool FunctionContext::is_col_constant(int i) const {
    if (i < 0 || i >= _impl->_constant_cols.size()) {
        return false;
    }
    return _impl->_constant_cols[i] != nullptr;
}

AnyVal* FunctionContext::get_constant_arg(int i) const {
    if (i < 0 || i >= _impl->_constant_args.size()) {
        return nullptr;
    }
    return _impl->_constant_args[i];
}

doris::ColumnPtrWrapper* FunctionContext::get_constant_col(int i) const {
    if (i < 0 || i >= _impl->_constant_cols.size()) {
        return nullptr;
    }
    return _impl->_constant_cols[i];
}

int FunctionContext::get_num_args() const {
    return _impl->_arg_types.size();
}

int FunctionContext::get_num_constant_args() const {
    return _impl->_constant_args.size();
}

const FunctionContext::TypeDesc& FunctionContext::get_return_type() const {
    return _impl->_return_type;
}

void* FunctionContext::get_function_state(FunctionStateScope scope) const {
    // assert(!_impl->_closed);
    switch (scope) {
    case THREAD_LOCAL:
        return _impl->_thread_local_fn_state;
        break;
    case FRAGMENT_LOCAL:
        return _impl->_fragment_local_fn_state;
        break;
    default:
        // TODO: signal error somehow
        return nullptr;
    }
}
std::ostream& operator<<(std::ostream& os, const StringVal& string_val) {
    return os << string_val.to_string();
}
} // namespace doris_udf
