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

#include "runtime/free_pool.hpp"
#include "runtime/runtime_state.h"
#include "runtime/descriptors.h"
#include "udf/udf_internal.h"

namespace doris {

doris_udf::RecordStore* RecordStoreImpl::create_record_store(FreePool* free_pool, TupleDescriptor* descriptor) {
    doris_udf::RecordStore *store = new doris_udf::RecordStore();
    store->_impl->_free_pool = free_pool;
    store->_impl->_descriptor = descriptor;
    return store;
}

doris_udf::Record *RecordStoreImpl::allocate_record() {
    uint8_t *bytes = _free_pool->allocate(sizeof(TupleDescriptor*) + _descriptor->byte_size());
    memcpy(bytes, &_descriptor, sizeof(TupleDescriptor*));
    return (doris_udf::Record*) (bytes + sizeof(TupleDescriptor*));
}

void *RecordStoreImpl::allocate(size_t byte_size) {
    uint8_t* buffer = _free_pool->allocate(byte_size);
    _allocations.push_back(buffer);
    return buffer;
}

void RecordStoreImpl::append_record(doris_udf::Record *record) {
    _record_vec.push_back(record);
}

void RecordStoreImpl::free_record(doris_udf::Record *record) {
    _free_pool->free((uint8_t *) record - sizeof(TupleDescriptor*));
}

doris_udf::Record *RecordStoreImpl::get(int idx) {
    return _record_vec[idx];
}

size_t RecordStoreImpl::size() {
    return _record_vec.size();
}

RecordStoreImpl::~RecordStoreImpl() {
    //TODO: free string type allocate ?
    /*
    for (int i = 0; i < _allocations.size(); ++i) {
        _free_pool->free(_allocations[i]);
    }
    _allocations.clear();
    */
}

}
