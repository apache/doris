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

#ifndef DORIS_RECORD_STORE_IMP_H
#define DORIS_RECORD_STORE_IMP_H

#include "udf/udf_internal.h"
#include "runtime/free_pool.hpp"
#include "runtime/runtime_state.h"
#include "runtime/descriptors.h"

namespace doris {

doris_udf::RecordStore* RecordStoreImpl::create_record_store(FreePool* free_pool, TupleDescriptor* descriptor) {
    doris_udf::RecordStore *store = new doris_udf::RecordStore();
    store->_impl->_free_pool = free_pool;
    store->_impl->_descriptor = descriptor;

    store->_impl->_size = descriptor->byte_size();
    store->_impl->_data = store->_impl->_free_pool->allocate(descriptor->byte_size());
    return store;
}

doris_udf::Record *RecordStoreImpl::allocate_record() {
    uint8_t *bytes = _free_pool->allocate(_descriptor->byte_size());
    doris_udf::Record *record = new doris_udf::Record(bytes, _descriptor);
    return record;
}

void RecordStoreImpl::append_record(doris_udf::Record *record) {
    if (_descriptor->byte_size() > (_size - _used_size)) {
        _data = _free_pool->reallocate(_data, _size * 2);
        _size = _size * 2;
    }
    memcpy((uint8_t *) _data + _used_size, (uint8_t *) record->get_data(), _descriptor->byte_size());

    _used_size += _descriptor->byte_size();
}

void RecordStoreImpl::free_record(doris_udf::Record *record) {
    _free_pool->free((uint8_t *) record);
}

void *RecordStoreImpl::allocate(size_t byte_size) {
    uint8_t* buffer = _free_pool->allocate(byte_size);
    _allocations.push_back(buffer);
    return buffer;
}

uint8_t *RecordStoreImpl::get_record(int idx) {
    return _data + idx * _descriptor->byte_size();
}

RecordStoreImpl::~RecordStoreImpl() {
    for (int i = 0; i < _allocations.size(); ++i) {
        _free_pool->free(_allocations[i]);
    }
    _allocations.clear();
}

}

#endif //DORIS_RECORD_STORE_IMP_H