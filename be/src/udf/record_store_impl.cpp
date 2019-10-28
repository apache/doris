//
// Created by 刘航源 on 2019/10/22.
//

#ifndef DORIS_RECORD_STORE_IMP_H
#define DORIS_RECORD_STORE_IMP_H

#include "record_store_impl.h"

namespace doris {

RecordStore* RecordStoreImpl::create_record_store(MemPool* pool, TupleDescriptor* descriptor) {
    RecordStore* store = new RecordStore();
    store->_impl->_free_pool = new FreePool(pool);
    store->_impl->_mem_pool = pool;
    store->_impl->_descriptor = descriptor;
    store->_impl->_size = descriptor->byte_size();
    return store;
}

}

#endif //DORIS_RECORD_STORE_IMP_H