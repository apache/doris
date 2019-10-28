//
// Created by 刘航源 on 2019/10/22.
//

#include "record_store.h"
#include "record_store_impl.h"

namespace doris {

RecordStore::RecordStore() : _impl(new doris::RecordStoreImpl(this)) {}


// Allocate a record to store data.
// Returned record can be added to this store through calling
// append_record function. If returned record is not added back,
// client should call free_record to free it.
Record *RecordStore::allocate_record() {
    return _impl->allocate_record();
}

// Append a record to this store. The input record must be returned
// by allocate_record function of this RecordStore. Otherwise
// undefined error would happen.
void RecordStore::append_record(Record *record) {
    _impl->append_record(record);
}

// This function is to free the unused record created by allocate_record
// function.
void RecordStore::free_record(Record *record) {
    _impl->free_record(record);
}

// Allocate memory for variable length filed in record, such as string
// type. The allocated memory need not to be freed by client, they will
// be freed when this store is destroyed.
void* RecordStore::allocate(size_t size) {
    return (void*)_impl->allocate(size);
}

}