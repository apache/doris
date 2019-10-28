//
// Created by 刘航源 on 2019/10/22.
//

#ifndef DORIS_RECORD_STORE_IMPL_H
#define DORIS_RECORD_STORE_IMPL_H

#include "runtime/mem_pool.h"
#include "runtime/descriptors.h"
#include "udf.h"
#include "record_store.h"


namespace doris {

class FreePool {
public:
    FreePool(MemPool* mem_pool) {
    }

    uint8_t *allocate(int byte_size) {
        return reinterpret_cast<uint8_t *>(malloc(byte_size));
    }

    uint8_t *reallocate(uint8_t *ptr, int byte_size) {
        return reinterpret_cast<uint8_t *>(realloc(ptr, byte_size));
    }

    void free(uint8_t *ptr) {
        free(ptr);
    }
};

class RecordStoreImpl {
public:
    static RecordStore *create_record_store(MemPool* pool, TupleDescriptor* descriptor);

    RecordStoreImpl(RecordStore *parent) {
        _data = _free_pool->allocate(_descriptor->byte_size());
    }

    ~RecordStoreImpl() {
        _mem_pool->free_all();
    }

    Record* allocate_record() {
        std::cout << "bytes : " <<  _descriptor->byte_size() << std::endl;
        uint8_t *bytes = _free_pool->allocate(_descriptor->byte_size());
        Record* record = new Record(bytes, _descriptor);
        return record;
    }

    void append_record(Record *record) {
        if (_descriptor->byte_size() > (_size - _used_size)) {
            _data =  _free_pool->reallocate(_data, _size * 2);
        }
        memcpy((uint8_t*)_data, (uint8_t*)record->get_data(), _descriptor->byte_size());
    }
    void free_record(Record *record) {
        _free_pool->free((uint8_t*) record);
    }

    void* allocate(size_t size) {
        return (void*) _mem_pool->allocate(size);
    }

private:

    FreePool* _free_pool;
    MemPool* _mem_pool;
    TupleDescriptor* _descriptor;
    uint8_t* _data;

    int _used_size = 0;
    int _size;
};

}
#endif //DORIS_RECORD_STORE_IMPL_H
