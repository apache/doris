//
// Created by 刘航源 on 2019/10/22.
//

#ifndef DORIS_RECORD_STORE_H
#define DORIS_RECORD_STORE_H

#include <boost/cstdint.hpp>
#include <cstring>
#include <runtime/descriptors.h>

namespace doris {
class RecordStoreImpl;

class Record {
public:
    // Set idx field to null
    void set_null(int idx) {
        NullIndicatorOffset offset =  _descriptor->slots()[idx]->null_indicator_offset();
        char* null_indicator_byte = reinterpret_cast<char*>(_data) + offset.byte_offset;
        *null_indicator_byte |= offset.bit_mask;
    }

    // set idx filed to val as int
    void set_int(int idx, int val) {
        uint8_t* dst = (uint8_t*) _data + _descriptor->slots()[idx]->tuple_offset();
        memcpy(dst, reinterpret_cast<uint8_t*>(&val), sizeof(int));
    }

    // set idx filed to ptr with len as string, this function will
    // use input buffer directly without copy. Client should allocate
    // memory from RecordStore.
    void set_string(int idx, const uint8_t *ptr, size_t len) {
         uint8_t* dst = (uint8_t*) _data + _descriptor->slots()[idx]->tuple_offset();
         memcpy(dst, &ptr, sizeof(char*));
         memcpy(dst + sizeof(size_t) ,reinterpret_cast<uint8_t*>(&len) ,sizeof(size_t));
    }

    Record(uint8_t* data, TupleDescriptor* descriptor) {
        _descriptor = descriptor;
        _data = data;
        memset(_data,0, _descriptor->byte_size());
    }

    void* get_data() { return _data; }
private:
    TupleDescriptor* _descriptor;
    void* _data;
};

class RecordStore {
public:
    // Allocate a record to store data.
    // Returned record can be added to this store through calling
    // append_record function. If returned record is not added back,
    // client should call free_record to free it.
    Record *allocate_record();

    // Append a record to this store. The input record must be returned
    // by allocate_record function of this RecordStore. Otherwise
    // undefined error would happen.
    void append_record(Record *record);

    // This function is to free the unused record created by allocate_record
    // function.
    void free_record(Record *record);

    // Allocate memory for variable length filed in record, such as string
    // type. The allocated memory need not to be freed by client, they will
    // be freed when this store is destroyed.
    void *allocate(size_t size);

    uint8_t* get_record(int idx);

private:
    friend class doris::RecordStoreImpl;
    RecordStore();
    RecordStoreImpl *_impl; // Owned by this object.
};

}

#endif //DORIS_RECORD_STORE_H
