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

#ifndef DORIS_BE_SRC_OLAP_FIELD_H
#define DORIS_BE_SRC_OLAP_FIELD_H

#include <sstream>
#include <string>

#include "olap/aggregate_func.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_cursor_cell.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "runtime/collection_value.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/mem_util.hpp"
#include "util/slice.h"

namespace doris {

// A Field is used to represent a column in memory format.
// User can use this class to access or deal with column data in memory.
class Field {
public:
    explicit Field() = default;
    explicit Field(const TabletColumn& column)
            : _type_info(get_type_info(&column)),
              _key_coder(get_key_coder(column.type())),
              _name(column.name()),
              _index_size(column.index_length()),
              _is_nullable(column.is_nullable()),
              _length(column.length()) {
        if (column.type() == OLAP_FIELD_TYPE_ARRAY) {
            _agg_info = get_aggregate_info(column.aggregation(), column.type(),
                                           column.get_sub_column(0).type());
        } else {
            _agg_info = get_aggregate_info(column.aggregation(), column.type());
        }
    }

    virtual ~Field() = default;

    inline size_t size() const { return _type_info->size(); }
    inline int32_t length() const { return _length; }
    inline size_t field_size() const { return size() + 1; }
    inline size_t index_size() const { return _index_size; }
    inline const std::string& name() const { return _name; }

    virtual inline void set_to_max(char* buf) const { return _type_info->set_to_max(buf); }
    inline void set_to_min(char* buf) const { return _type_info->set_to_min(buf); }

    // This function allocate memory from pool, other than allocate_memory
    // reserve memory from continuous memory.
    virtual inline char* allocate_value(MemPool* pool) const {
        return (char*)pool->allocate(_type_info->size());
    }

    inline void agg_update(RowCursorCell* dest, const RowCursorCell& src,
                           MemPool* mem_pool = nullptr) const {
        _agg_info->update(dest, src, mem_pool);
    }

    inline void agg_finalize(RowCursorCell* dst, MemPool* mem_pool) const {
        _agg_info->finalize(dst, mem_pool);
    }

    virtual void consume(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                         ObjectPool* agg_pool) const {
        _agg_info->init(dst, src, src_null, mem_pool, agg_pool);
    }

    // todo(kks): Unify AggregateInfo::init method and Field::agg_init method

    // This function will initialize destination with source.
    // This function differs copy function in that if this field
    // contain aggregate information, this function will initialize
    // destination in aggregate format, and update with source content.
    virtual void agg_init(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool,
                          ObjectPool* agg_pool) const {
        direct_copy(dst, src);
    }

    virtual char* allocate_memory(char* cell_ptr, char* variable_ptr) const { return variable_ptr; }

    virtual size_t get_variable_len() const { return 0; }

    virtual Field* clone() const {
        auto* local = new Field();
        this->clone(local);
        return local;
    }

    // Test if these two cell is equal with each other
    template <typename LhsCellType, typename RhsCellType>
    bool equal(const LhsCellType& lhs, const RhsCellType& rhs) const {
        bool l_null = lhs.is_null();
        bool r_null = rhs.is_null();

        if (l_null != r_null) {
            return false;
        } else if (l_null) {
            return true;
        } else {
            return _type_info->equal(lhs.cell_ptr(), rhs.cell_ptr());
        }
    }

    // Only compare column content, without considering NULL condition.
    // RETURNS:
    //      0 means equal,
    //      -1 means left less than right,
    //      1 means left bigger than right
    int compare(const void* left, const void* right) const { return _type_info->cmp(left, right); }

    // Compare two types of cell.
    // This function differs compare in that this function compare cell which
    // will consider the condition which cell may be NULL. While compare only
    // compare column content without considering NULL condition.
    // Only compare column content, without considering NULL condition.
    // RETURNS:
    //      0 means equal,
    //      -1 means left less than right,
    //      1 means left bigger than right
    template <typename LhsCellType, typename RhsCellType>
    int compare_cell(const LhsCellType& lhs, const RhsCellType& rhs) const {
        bool l_null = lhs.is_null();
        bool r_null = rhs.is_null();
        if (l_null != r_null) {
            return l_null ? -1 : 1;
        }
        return l_null ? 0 : _type_info->cmp(lhs.cell_ptr(), rhs.cell_ptr());
    }

    // Used to compare short key index. Because short key will truncate
    // a varchar column, this function will handle in this condition.
    template <typename LhsCellType, typename RhsCellType>
    inline int index_cmp(const LhsCellType& lhs, const RhsCellType& rhs) const;

    // Copy source cell's content to destination cell directly.
    // For string type, this function assume that destination has
    // enough space and copy source content into destination without
    // memory allocation.
    template <typename DstCellType, typename SrcCellType>
    void direct_copy(DstCellType* dst, const SrcCellType& src) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        return _type_info->direct_copy(dst->mutable_cell_ptr(), src.cell_ptr());
    }

    // deep copy source cell' content to destination cell.
    // For string type, this will allocate data form pool,
    // and copy source's content.
    template <typename DstCellType, typename SrcCellType>
    void copy_object(DstCellType* dst, const SrcCellType& src, MemPool* pool) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        _type_info->copy_object(dst->mutable_cell_ptr(), src.cell_ptr(), pool);
    }

    // deep copy source cell' content to destination cell.
    // For string type, this will allocate data form pool,
    // and copy source's content.
    template <typename DstCellType, typename SrcCellType>
    void deep_copy(DstCellType* dst, const SrcCellType& src, MemPool* pool) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        _type_info->deep_copy(dst->mutable_cell_ptr(), src.cell_ptr(), pool);
    }

    // deep copy field content from `src` to `dst` without null-byte
    inline void deep_copy_content(char* dst, const char* src, MemPool* mem_pool) const {
        _type_info->deep_copy(dst, src, mem_pool);
    }

    // shallow copy field content from `src` to `dst` without null-byte.
    // for string like type, shallow copy only copies Slice, not the actual data pointed by slice.
    inline void shallow_copy_content(char* dst, const char* src) const {
        _type_info->shallow_copy(dst, src);
    }

    //convert and copy field from src to desc
    inline OLAPStatus convert_from(char* dest, const char* src, const TypeInfo* src_type,
                                   MemPool* mem_pool) const {
        return _type_info->convert_from(dest, src, src_type, mem_pool);
    }

    // Copy source content to destination in index format.
    template <typename DstCellType, typename SrcCellType>
    void to_index(DstCellType* dst, const SrcCellType& src) const;

    // used by init scan key stored in string format
    // value_string should end with '\0'
    inline OLAPStatus from_string(char* buf, const std::string& value_string) const {
        return _type_info->from_string(buf, value_string);
    }

    // 将内部的value转成string输出
    // 没有考虑实现的性能，仅供DEBUG使用
    inline std::string to_string(const char* src) const { return _type_info->to_string(src); }

    template <typename CellType>
    std::string debug_string(const CellType& cell) const {
        std::stringstream ss;
        if (cell.is_null()) {
            ss << "(null)";
        } else {
            ss << _type_info->to_string(cell.cell_ptr());
        }
        return ss.str();
    }

    template <typename CellType>
    uint32_t hash_code(const CellType& cell, uint32_t seed) const;

    FieldType type() const { return _type_info->type(); }
    FieldAggregationMethod aggregation() const { return _agg_info->agg_method(); }
    const TypeInfo* type_info() const { return _type_info; }
    bool is_nullable() const { return _is_nullable; }

    // similar to `full_encode_ascending`, but only encode part (the first `index_size` bytes) of the value.
    // only applicable to string type
    void encode_ascending(const void* value, std::string* buf) const {
        _key_coder->encode_ascending(value, _index_size, buf);
    }

    // encode the provided `value` into `buf`.
    void full_encode_ascending(const void* value, std::string* buf) const {
        _key_coder->full_encode_ascending(value, buf);
    }

    Status decode_ascending(Slice* encoded_key, uint8_t* cell_ptr, MemPool* pool) const {
        return _key_coder->decode_ascending(encoded_key, _index_size, cell_ptr, pool);
    }
    void add_sub_field(std::unique_ptr<Field> sub_field) {
        _sub_fields.emplace_back(std::move(sub_field));
    }
    Field* get_sub_field(int i) { return _sub_fields[i].get(); }

protected:
    const TypeInfo* _type_info;

private:
    // Field的最大长度，单位为字节，通常等于length， 变长字符串不同
    const KeyCoder* _key_coder;
    std::string _name;
    uint16_t _index_size;
    bool _is_nullable;
    std::vector<std::unique_ptr<Field>> _sub_fields;

protected:
    const AggregateInfo* _agg_info;
    // 长度，单位为字节
    // 除字符串外，其它类型都是确定的
    uint32_t _length;

    char* allocate_string_value(MemPool* pool) const {
        char* type_value = (char*)pool->allocate(sizeof(Slice));
        auto slice = reinterpret_cast<Slice*>(type_value);
        slice->size = _length;
        slice->data = (char*)pool->allocate(slice->size);
        return type_value;
    }

    void clone(Field* other) const {
        other->_type_info = this->_type_info;
        other->_key_coder = this->_key_coder;
        other->_name = this->_name;
        other->_index_size = this->_index_size;
        other->_is_nullable = this->_is_nullable;
        other->_sub_fields.clear();
        for (const auto& f : _sub_fields) {
            Field* item = f->clone();
            other->add_sub_field(std::unique_ptr<Field>(item));
        }
    }
};

template <typename LhsCellType, typename RhsCellType>
int Field::index_cmp(const LhsCellType& lhs, const RhsCellType& rhs) const {
    bool l_null = lhs.is_null();
    bool r_null = rhs.is_null();
    if (l_null != r_null) {
        return l_null ? -1 : 1;
    } else if (l_null) {
        return 0;
    }

    int32_t res = 0;
    if (type() == OLAP_FIELD_TYPE_VARCHAR) {
        const Slice* l_slice = reinterpret_cast<const Slice*>(lhs.cell_ptr());
        const Slice* r_slice = reinterpret_cast<const Slice*>(rhs.cell_ptr());

        if (r_slice->size + OLAP_STRING_MAX_BYTES > _index_size ||
            l_slice->size + OLAP_STRING_MAX_BYTES > _index_size) {
            // 如果field的实际长度比short key长，则仅比较前缀，确保相同short key的所有block都被扫描，
            // 否则，可以直接比较short key和field
            int compare_size = _index_size - OLAP_STRING_MAX_BYTES;
            // l_slice size and r_slice size may be less than compare_size
            // so calculate the min of the three size as new compare_size
            compare_size = std::min(std::min(compare_size, (int)l_slice->size), (int)r_slice->size);

            // This function is used to compare prefix index.
            // Only the fixed length of prefix index should be compared.
            // If r_slice->size > l_slice->size, ignore the extra parts directly.
            res = strncmp(l_slice->data, r_slice->data, compare_size);
            if (res == 0 && compare_size != (_index_size - OLAP_STRING_MAX_BYTES)) {
                if (l_slice->size < r_slice->size) {
                    res = -1;
                } else if (l_slice->size > r_slice->size) {
                    res = 1;
                } else {
                    res = 0;
                }
            }
        } else {
            res = l_slice->compare(*r_slice);
        }
    } else {
        res = _type_info->cmp(lhs.cell_ptr(), rhs.cell_ptr());
    }

    return res;
}

template <typename DstCellType, typename SrcCellType>
void Field::to_index(DstCellType* dst, const SrcCellType& src) const {
    bool is_null = src.is_null();
    dst->set_is_null(is_null);
    if (is_null) {
        return;
    }

    if (type() == OLAP_FIELD_TYPE_VARCHAR) {
        // 先清零，再拷贝
        memset(dst->mutable_cell_ptr(), 0, _index_size);
        const Slice* slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        size_t copy_size = slice->size < _index_size - OLAP_STRING_MAX_BYTES
                                   ? slice->size
                                   : _index_size - OLAP_STRING_MAX_BYTES;
        *reinterpret_cast<StringLengthType*>(dst->mutable_cell_ptr()) = copy_size;
        memory_copy((char*)dst->mutable_cell_ptr() + OLAP_STRING_MAX_BYTES, slice->data, copy_size);
    } else if (type() == OLAP_FIELD_TYPE_CHAR) {
        // 先清零，再拷贝
        memset(dst->mutable_cell_ptr(), 0, _index_size);
        const Slice* slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        memory_copy(dst->mutable_cell_ptr(), slice->data, _index_size);
    } else {
        memory_copy(dst->mutable_cell_ptr(), src.cell_ptr(), size());
    }
}

template <typename CellType>
uint32_t Field::hash_code(const CellType& cell, uint32_t seed) const {
    bool is_null = cell.is_null();
    if (is_null) {
        return HashUtil::hash(&is_null, sizeof(is_null), seed);
    }
    return _type_info->hash_code(cell.cell_ptr(), seed);
}

class ArrayField : public Field {
public:
    explicit ArrayField(const TabletColumn& column) : Field(column) {}

    void consume(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                 ObjectPool* agg_pool) const override {
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }
        _type_info->deep_copy(dst->mutable_cell_ptr(), src, mem_pool);
    }

    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto array_v = (CollectionValue*)cell_ptr;
        array_v->set_null_signs(reinterpret_cast<bool*>(variable_ptr + sizeof(CollectionValue)));
        array_v->set_data(variable_ptr + sizeof(CollectionValue) +
                          OLAP_ARRAY_MAX_BYTES / sizeof(char*));
        return variable_ptr + _length;
    }

    size_t get_variable_len() const override { return _length; }
};

class CharField : public Field {
public:
    explicit CharField() : Field() {}
    explicit CharField(const TabletColumn& column) : Field(column) {}

    // the char field is especial, which need the _length info when consume raw data
    void consume(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                 ObjectPool* agg_pool) const override {
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }

        auto* value = reinterpret_cast<const StringValue*>(src);
        auto* dest_slice = (Slice*)(dst->mutable_cell_ptr());
        dest_slice->size = _length;
        dest_slice->data = (char*)mem_pool->allocate(dest_slice->size);
        memcpy(dest_slice->data, value->ptr, value->len);
        memset(dest_slice->data + value->len, 0, dest_slice->size - value->len);
    }

    size_t get_variable_len() const override { return _length; }

    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = variable_ptr;
        slice->size = _length;
        variable_ptr += slice->size;
        return variable_ptr;
    }

    CharField* clone() const override {
        auto* local = new CharField();
        Field::clone(local);
        return local;
    }

    char* allocate_value(MemPool* pool) const override {
        return Field::allocate_string_value(pool);
    }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length;
        memset(slice->data, 0xFF, slice->size);
    }
};

class VarcharField : public Field {
public:
    explicit VarcharField() : Field() {}
    explicit VarcharField(const TabletColumn& column) : Field(column) {}

    size_t get_variable_len() const override { return _length - OLAP_STRING_MAX_BYTES; }

    // minus OLAP_STRING_MAX_BYTES here just for being compatible with old storage format
    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = variable_ptr;
        slice->size = _length - OLAP_STRING_MAX_BYTES;
        variable_ptr += slice->size;
        return variable_ptr;
    }

    VarcharField* clone() const override {
        auto* local = new VarcharField();
        Field::clone(local);
        return local;
    }

    char* allocate_value(MemPool* pool) const override {
        return Field::allocate_string_value(pool);
    }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length - OLAP_STRING_MAX_BYTES;
        memset(slice->data, 0xFF, slice->size);
    }
};

class BitmapAggField : public Field {
public:
    explicit BitmapAggField() : Field() {}
    explicit BitmapAggField(const TabletColumn& column) : Field(column) {}

    // bitmap storage data always not null
    void agg_init(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool,
                  ObjectPool* agg_pool) const override {
        _agg_info->init(dst, (const char*)src.cell_ptr(), false, mem_pool, agg_pool);
    }

    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = nullptr;
        return variable_ptr;
    }

    BitmapAggField* clone() const override {
        auto* local = new BitmapAggField();
        Field::clone(local);
        return local;
    }
};

class HllAggField : public Field {
public:
    explicit HllAggField() : Field() {}
    explicit HllAggField(const TabletColumn& column) : Field(column) {}

    // Hll storage data always not null
    void agg_init(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool,
                  ObjectPool* agg_pool) const override {
        _agg_info->init(dst, (const char*)src.cell_ptr(), false, mem_pool, agg_pool);
    }

    char* allocate_memory(char* cell_ptr, char* variable_ptr) const override {
        auto slice = (Slice*)cell_ptr;
        slice->data = nullptr;
        return variable_ptr;
    }

    HllAggField* clone() const override {
        auto* local = new HllAggField();
        Field::clone(local);
        return local;
    }
};

class FieldFactory {
public:
    static Field* create(const TabletColumn& column) {
        // for key column
        if (column.is_key()) {
            switch (column.type()) {
            case OLAP_FIELD_TYPE_CHAR:
                return new CharField(column);
            case OLAP_FIELD_TYPE_VARCHAR:
                return new VarcharField(column);
            case OLAP_FIELD_TYPE_ARRAY: {
                std::unique_ptr<Field> item_field(FieldFactory::create(column.get_sub_column(0)));
                auto* local = new ArrayField(column);
                local->add_sub_field(std::move(item_field));
                return local;
            }
            default:
                return new Field(column);
            }
        }

        // for value column
        switch (column.aggregation()) {
        case OLAP_FIELD_AGGREGATION_NONE:
        case OLAP_FIELD_AGGREGATION_SUM:
        case OLAP_FIELD_AGGREGATION_MIN:
        case OLAP_FIELD_AGGREGATION_MAX:
        case OLAP_FIELD_AGGREGATION_REPLACE:
        case OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
            switch (column.type()) {
            case OLAP_FIELD_TYPE_CHAR:
                return new CharField(column);
            case OLAP_FIELD_TYPE_VARCHAR:
                return new VarcharField(column);
            case OLAP_FIELD_TYPE_ARRAY: {
                std::unique_ptr<Field> item_field(FieldFactory::create(column.get_sub_column(0)));
                auto* local = new ArrayField(column);
                local->add_sub_field(std::move(item_field));
                return local;
            }
            default:
                return new Field(column);
            }
        case OLAP_FIELD_AGGREGATION_HLL_UNION:
            return new HllAggField(column);
        case OLAP_FIELD_AGGREGATION_BITMAP_UNION:
            return new BitmapAggField(column);
        case OLAP_FIELD_AGGREGATION_UNKNOWN:
            LOG(WARNING) << "WOW! value column agg type is unknown";
            return nullptr;
        }
        LOG(WARNING) << "WOW! value column no agg type";
        return nullptr;
    }

    static Field* create_by_type(const FieldType& type) {
        TabletColumn column(OLAP_FIELD_AGGREGATION_NONE, type);
        return create(column);
    }
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_FIELD_H
