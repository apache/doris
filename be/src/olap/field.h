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

#include <sstream>
#include <string>

#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_cursor_cell.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "runtime/collection_value.h"
#include "runtime/map_value.h"
#include "util/hash_util.hpp"
#include "util/slice.h"
#include "vec/common/arena.h"
#include "vec/json/path_in_data.h"

namespace doris {

// A Field is used to represent a column in memory format.
// User can use this class to access or deal with column data in memory.
class Field {
public:
    Field(const TabletColumn& column)
            : _type_info(get_type_info(&column)),
              _desc(column),
              _length(column.length()),
              _key_coder(get_key_coder(column.type())),
              _name(column.name()),
              _index_size(column.index_length()),
              _is_nullable(column.is_nullable()),
              _unique_id(column.unique_id()),
              _parent_unique_id(column.parent_unique_id()),
              _is_extracted_column(column.is_extracted_column()),
              _path(column.path_info_ptr()) {}

    virtual ~Field() = default;

    size_t size() const { return _type_info->size(); }
    int32_t length() const { return _length; }
    size_t field_size() const { return size() + 1; }
    size_t index_size() const { return _index_size; }
    int32_t unique_id() const { return _unique_id; }
    int32_t parent_unique_id() const { return _parent_unique_id; }
    bool is_extracted_column() const { return _is_extracted_column; }
    const std::string& name() const { return _name; }
    const vectorized::PathInDataPtr& path() const { return _path; }

    virtual void set_to_max(char* buf) const { return _type_info->set_to_max(buf); }
    virtual void set_to_zone_map_max(char* buf) const { set_to_max(buf); }

    virtual void set_to_min(char* buf) const { return _type_info->set_to_min(buf); }
    virtual void set_to_zone_map_min(char* buf) const { set_to_min(buf); }

    void set_long_text_buf(char** buf) { _long_text_buf = buf; }

    // This function allocate memory from arena, other than allocate_memory
    // reserve memory from continuous memory.
    virtual char* allocate_value(vectorized::Arena* arena) const {
        return arena->alloc(_type_info->size());
    }

    virtual char* allocate_zone_map_value(vectorized::Arena* arena) const {
        return allocate_value(arena);
    }

    virtual size_t get_variable_len() const { return 0; }

    virtual void modify_zone_map_index(char*) const {}

    virtual Field* clone() const {
        auto* local = new Field(_desc);
        this->clone(local);
        return local;
    }

    // Only compare column content, without considering nullptr condition.
    // RETURNS:
    //      0 means equal,
    //      -1 means left less than right,
    //      1 means left bigger than right
    int compare(const void* left, const void* right) const { return _type_info->cmp(left, right); }

    // Compare two types of cell.
    // This function differs compare in that this function compare cell which
    // will consider the condition which cell may be nullptr. While compare only
    // compare column content without considering nullptr condition.
    // Only compare column content, without considering nullptr condition.
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
        if (type() == FieldType::OLAP_FIELD_TYPE_STRING) {
            auto dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
            auto src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
            if (dst_slice->size < src_slice->size) {
                *_long_text_buf = static_cast<char*>(realloc(*_long_text_buf, src_slice->size));
                dst_slice->data = *_long_text_buf;
                dst_slice->size = src_slice->size;
            }
        }
        return _type_info->direct_copy(dst->mutable_cell_ptr(), src.cell_ptr());
    }

    // deep copy source cell' content to destination cell.
    // For string type, this will allocate data form arena,
    // and copy source's content.
    template <typename DstCellType, typename SrcCellType>
    void deep_copy(DstCellType* dst, const SrcCellType& src, vectorized::Arena* arena) const {
        bool is_null = src.is_null();
        dst->set_is_null(is_null);
        if (is_null) {
            return;
        }
        _type_info->deep_copy(dst->mutable_cell_ptr(), src.cell_ptr(), arena);
    }

    // used by init scan key stored in string format
    // value_string should end with '\0'
    Status from_string(char* buf, const std::string& value_string, const int precision = 0,
                       const int scale = 0) const {
        if (type() == FieldType::OLAP_FIELD_TYPE_STRING && !value_string.empty()) {
            auto slice = reinterpret_cast<Slice*>(buf);
            if (slice->size < value_string.size()) {
                *_long_text_buf = static_cast<char*>(realloc(*_long_text_buf, value_string.size()));
                slice->data = *_long_text_buf;
                slice->size = value_string.size();
            }
        }
        return _type_info->from_string(buf, value_string, precision, scale);
    }

    //  convert inner value to string
    //  performance is not considered, only for debug use
    std::string to_string(const char* src) const { return _type_info->to_string(src); }

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

    FieldType type() const { return _type_info->type(); }
    const TypeInfo* type_info() const { return _type_info.get(); }
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
    void add_sub_field(std::unique_ptr<Field> sub_field) {
        _sub_fields.emplace_back(std::move(sub_field));
    }
    Field* get_sub_field(int i) const { return _sub_fields[i].get(); }
    size_t get_sub_field_count() const { return _sub_fields.size(); }

    void set_precision(int32_t precision) { _precision = precision; }
    void set_scale(int32_t scale) { _scale = scale; }
    int32_t get_precision() const { return _precision; }
    int32_t get_scale() const { return _scale; }
    const TabletColumn& get_desc() const { return _desc; }

protected:
    TypeInfoPtr _type_info;
    TabletColumn _desc;
    // unit : byte
    // except for strings, other types have fixed lengths
    // Note that, the struct type itself has fixed length, but due to
    // its number of subfields is a variable, so the actual length of
    // a struct field is not fixed.
    uint32_t _length;
    // Since the length of the STRING type cannot be determined,
    // only dynamic memory can be used. Arena cannot realize realloc.
    // The schema information is shared globally. Therefore,
    // dynamic memory can only be managed in thread local mode.
    // The memory will be created and released in rowcursor.
    char** _long_text_buf = nullptr;

    char* allocate_string_value(vectorized::Arena* arena) const {
        char* type_value = arena->alloc(sizeof(Slice));
        auto slice = reinterpret_cast<Slice*>(type_value);
        slice->size = _length;
        slice->data = arena->alloc(slice->size);
        return type_value;
    }

    void clone(Field* other) const {
        other->_type_info = clone_type_info(this->_type_info.get());
        other->_key_coder = this->_key_coder;
        other->_name = this->_name;
        other->_index_size = this->_index_size;
        other->_is_nullable = this->_is_nullable;
        other->_sub_fields.clear();
        other->_precision = this->_precision;
        other->_scale = this->_scale;
        other->_unique_id = this->_unique_id;
        other->_parent_unique_id = this->_parent_unique_id;
        other->_is_extracted_column = this->_is_extracted_column;
        for (const auto& f : _sub_fields) {
            Field* item = f->clone();
            other->add_sub_field(std::unique_ptr<Field>(item));
        }
    }

private:
    // maximum length of Field, unit : bytes
    // usually equal to length, except for variable-length strings
    const KeyCoder* _key_coder;
    std::string _name;
    uint16_t _index_size;
    bool _is_nullable;
    std::vector<std::unique_ptr<Field>> _sub_fields;
    int32_t _precision;
    int32_t _scale;
    int32_t _unique_id;
    int32_t _parent_unique_id;
    bool _is_extracted_column = false;
    vectorized::PathInDataPtr _path;
};

class MapField : public Field {
public:
    MapField(const TabletColumn& column) : Field(column) {}

    size_t get_variable_len() const override { return _length; }
};

class StructField : public Field {
public:
    StructField(const TabletColumn& column) : Field(column) {}

    size_t get_variable_len() const override {
        size_t variable_len = _length;
        for (size_t i = 0; i < get_sub_field_count(); i++) {
            variable_len += get_sub_field(i)->get_variable_len();
        }
        return variable_len;
    }
};

class ArrayField : public Field {
public:
    ArrayField(const TabletColumn& column) : Field(column) {}

    size_t get_variable_len() const override { return _length; }
};

class CharField : public Field {
public:
    CharField(const TabletColumn& column) : Field(column) {}

    size_t get_variable_len() const override { return _length; }

    CharField* clone() const override {
        auto* local = new CharField(_desc);
        Field::clone(local);
        return local;
    }

    char* allocate_value(vectorized::Arena* arena) const override {
        return Field::allocate_string_value(arena);
    }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length;
        memset(slice->data, 0xFF, slice->size);
    }

    // To prevent zone map cost too many memory, if varchar length
    // longer than `MAX_ZONE_MAP_INDEX_SIZE`. we just allocate
    // `MAX_ZONE_MAP_INDEX_SIZE` of memory
    char* allocate_zone_map_value(vectorized::Arena* arena) const override {
        char* type_value = arena->alloc(sizeof(Slice));
        auto slice = reinterpret_cast<Slice*>(type_value);
        slice->size = MAX_ZONE_MAP_INDEX_SIZE > _length ? _length : MAX_ZONE_MAP_INDEX_SIZE;
        slice->data = arena->alloc(slice->size);
        return type_value;
    }

    // only varchar filed need modify zone map index when zone map max_value
    // index longer than `MAX_ZONE_MAP_INDEX_SIZE`. so here we add one
    // for the last byte
    // In UTF8 encoding, here do not appear 0xff in last byte
    void modify_zone_map_index(char* src) const override {
        auto slice = reinterpret_cast<Slice*>(src);
        if (slice->size == MAX_ZONE_MAP_INDEX_SIZE) {
            slice->mutable_data()[slice->size - 1] += 1;
        }
    }

    void set_to_zone_map_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        int length = _length < MAX_ZONE_MAP_INDEX_SIZE ? _length : MAX_ZONE_MAP_INDEX_SIZE;
        slice->size = length;
        memset(slice->data, 0xFF, slice->size);
    }
};

class VarcharField : public Field {
public:
    VarcharField(const TabletColumn& column) : Field(column) {}

    size_t get_variable_len() const override { return _length - OLAP_VARCHAR_MAX_BYTES; }

    VarcharField* clone() const override {
        auto* local = new VarcharField(_desc);
        Field::clone(local);
        return local;
    }

    char* allocate_value(vectorized::Arena* arena) const override {
        return Field::allocate_string_value(arena);
    }

    // To prevent zone map cost too many memory, if varchar length
    // longer than `MAX_ZONE_MAP_INDEX_SIZE`. we just allocate
    // `MAX_ZONE_MAP_INDEX_SIZE` of memory
    char* allocate_zone_map_value(vectorized::Arena* arena) const override {
        char* type_value = arena->alloc(sizeof(Slice));
        auto slice = reinterpret_cast<Slice*>(type_value);
        slice->size = MAX_ZONE_MAP_INDEX_SIZE > _length ? _length : MAX_ZONE_MAP_INDEX_SIZE;
        slice->data = arena->alloc(slice->size);
        return type_value;
    }

    // only varchar/string filed need modify zone map index when zone map max_value
    // index longer than `MAX_ZONE_MAP_INDEX_SIZE`. so here we add one
    // for the last byte
    // In UTF8 encoding, here do not appear 0xff in last byte
    void modify_zone_map_index(char* src) const override {
        auto slice = reinterpret_cast<Slice*>(src);
        if (slice->size == MAX_ZONE_MAP_INDEX_SIZE) {
            slice->mutable_data()[slice->size - 1] += 1;
        }
    }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length - OLAP_VARCHAR_MAX_BYTES;
        memset(slice->data, 0xFF, slice->size);
    }
    void set_to_zone_map_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        int length = _length < MAX_ZONE_MAP_INDEX_SIZE ? _length : MAX_ZONE_MAP_INDEX_SIZE;

        slice->size = length - OLAP_VARCHAR_MAX_BYTES;
        memset(slice->data, 0xFF, slice->size);
    }
};
class StringField : public Field {
public:
    StringField(const TabletColumn& column) : Field(column) {}

    StringField* clone() const override {
        auto* local = new StringField(_desc);
        Field::clone(local);
        return local;
    }

    char* allocate_value(vectorized::Arena* arena) const override {
        return Field::allocate_string_value(arena);
    }

    char* allocate_zone_map_value(vectorized::Arena* arena) const override {
        char* type_value = arena->alloc(sizeof(Slice));
        auto slice = reinterpret_cast<Slice*>(type_value);
        slice->size = MAX_ZONE_MAP_INDEX_SIZE;
        slice->data = arena->alloc(slice->size);
        return type_value;
    }
    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        memset(slice->data, 0xFF, slice->size);
    }
    // only varchar/string filed need modify zone map index when zone map max_value
    // index longer than `MAX_ZONE_MAP_INDEX_SIZE`. so here we add one
    // for the last byte
    // In UTF8 encoding, here do not appear 0xff in last byte
    void modify_zone_map_index(char* src) const override {
        auto slice = reinterpret_cast<Slice*>(src);
        if (slice->size == MAX_ZONE_MAP_INDEX_SIZE) {
            slice->mutable_data()[slice->size - 1] += 1;
        }
    }

    void set_to_zone_map_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        memset(slice->data, 0xFF, slice->size);
    }
    void set_to_zone_map_min(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        memset(slice->data, 0x00, slice->size);
    }
};

class BitmapAggField : public Field {
public:
    BitmapAggField(const TabletColumn& column) : Field(column) {}

    BitmapAggField* clone() const override {
        auto* local = new BitmapAggField(_desc);
        Field::clone(local);
        return local;
    }
};

class QuantileStateAggField : public Field {
public:
    QuantileStateAggField(const TabletColumn& column) : Field(column) {}

    QuantileStateAggField* clone() const override {
        auto* local = new QuantileStateAggField(_desc);
        Field::clone(local);
        return local;
    }
};

class AggStateField : public Field {
public:
    AggStateField(const TabletColumn& column) : Field(column) {}

    AggStateField* clone() const override {
        auto* local = new AggStateField(_desc);
        Field::clone(local);
        return local;
    }
};

class HllAggField : public Field {
public:
    HllAggField(const TabletColumn& column) : Field(column) {}

    HllAggField* clone() const override {
        auto* local = new HllAggField(_desc);
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
            case FieldType::OLAP_FIELD_TYPE_CHAR:
                return new CharField(column);
            case FieldType::OLAP_FIELD_TYPE_VARCHAR:
            case FieldType::OLAP_FIELD_TYPE_STRING:
                return new StringField(column);
            case FieldType::OLAP_FIELD_TYPE_STRUCT: {
                auto* local = new StructField(column);
                for (uint32_t i = 0; i < column.get_subtype_count(); i++) {
                    std::unique_ptr<Field> sub_field(
                            FieldFactory::create(column.get_sub_column(i)));
                    local->add_sub_field(std::move(sub_field));
                }
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_ARRAY: {
                std::unique_ptr<Field> item_field(FieldFactory::create(column.get_sub_column(0)));
                auto* local = new ArrayField(column);
                local->add_sub_field(std::move(item_field));
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_MAP: {
                std::unique_ptr<Field> key_field(FieldFactory::create(column.get_sub_column(0)));
                std::unique_ptr<Field> val_field(FieldFactory::create(column.get_sub_column(1)));
                auto* local = new MapField(column);
                local->add_sub_field(std::move(key_field));
                local->add_sub_field(std::move(val_field));
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_DECIMAL:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
                Field* field = new Field(column);
                field->set_precision(column.precision());
                field->set_scale(column.frac());
                return field;
            }
            default:
                return new Field(column);
            }
        }

        // for value column
        switch (column.aggregation()) {
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE:
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM:
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN:
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX:
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE:
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
            switch (column.type()) {
            case FieldType::OLAP_FIELD_TYPE_CHAR:
                return new CharField(column);
            case FieldType::OLAP_FIELD_TYPE_VARCHAR:
                return new VarcharField(column);
            case FieldType::OLAP_FIELD_TYPE_STRING:
                return new StringField(column);
            case FieldType::OLAP_FIELD_TYPE_STRUCT: {
                auto* local = new StructField(column);
                for (uint32_t i = 0; i < column.get_subtype_count(); i++) {
                    std::unique_ptr<Field> sub_field(
                            FieldFactory::create(column.get_sub_column(i)));
                    local->add_sub_field(std::move(sub_field));
                }
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_ARRAY: {
                std::unique_ptr<Field> item_field(FieldFactory::create(column.get_sub_column(0)));
                auto* local = new ArrayField(column);
                local->add_sub_field(std::move(item_field));
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_MAP: {
                DCHECK(column.get_subtype_count() == 2);
                auto* local = new MapField(column);
                std::unique_ptr<Field> key_field(FieldFactory::create(column.get_sub_column(0)));
                std::unique_ptr<Field> value_field(FieldFactory::create(column.get_sub_column(1)));
                local->add_sub_field(std::move(key_field));
                local->add_sub_field(std::move(value_field));
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_DECIMAL:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
                Field* field = new Field(column);
                field->set_precision(column.precision());
                field->set_scale(column.frac());
                return field;
            }
            default:
                return new Field(column);
            }
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_HLL_UNION:
            return new HllAggField(column);
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_BITMAP_UNION:
            return new BitmapAggField(column);
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_QUANTILE_UNION:
            return new QuantileStateAggField(column);
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_GENERIC:
            return new AggStateField(column);
        case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_UNKNOWN:
            CHECK(false) << ", value column no agg type";
            return nullptr;
        }
        return nullptr;
    }

    static Field* create_by_type(const FieldType& type) {
        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type);
        return create(column);
    }
};

} // namespace doris
