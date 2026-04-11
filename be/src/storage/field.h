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

#include <cstddef>
#include <sstream>
#include <string>

#include "core/arena.h"
#include "core/value/map_value.h"
#include "runtime/collection_value.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "storage/utils.h"
#include "util/hash_util.hpp"
#include "util/json/path_in_data.h"
#include "util/slice.h"

namespace doris {
// A Field is used to represent a column in memory format.
// User can use this class to access or deal with column data in memory.
class StorageField {
public:
    StorageField(const TabletColumn& column)
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

    virtual ~StorageField() = default;

    size_t size() const { return _type_info->size(); }
    size_t length() const { return _length; }
    size_t field_size() const { return size() + 1; }
    size_t index_size() const { return _index_size; }
    int32_t unique_id() const { return _unique_id; }
    int32_t parent_unique_id() const { return _parent_unique_id; }
    bool is_extracted_column() const { return _is_extracted_column; }
    const std::string& name() const { return _name; }
    const PathInDataPtr& path() const { return _path; }

    virtual void set_to_max(char* buf) const { return _type_info->set_to_max(buf); }

    virtual void set_to_min(char* buf) const { return _type_info->set_to_min(buf); }

    virtual StorageField* clone() const {
        auto* local = new StorageField(_desc);
        this->clone(local);
        return local;
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
    void add_sub_field(std::unique_ptr<StorageField> sub_field) {
        _sub_fields.emplace_back(std::move(sub_field));
    }
    StorageField* get_sub_field(size_t i) const { return _sub_fields[i].get(); }
    size_t get_sub_field_count() const { return _sub_fields.size(); }

    void set_precision(int32_t precision) { _precision = precision; }
    void set_scale(int32_t scale) { _scale = scale; }
    int32_t get_precision() const { return _precision; }
    int32_t get_scale() const { return _scale; }
    const TabletColumn& get_desc() const { return _desc; }

    int32_t get_unique_id() const {
        return is_extracted_column() ? parent_unique_id() : unique_id();
    }

protected:
    TypeInfoPtr _type_info;
    TabletColumn _desc;
    // unit : byte
    // except for strings, other types have fixed lengths
    // Note that, the struct type itself has fixed length, but due to
    // its number of subfields is a variable, so the actual length of
    // a struct field is not fixed.
    size_t _length;

    void clone(StorageField* other) const {
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
            StorageField* item = f->clone();
            other->add_sub_field(std::unique_ptr<StorageField>(item));
        }
    }

private:
    // maximum length of Field, unit : bytes
    // usually equal to length, except for variable-length strings
    const KeyCoder* _key_coder;
    std::string _name;
    size_t _index_size;
    bool _is_nullable;
    std::vector<std::unique_ptr<StorageField>> _sub_fields;
    int32_t _precision;
    int32_t _scale;
    int32_t _unique_id;
    int32_t _parent_unique_id;
    bool _is_extracted_column = false;
    PathInDataPtr _path;
};

class MapField : public StorageField {
public:
    MapField(const TabletColumn& column) : StorageField(column) {}
};

class StructField : public StorageField {
public:
    StructField(const TabletColumn& column) : StorageField(column) {}
};

class ArrayField : public StorageField {
public:
    ArrayField(const TabletColumn& column) : StorageField(column) {}
};

class CharField : public StorageField {
public:
    CharField(const TabletColumn& column) : StorageField(column) {}

    CharField* clone() const override {
        auto* local = new CharField(_desc);
        StorageField::clone(local);
        return local;
    }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length;
        memset(slice->data, 0xFF, slice->size);
    }
};

class VarcharField : public StorageField {
public:
    VarcharField(const TabletColumn& column) : StorageField(column) {}

    VarcharField* clone() const override {
        auto* local = new VarcharField(_desc);
        StorageField::clone(local);
        return local;
    }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        slice->size = _length - OLAP_VARCHAR_MAX_BYTES;
        memset(slice->data, 0xFF, slice->size);
    }
};
class StringField : public StorageField {
public:
    StringField(const TabletColumn& column) : StorageField(column) {}

    StringField* clone() const override {
        auto* local = new StringField(_desc);
        StorageField::clone(local);
        return local;
    }

    void set_to_max(char* ch) const override {
        auto slice = reinterpret_cast<Slice*>(ch);
        memset(slice->data, 0xFF, slice->size);
    }
};

class BitmapAggField : public StorageField {
public:
    BitmapAggField(const TabletColumn& column) : StorageField(column) {}

    BitmapAggField* clone() const override {
        auto* local = new BitmapAggField(_desc);
        StorageField::clone(local);
        return local;
    }
};

class QuantileStateAggField : public StorageField {
public:
    QuantileStateAggField(const TabletColumn& column) : StorageField(column) {}

    QuantileStateAggField* clone() const override {
        auto* local = new QuantileStateAggField(_desc);
        StorageField::clone(local);
        return local;
    }
};

class AggStateField : public StorageField {
public:
    AggStateField(const TabletColumn& column) : StorageField(column) {}

    AggStateField* clone() const override {
        auto* local = new AggStateField(_desc);
        StorageField::clone(local);
        return local;
    }
};

class HllAggField : public StorageField {
public:
    HllAggField(const TabletColumn& column) : StorageField(column) {}

    HllAggField* clone() const override {
        auto* local = new HllAggField(_desc);
        StorageField::clone(local);
        return local;
    }
};

class StorageFieldFactory {
public:
    static StorageField* create(const TabletColumn& column) {
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
                    std::unique_ptr<StorageField> sub_field(
                            StorageFieldFactory::create(column.get_sub_column(i)));
                    local->add_sub_field(std::move(sub_field));
                }
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_ARRAY: {
                std::unique_ptr<StorageField> item_field(
                        StorageFieldFactory::create(column.get_sub_column(0)));
                auto* local = new ArrayField(column);
                local->add_sub_field(std::move(item_field));
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_MAP: {
                std::unique_ptr<StorageField> key_field(
                        StorageFieldFactory::create(column.get_sub_column(0)));
                std::unique_ptr<StorageField> val_field(
                        StorageFieldFactory::create(column.get_sub_column(1)));
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
            case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
                StorageField* field = new StorageField(column);
                field->set_precision(column.precision());
                field->set_scale(column.frac());
                return field;
            }
            default:
                return new StorageField(column);
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
                    std::unique_ptr<StorageField> sub_field(
                            StorageFieldFactory::create(column.get_sub_column(i)));
                    local->add_sub_field(std::move(sub_field));
                }
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_ARRAY: {
                std::unique_ptr<StorageField> item_field(
                        StorageFieldFactory::create(column.get_sub_column(0)));
                auto* local = new ArrayField(column);
                local->add_sub_field(std::move(item_field));
                return local;
            }
            case FieldType::OLAP_FIELD_TYPE_MAP: {
                DCHECK(column.get_subtype_count() == 2);
                auto* local = new MapField(column);
                std::unique_ptr<StorageField> key_field(
                        StorageFieldFactory::create(column.get_sub_column(0)));
                std::unique_ptr<StorageField> value_field(
                        StorageFieldFactory::create(column.get_sub_column(1)));
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
            case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
                [[fallthrough]];
            case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
                StorageField* field = new StorageField(column);
                field->set_precision(column.precision());
                field->set_scale(column.frac());
                return field;
            }
            default:
                return new StorageField(column);
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

    static StorageField* create_by_type(const FieldType& type) {
        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type);
        return create(column);
    }
};
} // namespace doris
