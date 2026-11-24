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

#include "storage/schema.h"

#include <glog/logging.h>

#include <boost/iterator/iterator_facade.hpp>
#include <ostream>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/define_primitive_type.h"
#include "core/typeid_cast.h"
#include "core/types.h"
#include "runtime/descriptors.h"
#include "storage/olap_common.h"
#include "util/trace.h"

namespace doris {

namespace {

// Sets the type of `column` from `data_type`, sub columns included. Sub columns get the names FE
// gives them, and the length is computed the same way as for a column FE sends.
void set_type_from_data_type(const DataTypePtr& data_type, TabletColumn* column) {
    const DataTypePtr type = remove_nullable(data_type);
    const PrimitiveType primitive_type = type->get_primitive_type();
    column->set_type(type->get_storage_field_type());
    column->set_is_nullable(data_type->is_nullable());
    uint32_t string_length = 0;
    switch (primitive_type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        string_length = cast_set<uint32_t>(assert_cast<const DataTypeString&>(*type).len());
        break;
    case TYPE_DECIMALV2: {
        // DECIMALV2 always computes as (27, 9). Keep the precision and scale the column declares.
        const auto& decimal_type = assert_cast<const DataTypeDecimalV2&>(*type);
        column->set_precision(decimal_type.get_original_precision());
        column->set_frac(decimal_type.get_original_scale());
        break;
    }
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        column->set_precision(type->get_precision());
        column->set_frac(type->get_scale());
        break;
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
        column->set_frac(type->get_scale());
        break;
    case TYPE_VARIANT: {
        if (const auto* variant_v2_type = typeid_cast<const DataTypeVariantV2*>(type.get())) {
            column->set_variant_is_v2(true);
            column->set_variant_max_subcolumns_count(
                    variant_v2_type->variant_max_subcolumns_count());
            column->set_variant_enable_doc_mode(variant_v2_type->enable_doc_mode());
        } else {
            const auto& variant_type = assert_cast<const DataTypeVariant&>(*type);
            column->set_variant_max_subcolumns_count(variant_type.variant_max_subcolumns_count());
            column->set_variant_enable_doc_mode(variant_type.enable_doc_mode());
        }
        break;
    }
    case TYPE_ARRAY: {
        TabletColumn item;
        item.set_name("item");
        set_type_from_data_type(assert_cast<const DataTypeArray&>(*type).get_nested_type(), &item);
        column->add_sub_column(item);
        break;
    }
    case TYPE_MAP: {
        const auto& map_type = assert_cast<const DataTypeMap&>(*type);
        TabletColumn key;
        key.set_name("key");
        set_type_from_data_type(map_type.get_key_type(), &key);
        column->add_sub_column(key);
        TabletColumn value;
        value.set_name("value");
        set_type_from_data_type(map_type.get_value_type(), &value);
        column->add_sub_column(value);
        break;
    }
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*type);
        for (size_t i = 0; i < struct_type.get_elements().size(); ++i) {
            TabletColumn field;
            field.set_name(struct_type.get_element_name(i));
            set_type_from_data_type(struct_type.get_element(i), &field);
            column->add_sub_column(field);
        }
        break;
    }
    default:
        break;
    }
    column->set_length(cast_set<int32_t>(
            TabletColumn::get_field_length_by_type(to_thrift(primitive_type), string_length)));
}

} // namespace

Schema::Schema(const Schema& other) {
    _copy_from(other);
}

Schema& Schema::operator=(const Schema& other) {
    if (this != &other) {
        _copy_from(other);
    }
    return *this;
}

void Schema::_copy_from(const Schema& other) {
    _col_ids = other._col_ids;
    _num_key_columns = other._num_key_columns;
    _delete_sign_idx = other._delete_sign_idx;
    _has_sequence_col = other._has_sequence_col;
    _rowid_col_idx = other._rowid_col_idx;
    _version_col_idx = other._version_col_idx;

    _cols.resize(other._cols.size());
    for (auto cid : _col_ids) {
        _cols[cid] = other._cols[cid];
    }
}

void Schema::_init(const std::vector<TabletColumnPtr>& cols, const std::vector<ColumnId>& col_ids,
                   size_t num_key_columns) {
    _col_ids = col_ids;
    _num_key_columns = num_key_columns;

    _cols.resize(cols.size());

    std::unordered_set<uint32_t> col_id_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < cols.size(); ++cid) {
        if (col_id_set.find(cid) == col_id_set.end()) {
            continue;
        }
        _cols[cid] = cols[cid];
    }
}

Schema::~Schema() = default;

DataTypePtr Schema::get_data_type_ptr(const TabletColumn& column) {
    return DataTypeFactory::instance().create_data_type(column);
}

IColumn::MutablePtr Schema::get_predicate_column_ptr(const DataTypePtr& data_type,
                                                     const ReaderType reader_type) {
    // Low-cardinality dictionary optimization substitutes a ColumnDictI32 for the
    // canonical string column during query reads. Every other case just materializes
    // the data type's own canonical column (which already wraps nullable for us).
    if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY &&
        is_string_type(data_type->get_primitive_type())) {
        IColumn::MutablePtr ptr = doris::ColumnDictI32::create();
        if (data_type->is_nullable()) {
            return doris::ColumnNullable::create(std::move(ptr), doris::ColumnUInt8::create());
        }
        return ptr;
    }
    return data_type->create_column();
}

TabletColumn Schema::tablet_column_from_slot(const SlotDescriptor& slot) {
    TabletColumn column;
    column.set_name(slot.col_name());
    column.set_unique_id(slot.col_unique_id());
    set_type_from_data_type(slot.type(), &column);
    if (!slot.col_default_value().empty()) {
        column.set_default_value(slot.col_default_value());
    }
    return column;
}

} // namespace doris
