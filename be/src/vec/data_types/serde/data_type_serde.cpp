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
#include "data_type_serde.h"

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_jsonb_serde.h"
#include "vec/functions/cast/cast_base.h"
namespace doris {
namespace vectorized {
#include "common/compile_check_begin.h"
DataTypeSerDe::~DataTypeSerDe() = default;

DataTypeSerDeSPtrs create_data_type_serdes(const DataTypes& types) {
    DataTypeSerDeSPtrs serdes;
    serdes.reserve(types.size());
    for (const DataTypePtr& type : types) {
        serdes.push_back(type->get_serde());
    }
    return serdes;
}

DataTypeSerDeSPtrs create_data_type_serdes(const std::vector<SlotDescriptor*>& slots) {
    DataTypeSerDeSPtrs serdes;
    serdes.reserve(slots.size());
    for (const SlotDescriptor* slot : slots) {
        serdes.push_back(slot->get_data_type_ptr()->get_serde());
    }
    return serdes;
}

Status DataTypeSerDe::default_from_string(StringRef& str, IColumn& column) const {
    auto slice = str.to_slice();
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    ///TODO: Think again, when do we need to consider escape characters?
    // options.escape_char = '\\';
    // Deserialize the string into the column
    return deserialize_one_cell_from_json(column, slice, options);
}

Status DataTypeSerDe::serialize_column_to_jsonb_vector(const IColumn& from_column,
                                                       ColumnString& to_column) const {
    const auto size = from_column.size();
    JsonbWriter writer;
    for (int i = 0; i < size; i++) {
        writer.reset();
        RETURN_IF_ERROR(serialize_column_to_jsonb(from_column, i, writer));
        to_column.insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
    }
    return Status::OK();
}

Status DataTypeSerDe::parse_column_from_jsonb_string(IColumn& column, const JsonbValue* jsonb_value,
                                                     CastParameters& castParms) const {
    DCHECK(jsonb_value->isString());
    const auto* blob = jsonb_value->unpack<JsonbBinaryVal>();

    Slice slice(blob->getBlob(), blob->getBlobLen());

    DataTypeSerDe::FormatOptions format_options;
    format_options.converted_from_string = true;
    format_options.escape_char = '\\';

    return deserialize_one_cell_from_json(column, slice, format_options);
}

Status DataTypeSerDe::deserialize_column_from_jsonb_vector(ColumnNullable& column_to,
                                                           const ColumnString& col_from_json,
                                                           CastParameters& castParms) const {
    const size_t size = col_from_json.size();
    const bool is_strict = castParms.is_strict;
    for (size_t i = 0; i < size; ++i) {
        const auto& val = col_from_json.get_data_at(i);
        auto* value = handle_jsonb_value(val);
        if (!value) {
            column_to.insert_default();
            continue;
        }
        Status from_st =
                deserialize_column_from_jsonb(column_to.get_nested_column(), value, castParms);

        if (from_st.ok()) {
            // fill not null if success
            column_to.get_null_map_data().push_back(0);
        } else {
            if (is_strict) {
                return from_st;
            } else {
                // fill null if fail
                column_to.insert_default();
            }
        }
    }
    return Status::OK();
}

const std::string DataTypeSerDe::NULL_IN_COMPLEX_TYPE = "null";
const std::string DataTypeSerDe::NULL_IN_CSV_FOR_ORDINARY_TYPE = "\\N";

} // namespace vectorized
} // namespace doris
