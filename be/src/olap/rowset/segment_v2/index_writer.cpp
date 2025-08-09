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

#include "common/exception.h"
#include "olap/field.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

bool IndexColumnWriter::check_support_inverted_index(const TabletColumn& column) {
    // bellow types are not supported in inverted index for extracted columns
    static std::set<FieldType> invalid_types = {FieldType::OLAP_FIELD_TYPE_JSONB};
    if (invalid_types.contains(column.type())) {
        return false;
    }
    if (column.is_variant_type()) {
        return false;
    }
    if (column.is_array_type()) {
        // only support one level array
        const auto& subcolumn = column.get_sub_column(0);
        return !subcolumn.is_array_type() && check_support_inverted_index(subcolumn);
    }
    return true;
}

Status IndexColumnWriter::create(const Field* field, std::unique_ptr<IndexColumnWriter>* res,
                                 IndexFileWriter* index_file_writer,
                                 const TabletIndex* index_meta) {
    const auto* typeinfo = field->type_info();
    FieldType type = typeinfo->type();
    std::string field_name;
    auto storage_format = index_file_writer->get_storage_format();
    if (storage_format == InvertedIndexStorageFormatPB::V1) {
        field_name = field->name();
    } else {
        if (field->is_extracted_column()) {
            // variant sub col
            // field_name format: parent_unique_id.sub_col_name
            field_name = std::to_string(field->parent_unique_id()) + "." + field->name();
        } else {
            field_name = std::to_string(field->unique_id());
        }
    }
    bool single_field = true;
    if (type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        const auto* array_typeinfo = dynamic_cast<const ArrayTypeInfo*>(typeinfo);
        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_array_typeinfo_is_nullptr",
                        { array_typeinfo = nullptr; })
        if (array_typeinfo != nullptr) {
            typeinfo = array_typeinfo->item_type_info();
            type = typeinfo->type();
            single_field = false;
        } else {
            return Status::NotSupported("unsupported array type for inverted index: " +
                                        std::to_string(int(type)));
        }
    }

    DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_unsupported_type_for_inverted_index",
                    { type = FieldType::OLAP_FIELD_TYPE_JSONB; })
    switch (type) {
#define M(TYPE)                                                                                 \
    case TYPE:                                                                                  \
        *res = std::make_unique<InvertedIndexColumnWriter<TYPE>>(field_name, index_file_writer, \
                                                                 index_meta, single_field);     \
        break;
        M(FieldType::OLAP_FIELD_TYPE_TINYINT)
        M(FieldType::OLAP_FIELD_TYPE_SMALLINT)
        M(FieldType::OLAP_FIELD_TYPE_INT)
        M(FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT)
        M(FieldType::OLAP_FIELD_TYPE_BIGINT)
        M(FieldType::OLAP_FIELD_TYPE_LARGEINT)
        M(FieldType::OLAP_FIELD_TYPE_CHAR)
        M(FieldType::OLAP_FIELD_TYPE_VARCHAR)
        M(FieldType::OLAP_FIELD_TYPE_STRING)
        M(FieldType::OLAP_FIELD_TYPE_DATE)
        M(FieldType::OLAP_FIELD_TYPE_DATETIME)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL)
        M(FieldType::OLAP_FIELD_TYPE_DATEV2)
        M(FieldType::OLAP_FIELD_TYPE_DATETIMEV2)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL32)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL64)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL128I)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL256)
        M(FieldType::OLAP_FIELD_TYPE_BOOL)
        M(FieldType::OLAP_FIELD_TYPE_IPV4)
        M(FieldType::OLAP_FIELD_TYPE_IPV6)
        M(FieldType::OLAP_FIELD_TYPE_FLOAT)
        M(FieldType::OLAP_FIELD_TYPE_DOUBLE)
#undef M
    default:
        return Status::NotSupported("unsupported type for inverted index: " +
                                    std::to_string(int(type)));
    }
    if (*res != nullptr) {
        auto st = (*res)->init();
        if (!st.ok()) {
            (*res)->close_on_error();
            return st;
        }
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
