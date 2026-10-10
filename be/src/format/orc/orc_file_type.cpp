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

#include "format/orc/orc_file_type.h"

#include <array>
#include <orc/Type.hh>

#include "common/status.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/orc_serde_utils.h"

namespace doris {
namespace {

constexpr const char* FILE_MARKER = "doris.struct-type";
constexpr std::array<const char*, 6> FILE_NAMES = {"uri",          "offset",   "size",
                                                   "content_type", "checksum", "inline"};
constexpr std::array<orc::TypeKind, 6> FILE_KINDS = {orc::STRING, orc::LONG,   orc::LONG,
                                                     orc::STRING, orc::STRING, orc::BINARY};

} // namespace

bool is_orc_file_type(const orc::Type& type) {
    return type.getKind() == orc::STRUCT && type.hasAttributeKey(FILE_MARKER) &&
           type.getAttributeValue(FILE_MARKER) == "FILE";
}

std::unique_ptr<orc::Type> create_orc_file_type() {
    auto type = orc::createStructType();
    type->setAttribute(FILE_MARKER, "FILE");
    for (size_t i = 0; i < FILE_NAMES.size(); ++i) {
        type->addStructField(FILE_NAMES[i], orc::createPrimitiveType(FILE_KINDS[i]));
    }
    return type;
}

Status validate_orc_file_type(const orc::Type& type) {
    return orc_serde_utils::validate_orc_file_type(type);
}

Status annotate_orc_file_types(const DataTypePtr& data_type, orc::Type& type) {
    if (!contains_file_type(data_type)) {
        return Status::OK();
    }
    const auto nested = remove_nullable(data_type);
    switch (nested->get_primitive_type()) {
    case TYPE_FILE:
        if (type.getSubtypeCount() != DataTypeFile::FIELD_COUNT) {
            return Status::InvalidArgument("FILE ORC output requires six children");
        }
        type.setAttribute(FILE_MARKER, "FILE");
        return validate_orc_file_type(type);
    case TYPE_ARRAY: {
        if (type.getKind() != orc::LIST || type.getSubtypeCount() != 1) {
            return Status::InvalidArgument("ORC schema does not match ARRAY containing FILE");
        }
        const auto& array = assert_cast<const DataTypeArray&>(*nested);
        return annotate_orc_file_types(array.get_nested_type(), *type.getSubtype(0));
    }
    case TYPE_MAP: {
        if (type.getKind() != orc::MAP || type.getSubtypeCount() != 2) {
            return Status::InvalidArgument("ORC schema does not match MAP containing FILE");
        }
        const auto& map = assert_cast<const DataTypeMap&>(*nested);
        RETURN_IF_ERROR(annotate_orc_file_types(map.get_key_type(), *type.getSubtype(0)));
        return annotate_orc_file_types(map.get_value_type(), *type.getSubtype(1));
    }
    case TYPE_STRUCT: {
        const auto& structure = assert_cast<const DataTypeStruct&>(*nested);
        if (type.getKind() != orc::STRUCT ||
            type.getSubtypeCount() != structure.get_elements().size()) {
            return Status::InvalidArgument("ORC schema does not match STRUCT containing FILE");
        }
        for (size_t i = 0; i < structure.get_elements().size(); ++i) {
            RETURN_IF_ERROR(annotate_orc_file_types(structure.get_element(i), *type.getSubtype(i)));
        }
        return Status::OK();
    }
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "Unexpected type containing FILE: {}",
                        nested->get_name());
    }
}

} // namespace doris
