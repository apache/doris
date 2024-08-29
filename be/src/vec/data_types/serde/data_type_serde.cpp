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
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_jsonb_serde.h"

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

Status DataTypeSerDe::write_one_cell_to_json(const IColumn& column, rapidjson::Value& result,
                                             rapidjson::Document::AllocatorType& allocator,
                                             Arena& mem_pool, int64_t row_num,
                                             const DataTypePtr& type) const {
    const std::string str_rep = type->to_string(column, row_num);
    // allocate memory to prevent from heap use after free
    void* mem = allocator.Malloc(str_rep.size());
    memcpy(mem, str_rep.data(), str_rep.size());
    result.SetString((const char*)mem, (uint32_t)str_rep.size());
    return Status::OK();
}

Status DataTypeSerDe::read_one_cell_from_json(IColumn& column,
                                              const rapidjson::Value& result) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Not support read {} from rapidjson",
                           column.get_name());
}

const std::string DataTypeSerDe::NULL_IN_COMPLEX_TYPE = "null";
const std::string DataTypeSerDe::NULL_IN_CSV_FOR_ORDINARY_TYPE = "\\N";

} // namespace vectorized
} // namespace doris
