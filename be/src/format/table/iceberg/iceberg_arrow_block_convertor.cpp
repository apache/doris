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

#include "format/table/iceberg/iceberg_arrow_block_convertor.h"

#include <arrow/array/builder_base.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include "format/table/iceberg/arrow_schema_util.h"

namespace doris::iceberg {
#include "common/compile_check_begin.h"

Status IcebergArrowBlockConvertor::init() {
    if (_schema == nullptr) {
        return ArrowBlockConvertor::init();
    }
    // Field IDs, Variant storage and timestamp bindings must share the same target schema.
    std::vector<std::shared_ptr<arrow::Field>> fields;
    RETURN_IF_ERROR(ArrowSchemaUtil::convert(_schema, _timezone.name(), fields));
    _arrow_schema = arrow::schema(std::move(fields));
    if (!_schema_json.empty()) {
        _arrow_schema = _arrow_schema->WithMetadata(
                arrow::KeyValueMetadata::Make({"iceberg.schema"}, {_schema_json}));
    }
    return Status::OK();
}

Status IcebergArrowBlockConvertor::write_column(const std::shared_ptr<const IDataType>& type,
                                                const DataTypeSerDe& serde, const IColumn& column,
                                                const NullMap* null_map,
                                                const std::shared_ptr<arrow::Field>& field,
                                                arrow::ArrayBuilder* array_builder, int64_t start,
                                                int64_t end, const cctz::time_zone& ctz) const {
    // This adapter dereferences Arrow declarations that are intentionally forward-declared by
    // its public header, so keep the complete definitions local to this implementation file.
    return serde.write_column_to_iceberg_arrow(type, column, null_map,
                                               field->WithType(array_builder->type()),
                                               array_builder, start, end, ctz);
}

#include "common/compile_check_end.h"
} // namespace doris::iceberg
