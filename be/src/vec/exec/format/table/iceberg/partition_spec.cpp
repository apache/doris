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

#include "vec/exec/format/table/iceberg/partition_spec.h"

#include <memory>
#include <string>
#include <vector>

#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/exec/format/table/iceberg/struct_like.h"

namespace doris {
namespace iceberg {

PartitionField::PartitionField(int source_id, int field_id, std::string name, std::string transform)
        : _source_id(source_id),
          _field_id(field_id),
          _name(std::move(name)),
          _transform(std::move(transform)) {}

PartitionSpec::Builder::Builder(std::shared_ptr<Schema> schema)
        : _schema(std::move(schema)),
          _spec_id(0),
          _last_assigned_field_id(PARTITION_DATA_ID_START - 1) {}

PartitionSpec::Builder& PartitionSpec::Builder::with_spec_id(int new_spec_id) {
    _spec_id = new_spec_id;
    return *this;
}

PartitionSpec::Builder& PartitionSpec::Builder::add(int source_id, int field_id, std::string name,
                                                    std::string transform) {
    _fields.emplace_back(source_id, field_id, std::move(name), std::move(transform));
    _last_assigned_field_id.store(std::max(_last_assigned_field_id.load(), field_id));
    return *this;
}

PartitionSpec::Builder& PartitionSpec::Builder::add(int source_id, std::string name,
                                                    std::string transform) {
    return add(source_id, next_field_id(), std::move(name), std::move(transform));
}

std::unique_ptr<PartitionSpec> PartitionSpec::Builder::build() {
    return std::make_unique<PartitionSpec>(std::move(_schema), _spec_id, std::move(_fields),
                                           _last_assigned_field_id.load());
}

PartitionSpec::PartitionSpec(std::shared_ptr<Schema> schema, int spec_id,
                             std::vector<PartitionField> fields, int last_assigned_field_id)
        : _schema(std::move(schema)),
          _spec_id(spec_id),
          _fields(std::move(fields)),
          _last_assigned_field_id(last_assigned_field_id) {}

} // namespace iceberg
} // namespace doris
