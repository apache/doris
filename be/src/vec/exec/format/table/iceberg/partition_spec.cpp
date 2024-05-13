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
//#include <atomic>
//#include <iostream>
//#include <map>
#include <memory>
#include <string>
#include <vector>

#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/exec/format/table/iceberg/struct_like.h"
#include "vec/exec/format/table/iceberg/transforms.h"

namespace doris {
namespace iceberg {

PartitionField::PartitionField(int sourceId, int fieldId, std::string name,
                               std::unique_ptr<Transform> transform)
        : _source_id(sourceId),
          _field_id(fieldId),
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

PartitionSpec::Builder& PartitionSpec::Builder::add(int sourceId, int fieldId,
                                                    const std::string& name,
                                                    std::unique_ptr<Transform> transform) {
    _fields.emplace_back(sourceId, fieldId, name, std::move(transform));
    _last_assigned_field_id.store(std::max(_last_assigned_field_id.load(), fieldId));
    return *this;
}

PartitionSpec::Builder& PartitionSpec::Builder::add(int sourceId, const std::string& name,
                                                    std::unique_ptr<Transform> transform) {
    return add(sourceId, nextFieldId(), name, std::move(transform));
}

std::unique_ptr<PartitionSpec> PartitionSpec::Builder::build() {
    return std::make_unique<PartitionSpec>(_schema, _spec_id, std::move(_fields),
                                           _last_assigned_field_id.load());
}

PartitionSpec::PartitionSpec(std::shared_ptr<Schema> schema, int spec_id,
                             std::vector<PartitionField> fields, int last_assigned_field_id)
        : _schema(std::move(schema)),
          _spec_id(spec_id),
          _fields(std::move(fields)),
          _last_assigned_field_id(last_assigned_field_id) {}

bool PartitionSpec::is_partitioned() const {
    return !_fields.empty() &&
           std::any_of(_fields.begin(), _fields.end(),
                       [](const PartitionField& f) { return !f.transform().is_void(); });
}

bool PartitionSpec::is_unpartitioned() const {
    return !is_partitioned();
}

std::string PartitionSpec::partition_to_path(const StructLike& data) {
    std::stringstream ss;

    for (size_t i = 0; i < _fields.size(); i++) {
        Type* source_type = _schema->find_type(_fields[i].source_id());
        Type& result_type = _fields[i].transform().get_result_type(*source_type);
        std::string value_string = _fields[i].transform().to_human_string(
                result_type, data.get(i, result_type.type_id()));
        if (i > 0) {
            ss << "/";
        }
        ss << _escape(_fields[i].name()) << '=' << _escape(value_string);
    }

    return ss.str();
}

std::vector<std::string> PartitionSpec::partition_values(const StructLike& data) {
    std::vector<std::string> partition_values;
    partition_values.reserve(_fields.size());
    for (size_t i = 0; i < _fields.size(); i++) {
        Type* source_type = _schema->find_type(_fields[i].source_id());
        Type& result_type = _fields[i].transform().get_result_type(*source_type);
        partition_values.emplace_back(_fields[i].transform().to_human_string(
                result_type, data.get(i, result_type.type_id())));
    }
    return partition_values;
}

std::string PartitionSpec::_escape(const std::string& str) {
    return str;
}

} // namespace iceberg
} // namespace doris
