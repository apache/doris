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

#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace doris {
namespace iceberg {

class StructLike;
class Schema;

class PartitionField {
public:
    PartitionField(int sourceId, int fieldId, std::string name, std::string transform);

    int source_id() const { return _source_id; }

    int field_id() const { return _field_id; }

    const std::string& name() const { return _name; }

    const std::string& transform() const { return _transform; }

private:
    int _source_id;
    int _field_id;
    std::string _name;
    std::string _transform;
};

class PartitionSpec {
public:
    class Builder {
    public:
        Builder(std::shared_ptr<Schema> schema);

        ~Builder() = default;

        Builder& with_spec_id(int new_spec_id);

        Builder& add(int sourceId, int fieldId, std::string name, std::string transform);

        Builder& add(int sourceId, std::string name, std::string transform);

        std::unique_ptr<PartitionSpec> build();

    private:
        int next_field_id() { return ++_last_assigned_field_id; }

    private:
        std::shared_ptr<Schema> _schema;
        std::vector<PartitionField> _fields;
        int _spec_id;
        std::atomic<int> _last_assigned_field_id;
    };

    PartitionSpec(std::shared_ptr<Schema> schema, int spec_id, std::vector<PartitionField> fields,
                  int last_assigned_field_id);

    const Schema& schema() const { return *_schema; }

    int spec_id() { return _spec_id; }

    const std::vector<PartitionField>& fields() const { return _fields; }

    int last_assigned_field_id() const { return _last_assigned_field_id; }

private:
    // IDs for partition fields start at 1000
    static constexpr int PARTITION_DATA_ID_START = 1000;
    std::shared_ptr<Schema> _schema;
    int _spec_id;
    std::vector<PartitionField> _fields;
    int _last_assigned_field_id;
};

} // namespace iceberg
} // namespace doris
