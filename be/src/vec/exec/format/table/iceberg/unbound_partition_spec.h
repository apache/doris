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

#include <string>
#include <vector>

#include "partition_spec.h"
#include "vec/exec/format/table/iceberg/partition_spec.h"

namespace doris {
namespace iceberg {

struct UnboundPartitionField {
    std::string _transform;
    int _source_id;
    const int _partition_id;
    std::string _name;

    UnboundPartitionField(std::string transform, int source_id, int partition_id, std::string name)
            : _transform(std::move(transform)),
              _source_id(source_id),
              _partition_id(partition_id),
              _name(std::move(name)) {}
};

class UnboundPartitionSpec {
public:
    class Builder {
    public:
        Builder& with_spec_id(int new_spec_id);

        Builder& add_field(const std::string& transform_as_string, int source_id,
                           const std::string& name);

        Builder& add_field(const std::string& transform_as_string, int source_id, int partition_id,
                           const std::string& name);

        std::unique_ptr<UnboundPartitionSpec> build();

    private:
        int _spec_id = 0;
        std::vector<UnboundPartitionField> _fields;
    };

    UnboundPartitionSpec(int specId, std::vector<UnboundPartitionField> fields);

    std::unique_ptr<PartitionSpec> bind(const std::shared_ptr<Schema>& schema) const;

    int spec_id() const { return _spec_id; }

private:
    std::unique_ptr<PartitionSpec::Builder> _copy_to_builder(
            const std::shared_ptr<Schema>& schema) const;
    int _spec_id;
    std::vector<UnboundPartitionField> _fields;
};

} // namespace iceberg
} // namespace doris
