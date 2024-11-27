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

#include "vec/exec/format/table/iceberg/struct_like.h"

namespace doris {
namespace vectorized {

class PartitionData : public iceberg::StructLike {
public:
    explicit PartitionData(std::vector<std::any> partition_values)
            : _partition_values(std::move(partition_values)) {}

    int size() const override { return _partition_values.size(); }

    std::any get(int pos) const override {
        if (pos < 0 || pos >= _partition_values.size()) {
            throw std::out_of_range("Index out of range");
        }
        return _partition_values[pos];
    }

    void set(int pos, const std::any& value) override {
        if (pos < 0 || pos >= _partition_values.size()) {
            throw std::out_of_range("Index out of range");
        }
        _partition_values[pos] = value;
    }

private:
    std::vector<std::any> _partition_values;
};

} // namespace vectorized
} // namespace doris
