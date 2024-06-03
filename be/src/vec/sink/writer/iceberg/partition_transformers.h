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

#include "runtime/types.h"
#include "util/bit_util.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/function_string.h"

namespace doris {

namespace iceberg {
class Type;
class PartitionField;
}; // namespace iceberg

namespace vectorized {

class IColumn;
class PartitionColumnTransform;

class PartitionColumnTransforms {
private:
    PartitionColumnTransforms();

public:
    static std::unique_ptr<PartitionColumnTransform> create(
            const doris::iceberg::PartitionField& field, const TypeDescriptor& source_type);
};

class PartitionColumnTransform {
public:
    PartitionColumnTransform() = default;

    virtual ~PartitionColumnTransform() = default;

    virtual bool preserves_non_null() const { return false; }

    virtual bool monotonic() const { return true; }

    virtual bool temporal() const { return false; }

    virtual const TypeDescriptor& get_result_type() const = 0;

    virtual bool is_void() const { return false; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) = 0;

    virtual std::string to_human_string(const TypeDescriptor& type, const std::any& value) const;
};

class IdentityPartitionColumnTransform : public PartitionColumnTransform {
public:
    IdentityPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type) {}

    virtual const TypeDescriptor& get_result_type() const { return _source_type; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx);

private:
    TypeDescriptor _source_type;
};

} // namespace vectorized
} // namespace doris
