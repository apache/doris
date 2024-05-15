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

#include <any>

#include "runtime/types.h"

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
            const doris::iceberg::PartitionField& field);
};

class PartitionColumnTransform {
public:
    PartitionColumnTransform() = default;

    virtual ~PartitionColumnTransform() = default;

    virtual bool preserves_non_null() const { return false; }

    virtual bool monotonic() const { return true; }

    virtual bool temporal() const { return false; }

    virtual const TypeDescriptor get_result_type(const TypeDescriptor& source_type) = 0;

    virtual bool is_void() const { return false; }

    virtual void apply(IColumn& column) = 0;

    virtual std::string to_human_string(const TypeDescriptor& type, const std::any& value) const;
};

class IdentityPartitionColumnTransform : public PartitionColumnTransform {
    virtual const TypeDescriptor get_result_type(const TypeDescriptor& source_type) {
        return source_type;
    }

    virtual void apply(IColumn& column) { return; }
};

} // namespace vectorized
} // namespace doris
