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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "core/data_type/data_type.h"

namespace doris::format::parquet {

struct ParquetColumnSchema;

// V2-owned semantic mapping consumed by the native decoder. It deliberately contains no V1
// reader/schema-helper state, so FileScannerV2 can build the mapping from its native metadata tree.
class NativeSchemaNode {
public:
    virtual ~NativeSchemaNode() = default;

    virtual std::shared_ptr<NativeSchemaNode> child(const std::string&) const { return nullptr; }
    virtual std::string file_child_name(const std::string&) const { return {}; }
    virtual bool has_child(const std::string&) const { return false; }
    virtual std::shared_ptr<NativeSchemaNode> element() const { return nullptr; }
    virtual std::shared_ptr<NativeSchemaNode> key() const { return nullptr; }
    virtual std::shared_ptr<NativeSchemaNode> value() const { return nullptr; }
};

class NativeScalarSchemaNode final : public NativeSchemaNode {};

class NativeStructSchemaNode final : public NativeSchemaNode {
public:
    void add_child(std::string table_name, std::string file_name,
                   std::shared_ptr<NativeSchemaNode> node);
    void add_missing_child(std::string table_name);

    std::shared_ptr<NativeSchemaNode> child(const std::string& table_name) const override;
    std::string file_child_name(const std::string& table_name) const override;
    bool has_child(const std::string& table_name) const override;

private:
    struct Child {
        std::string file_name;
        std::shared_ptr<NativeSchemaNode> node;
    };
    std::map<std::string, Child> _children;
};

class NativeArraySchemaNode final : public NativeSchemaNode {
public:
    explicit NativeArraySchemaNode(std::shared_ptr<NativeSchemaNode> element)
            : _element(std::move(element)) {}
    std::shared_ptr<NativeSchemaNode> element() const override { return _element; }

private:
    std::shared_ptr<NativeSchemaNode> _element;
};

class NativeMapSchemaNode final : public NativeSchemaNode {
public:
    NativeMapSchemaNode(std::shared_ptr<NativeSchemaNode> key,
                        std::shared_ptr<NativeSchemaNode> value)
            : _key(std::move(key)), _value(std::move(value)) {}
    std::shared_ptr<NativeSchemaNode> key() const override { return _key; }
    std::shared_ptr<NativeSchemaNode> value() const override { return _value; }

private:
    std::shared_ptr<NativeSchemaNode> _key;
    std::shared_ptr<NativeSchemaNode> _value;
};

Status build_native_schema_node(const DataTypePtr& projected_type,
                                const ParquetColumnSchema& file_schema,
                                std::shared_ptr<NativeSchemaNode>* result);

} // namespace doris::format::parquet
