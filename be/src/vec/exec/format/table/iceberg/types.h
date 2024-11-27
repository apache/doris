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

#include <iostream>
#include <map>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/exception.h"

namespace doris {
namespace iceberg {

class PrimitiveType;
class StructType;
class ListType;
class MapType;
class NestedType;

enum TypeID {
    BOOLEAN,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    TIME,
    TIMESTAMP,
    STRING,
    UUID,
    FIXED,
    BINARY,
    DECIMAL,
    STRUCT,
    LIST,
    MAP
};

class Type {
public:
    virtual ~Type() = default;
    virtual TypeID type_id() const = 0;
    virtual bool is_primitive_type() { return false; }
    virtual bool is_nested_type() { return false; }
    virtual bool is_struct_type() { return false; }
    virtual bool is_list_type() { return false; }
    virtual bool is_map_type() { return false; }
    virtual PrimitiveType* as_primitive_type() {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Not a primitive type.");
    }
    virtual StructType* as_struct_type() {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Not a struct type.");
    }
    virtual ListType* as_list_type() {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Not a list type.");
    }
    virtual MapType* as_map_type() {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Not a map type.");
    }
    virtual NestedType* as_nested_type() {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Not a nested type.");
    }

    virtual std::string to_string() const = 0;
};

class NestedField {
public:
    NestedField(bool optional, int field_id, std::string field_name,
                std::unique_ptr<Type> field_type, std::optional<std::string> field_doc)
            : _is_optional(optional),
              _id(field_id),
              _name(std::move(field_name)),
              _type(std::move(field_type)),
              _doc(std::move(field_doc)) {}

    bool is_optional() const { return _is_optional; }

    bool is_required() const { return !_is_optional; }

    int field_id() const { return _id; }

    const std::string& field_name() const { return _name; }

    Type* field_type() const { return _type.get(); }

    const std::optional<std::string>& field_doc() const { return _doc; }

private:
    bool _is_optional;
    int _id;
    std::string _name;
    std::unique_ptr<Type> _type;
    std::optional<std::string> _doc;
};

class NestedType : public Type {
public:
    virtual ~NestedType() override = default;

    bool is_nested_type() override { return true; }
    NestedType* as_nested_type() override { return this; }
    virtual int field_count() const = 0;
    virtual Type* field_type(const std::string& field_name) = 0;
    virtual const NestedField* field(int field_id) = 0;
};

class PrimitiveType : public Type {
public:
    virtual ~PrimitiveType() override = default;
    bool is_primitive_type() override { return true; }
    PrimitiveType* as_primitive_type() override { return this; }
};

class MapType : public NestedType {
public:
    ~MapType() = default;
    static std::unique_ptr<MapType> of_optional(int key_id, int value_id,
                                                std::unique_ptr<Type> key_type,
                                                std::unique_ptr<Type> value_type);
    static std::unique_ptr<MapType> of_required(int key_id, int value_id,
                                                std::unique_ptr<Type> key_type,
                                                std::unique_ptr<Type> value_type);

    const NestedField& key_field() const;
    const NestedField& value_field() const;
    Type* key_type() const;
    Type* value_type() const;
    int key_id() const;
    int value_id() const;
    bool is_value_required() const;
    bool is_value_optional() const;
    TypeID type_id() const override { return TypeID::MAP; }
    bool is_map_type() override { return true; }

    MapType* as_map_type() override { return this; }

    virtual int field_count() const override { return 2; }

    virtual Type* field_type(const std::string& field_name) override;

    virtual const NestedField* field(int field_id) override;

    std::string to_string() const override;

private:
    MapType(std::unique_ptr<NestedField> key_field, std::unique_ptr<NestedField> value_field)
            : _key_field(std::move(key_field)), _value_field(std::move(value_field)) {}
    friend std::unique_ptr<MapType> of_optional(int key_id, int value_id,
                                                std::unique_ptr<Type> key_type,
                                                std::unique_ptr<Type> value_type);
    friend std::unique_ptr<MapType> of_required(int key_id, int value_id,
                                                std::unique_ptr<Type> key_type,
                                                std::unique_ptr<Type> value_type);

    std::unique_ptr<NestedField> _key_field;
    std::unique_ptr<NestedField> _value_field;
    std::vector<NestedField*> _fields;
};

class ListType : public NestedType {
public:
    ~ListType() = default;
    static std::unique_ptr<ListType> of_optional(int element_id,
                                                 std::unique_ptr<Type> element_type);
    static std::unique_ptr<ListType> of_required(int element_id,
                                                 std::unique_ptr<Type> element_type);

    virtual TypeID type_id() const override { return TypeID::LIST; }

    bool is_list_type() override { return true; }

    ListType* as_list_type() override { return this; }

    const NestedField& element_field() const { return _element_field; }

    virtual std::string to_string() const override;

    virtual int field_count() const override { return 1; }

    virtual Type* field_type(const std::string& field_name) override;

    virtual const NestedField* field(int field_id) override;

private:
    ListType(NestedField element_field) : _element_field(std::move(element_field)) {}

    friend std::unique_ptr<ListType> of_optional(int element_id,
                                                 std::unique_ptr<Type> element_type);
    friend std::unique_ptr<ListType> of_required(int element_id,
                                                 std::unique_ptr<Type> element_type);

    NestedField _element_field;
};

class StructType : public NestedType {
public:
    ~StructType() = default;
    StructType(std::vector<NestedField> fields) : _fields(std::move(fields)) {
        for (const NestedField& field : _fields) {
            _fields_by_id.insert({field.field_id(), &field});
            _fields_by_name.insert({field.field_name(), &field});
        }
    }

    StructType(const StructType& other) {}

    virtual TypeID type_id() const override { return TypeID::STRUCT; }

    bool is_struct_type() override { return true; }

    StructType* as_struct_type() override { return this; }

    virtual std::string to_string() const override;

    virtual int field_count() const override { return _fields.size(); }

    virtual Type* field_type(const std::string& field_name) override;

    virtual const NestedField* field(int field_id) override { return _fields_by_id[field_id]; }

    const std::vector<NestedField>& fields() const { return _fields; }

    std::vector<NestedField>&& move_fields() { return std::move(_fields); }

private:
    std::vector<NestedField> _fields;
    std::unordered_map<int, const NestedField*> _fields_by_id;
    std::unordered_map<std::string, const NestedField*> _fields_by_name;
};

class DecimalType : public PrimitiveType {
private:
    int scale;
    int precision;

public:
    ~DecimalType() = default;

    DecimalType(int p, int s) : scale(s), precision(p) {}

    virtual TypeID type_id() const override { return TypeID::DECIMAL; }

    std::string to_string() const override {
        std::stringstream ss;
        ss << "decimal(" << precision << ", " << scale << ")";
        return ss.str();
    }
};

class BinaryType : public PrimitiveType {
public:
    ~BinaryType() = default;

    TypeID type_id() const override { return TypeID::BINARY; }

    std::string to_string() const override { return "binary"; }
};

class FixedType : public PrimitiveType {
public:
    ~FixedType() = default;
    FixedType(int len) : length(len) {}

    virtual TypeID type_id() const override { return TypeID::FIXED; }

    std::string to_string() const override {
        std::stringstream ss;
        ss << "fixed[" << length << "]";
        return ss.str();
    }

private:
    int length;
};

class UUIDType : public PrimitiveType {
public:
    ~UUIDType() = default;

    TypeID type_id() const override { return TypeID::UUID; }

    std::string to_string() const override { return "uuid"; }
};

class StringType : public PrimitiveType {
public:
    ~StringType() = default;

    TypeID type_id() const override { return TypeID::STRING; }

    std::string to_string() const override { return "string"; }
};

class TimestampType : public PrimitiveType {
public:
    ~TimestampType() = default;

    TimestampType(bool adjust_to_utc) : _adjust_to_utc(adjust_to_utc) {}

    virtual TypeID type_id() const override { return TypeID::TIMESTAMP; }

    bool should_adjust_to_utc() const { return _adjust_to_utc; }

    std::string to_string() const override {
        if (should_adjust_to_utc()) {
            return "timestamptz";
        } else {
            return "timestamp";
        }
    }

private:
    bool _adjust_to_utc;
};

class TimeType : public PrimitiveType {
public:
    ~TimeType() = default;

    TypeID type_id() const override { return TypeID::TIME; }

    std::string to_string() const override { return "time"; }
};

class DateType : public PrimitiveType {
public:
    ~DateType() = default;

    TypeID type_id() const override { return TypeID::DATE; }

    std::string to_string() const override { return "date"; }
};

class DoubleType : public PrimitiveType {
public:
    ~DoubleType() = default;

    TypeID type_id() const override { return TypeID::DOUBLE; }

    std::string to_string() const override { return "double"; }
};

class FloatType : public PrimitiveType {
public:
    TypeID type_id() const override { return TypeID::FLOAT; }

    std::string to_string() const override { return "float"; }
};

class LongType : public PrimitiveType {
public:
    ~LongType() = default;

    TypeID type_id() const override { return TypeID::LONG; }

    std::string to_string() const override { return "long"; }
};

class IntegerType : public PrimitiveType {
public:
    ~IntegerType() = default;

    TypeID type_id() const override { return TypeID::INTEGER; }

    std::string to_string() const override { return "int"; }
};

class BooleanType : public PrimitiveType {
public:
    ~BooleanType() = default;

    TypeID type_id() const override { return TypeID::BOOLEAN; }

    std::string to_string() const override { return "boolean"; }
};

class Types {
public:
    static std::unique_ptr<PrimitiveType> from_primitive_string(const std::string& type_string);
};

} // namespace iceberg
} // namespace doris
