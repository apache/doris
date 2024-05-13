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
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace iceberg {

class Type;
class StructType;

class Schema {
public:
    //    Schema(std::vector<NestedField> columns, std::unordered_map<std::string, int> aliases)
    //            : Schema(std::move(columns), aliases, std::unordered_set<int>()) {}
    //
    //    Schema(std::vector<NestedField> columns, std::unordered_map<std::string, int> aliases, std::unordered_set<int> identifierFieldIds)
    //            : Schema(DEFAULT_SCHEMA_ID, std::move(columns), aliases, identifierFieldIds) {}
    //
    //    Schema(std::vector<NestedField> columns)
    //            : Schema(std::move(columns), std::unordered_set<int>()) {}
    //
    //    Schema(std::vector<NestedField> columns, std::unordered_set<int> identifierFieldIds)
    //            : Schema(DEFAULT_SCHEMA_ID, std::move(columns), identifierFieldIds) {}
    //
    //    Schema(int schema_id, std::vector<NestedField> columns)
    //            : Schema(schema_id, std::move(columns), std::unordered_set<int>()) {}
    //
    //    Schema(int schema_id, std::vector<NestedField> columns, std::unordered_set<int> identifierFieldIds)
    //            :   _schema_id(schema_id), _root_struct(std::move(columns)), identifierFieldIds(identifierFieldIds.begin(), identifierFieldIds.end()), _aliasToId(aliases) {
    //
    //
    //    Schema(int schema_id, std::vector<NestedField> columns, std::unordered_map<std::string, int> aliases, std::unordered_set<int> identifierFieldIds)
    //            : _schema_id(schema_id), _root_struct(std::move(columns)), identifierFieldIds(identifierFieldIds.begin(), identifierFieldIds.end()), _aliasToId(aliases) {
    //        // Validate identifier fields
    //        if (!identifierFieldIds.empty()) {
    //            validateIdentifierFields(identifierFieldIds);
    //        }
    //
    //        highestFieldId = 0;
    //        for (const auto& field : *_root_struct.fields()) {
    //            int fieldId = field.getFieldId();
    //            _id_to_field[fieldId] = &field;
    //            idToName[fieldId] = field.getFieldName();
    //            nameToId[field.getFieldName()] = fieldId;
    //            lowerCaseNameToId[toLowerCase(field.getFieldName())] = fieldId;
    //            highestFieldId = std::max(highestFieldId, fieldId);
    //        }
    //    }

    Schema(int schema_id, std::vector<NestedField> columns);
    Schema(std::vector<NestedField> columns);

    //    void validateIdentifierFields(const std::unordered_set<int>& ids) {
    //        for (int id : ids) {
    //            auto it = _id_to_field.find(id);
    //            if (it == _id_to_field.end()) {
    //                throw std::invalid_argument("Cannot add fieldId " + std::to_string(id) + " as an identifier field: field does not exist");
    //            }
    //            const auto& field = it->second;
    //            if (!field->getFieldType()->isPrimitiveType()) {
    //                throw std::invalid_argument("Cannot add field " + field->getFieldName() + " as an identifier field: not a primitive type field");
    //            }
    //            if (!field->isRequired()) {
    //                throw std::invalid_argument("Cannot add field " + field->getFieldName() + " as an identifier field: not a required field");
    //            }
    //            if (field->getFieldType()->typeId() == TypeID::DOUBLE || field->getFieldType()->typeId() == TypeID::FLOAT) {
    //                throw std::invalid_argument("Cannot add field " + field->getFieldName() + " as an identifier field: must not be float or double field");
    //            }

    // Check whether the nested field is in a chain of required struct fields
    //            int parentId = field.parentId();
    //            while (parentId != -1) {
    //                const auto& parentField = _id_to_field[parentId];
    //                if (!parentField.getFieldType()->isStructType()) {
    //                    throw std::invalid_argument("Cannot add field " + field.getFieldName() + " as an identifier field: must not be nested in " + parentField.name());
    //                }
    //                if (!parentField.isRequired()) {
    //                    throw std::invalid_argument("Cannot add field " + field.getFieldName() + " as an identifier field: must not be nested in an optional field " + parentField.name());
    //                }
    //                parentId = parentField.parentId();
    //            }
    //        }
    //    }

    //    Schema(std::initializer_list<NestedField> columns) : Schema(DEFAULT_SCHEMA_ID, std::vector<NestedField>(columns)) {}
    //    Schema(int schema_id, std::initializer_list<NestedField> columns) : Schema(schema_id, std::vector<NestedField>(columns)) {}

    //    std::string toString() const {
    //        std::string result = "table {" + NEWLINE;
    //        for (const auto& field : _root_struct.fields()) {
    //            result += "  " + field.toString() + (identifierFieldIdSet.count(field.fieldId()) ? " (id)" : "") + NEWLINE;
    //        }
    //        result += "}";
    //        return result;
    //        return "";
    //    }

    int schema_id() const { return _schema_id; }

    const StructType& root_struct() const { return _root_struct; }

    const std::vector<NestedField>& columns() const { return *_root_struct.fields(); }

    //    const std::unordered_map<std::string, int>& getAliases() const {
    //        return _aliasToId;
    //    }
    //
    //    const std::unordered_map<int, std::string>& getIdToName() const {
    //        return idToName;
    //    }

    //    Type* findType(const std::string& name) const {
    //        auto it = nameToId.find(name);
    //        if (it != nameToId.end()) {
    //            return findType(it->second);
    //        }
    //        return nullptr;
    //    }

    Type* find_type(int id) const;

    const NestedField* find_field(int id) const;

    //    const NestedField* find_field(const std::string& name) const {
    //        auto it = nameToId.find(name);
    //        if (it != nameToId.end()) {
    //            return find_field(it->second);
    //        }
    //        return nullptr;
    //    }

    //    const NestedField* caseInsensitiveFindField(const std::string& name) const {
    //        auto it = lowerCaseNameToId.find(toLowerCase(name));
    //        if (it != lowerCaseNameToId.end()) {
    //            return find_field(it->second);
    //        }
    //        return nullptr;
    //    }

    //    std::string findColumnName(int id) const {
    //        auto it = idToName.find(id);
    //        if (it != idToName.end()) {
    //            return it->second;
    //        }
    //        return "";
    //    }
    //
    //    int aliasToId(const std::string& alias) const {
    //        auto it = _aliasToId.find(alias);
    //        if (it != _aliasToId.end()) {
    //            return it->second;
    //        }
    //        return -1;
    //    }
    //
    //    std::string idToAlias(int fieldId) const {
    //        for (const auto& entry : _aliasToId) {
    //            if (entry.second == fieldId) {
    //                return entry.first;
    //            }
    //        }
    //        return "";
    //    }

    //    std::shared_ptr<Accessor<StructLike>> accessorForField(int id) const {
    //        auto it = idToAccessor.find(id);
    //        if (it != idToAccessor.end()) {
    //            return it->second;
    //        }
    //        return nullptr;
    //    }

    //    Schema select(const std::unordered_set<std::string>& names) const {
    //        if (names.count(ALL_COLUMNS) > 0) {
    //            return *this;
    //        }
    //
    //        std::unordered_set<int> selectedIds;
    //        for (const auto& name : names) {
    //            auto it = nameToId.find(name);
    //            if (it != nameToId.end()) {
    //                selectedIds.insert(it->second);
    //            }
    //        }
    //
    //        return select(selectedIds);
    //    }
    //
    //    Schema caseInsensitiveSelect(const std::unordered_set<std::string>& names) const {
    //        if (names.count(ALL_COLUMNS) > 0) {
    //            return *this;
    //        }
    //
    //        std::unordered_set<int> selectedIds;
    //        for (const auto& name : names) {
    //            auto it = lowerCaseNameToId.find(toLowerCase(name));
    //            if (it != lowerCaseNameToId.end()) {
    //                selectedIds.insert(it->second);
    //            }
    //        }
    //
    //        return select(selectedIds);
    //    }

    //    bool sameSchema(const Schema& anotherSchema) const {
    ////        return _root_struct == anotherSchema.getStruct() &&
    ////               identifierFieldIds == anotherSchema.identifierFieldIds;
    //        return false;
    //    }

private:
    //    Schema select(const std::unordered_set<int>& ids) const {
    //        return Schema(schema_id, TypeUtil::select(_root_struct, ids), aliasToId, ids);
    //    }

    //    std::string toLowerCase(const std::string& str) const {
    //        std::string result = str;
    //        std::transform(result.begin(), result.end(), result.begin(),
    //                       [](unsigned char c) { return std::tolower(c); });
    //        return result;
    //    }

private:
    static const char NEWLINE = '\n';
    static const std::string ALL_COLUMNS;
    static const int DEFAULT_SCHEMA_ID;

    int _schema_id;
    StructType _root_struct;
    //    std::vector<int> identifierFieldIds;

    //    std::unordered_map<std::string, int> _aliasToId;
    std::unordered_map<int, const NestedField*> _id_to_field;
    //    std::unordered_map<std::string, int> nameToId;
    //    std::unordered_map<std::string, int> lowerCaseNameToId;
    //    std::unordered_map<int, std::shared_ptr<Accessor<StructLike>>> idToAccessor;
    //    std::unordered_map<int, std::string> idToName;
    //    std::unordered_set<int> identifierFieldIdSet;
};

} // namespace iceberg
} // namespace doris