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

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/data_type/define_primitive_type.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"

namespace doris {
struct TransactionalHive {
    static const std::string OPERATION;
    static const std::string ORIGINAL_TRANSACTION;
    static const std::string BUCKET;
    static const std::string ROW_ID;
    static const std::string CURRENT_TRANSACTION;
    static const std::string ROW;
    static const std::string OPERATION_LOWER_CASE;
    static const std::string ORIGINAL_TRANSACTION_LOWER_CASE;
    static const std::string BUCKET_LOWER_CASE;
    static const std::string ROW_ID_LOWER_CASE;
    static const std::string CURRENT_TRANSACTION_LOWER_CASE;
    static const std::string ROW_LOWER_CASE;
    static const int ROW_OFFSET;
    struct Param {
        const std::string column_name;
        const std::string column_lower_case;
        const PrimitiveType type;
    };
    static const std::vector<Param> DELETE_ROW_PARAMS;
    static const std::vector<Param> READ_PARAMS;
    static const std::vector<std::string> DELETE_ROW_COLUMN_NAMES;
    static const std::vector<std::string> DELETE_ROW_COLUMN_NAMES_LOWER_CASE;
    static const std::vector<std::string> READ_ROW_COLUMN_NAMES;
    static const std::vector<std::string> READ_ROW_COLUMN_NAMES_LOWER_CASE;
    static const std::vector<std::string> ACID_COLUMN_NAMES;
    static const std::vector<std::string> ACID_COLUMN_NAMES_LOWER_CASE;

    static const std::unordered_map<std::string, uint32_t> DELETE_COL_NAME_TO_BLOCK_IDX;
};

// ACID row identifier for transactional Hive tables, used for delete row matching.
// Placed here (not in TransactionalHiveReader) to avoid circular dependency with OrcReader.
struct AcidRowID {
    int64_t original_transaction;
    int64_t bucket;
    int64_t row_id;

    struct Hash {
        size_t operator()(const AcidRowID& transactional_row_id) const {
            size_t hash_value = 0;
            hash_value ^= std::hash<int64_t> {}(transactional_row_id.original_transaction) +
                          0x9e3779b9 + (hash_value << 6) + (hash_value >> 2);
            hash_value ^= std::hash<int64_t> {}(transactional_row_id.bucket) + 0x9e3779b9 +
                          (hash_value << 6) + (hash_value >> 2);
            hash_value ^= std::hash<int64_t> {}(transactional_row_id.row_id) + 0x9e3779b9 +
                          (hash_value << 6) + (hash_value >> 2);
            return hash_value;
        }
    };

    struct Eq {
        bool operator()(const AcidRowID& lhs, const AcidRowID& rhs) const {
            return lhs.original_transaction == rhs.original_transaction &&
                   lhs.bucket == rhs.bucket && lhs.row_id == rhs.row_id;
        }
    };
};

using AcidRowIDSet = flat_hash_set<AcidRowID, AcidRowID::Hash, AcidRowID::Eq>;

inline bool operator<(const AcidRowID& lhs, const AcidRowID& rhs) {
    if (lhs.original_transaction != rhs.original_transaction) {
        return lhs.original_transaction < rhs.original_transaction;
    } else if (lhs.bucket != rhs.bucket) {
        return lhs.bucket < rhs.bucket;
    } else if (lhs.row_id != rhs.row_id) {
        return lhs.row_id < rhs.row_id;
    } else {
        return false;
    }
}

} // namespace doris
