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

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/olap_common.h"
#include "table_format_reader.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_dictionary.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"

namespace doris {
class RuntimeState;
class SlotDescriptor;
class TFileRangeDesc;
class TFileScanRangeParams;

namespace io {
struct IOContext;
} // namespace io
struct TypeDescriptor;

namespace vectorized {
class Block;
class GenericReader;
class ShardedKVCache;
class VExprContext;

class TransactionalHiveReader : public TableFormatReader {
    ENABLE_FACTORY_CREATOR(TransactionalHiveReader);

public:
    struct AcidRowID {
        int64_t original_transaction;
        int32_t bucket;
        int64_t row_id;

        struct Hash {
            size_t operator()(const AcidRowID& transactional_row_id) const {
                size_t hash_value = 0;
                hash_value ^= std::hash<int64_t> {}(transactional_row_id.original_transaction) +
                              0x9e3779b9 + (hash_value << 6) + (hash_value >> 2);
                hash_value ^= std::hash<int32_t> {}(transactional_row_id.bucket) + 0x9e3779b9 +
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

    using AcidRowIDSet = vectorized::flat_hash_set<AcidRowID, AcidRowID::Hash, AcidRowID::Eq>;

    TransactionalHiveReader(std::unique_ptr<GenericReader> file_format_reader,
                            RuntimeProfile* profile, RuntimeState* state,
                            const TFileScanRangeParams& params, const TFileRangeDesc& range,
                            io::IOContext* io_ctx);
    ~TransactionalHiveReader() override = default;

    Status init_row_filters(const TFileRangeDesc& range) override;

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status init_reader(
            const std::vector<std::string>& column_names,
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts);

private:
    struct TransactionalHiveProfile {
        RuntimeProfile::Counter* num_delete_files = nullptr;
        RuntimeProfile::Counter* num_delete_rows = nullptr;
        RuntimeProfile::Counter* delete_files_read_time = nullptr;
    };

    RuntimeProfile* _profile = nullptr;
    RuntimeState* _state = nullptr;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    TransactionalHiveProfile _transactional_orc_profile;
    AcidRowIDSet _delete_rows;
    std::unique_ptr<IColumn::Filter> _delete_rows_filter_ptr;
    std::vector<std::string> _col_names;

    io::IOContext* _io_ctx = nullptr;
};

inline bool operator<(const TransactionalHiveReader::AcidRowID& lhs,
                      const TransactionalHiveReader::AcidRowID& rhs) {
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

} // namespace vectorized
} // namespace doris
