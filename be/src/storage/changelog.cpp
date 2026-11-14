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

#include "storage/changelog.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/olap_file.pb.h>

#include <unordered_map>
#include <unordered_set>

#include "storage/tablet/tablet_schema.h"

namespace doris::changelog {

static bool is_compatible_type_shape(const TabletColumn& source, const TabletColumn& target,
                                     bool allow_nullable_widening) {
    if (source.type() != target.type() || source.length() != target.length() ||
        source.get_subtype_count() != target.get_subtype_count() ||
        (source.is_nullable() && !target.is_nullable()) ||
        (!allow_nullable_widening && source.is_nullable() != target.is_nullable())) {
        return false;
    }
    // INT columns built from PB and Thrift can carry different unused precision/scale
    // defaults (-1 versus 0). Compare these attributes only for types that use them.
    switch (source.type()) {
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        if (source.precision() != target.precision() || source.frac() != target.frac()) {
            return false;
        }
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        if (source.frac() != target.frac()) {
            return false;
        }
        break;
    default:
        break;
    }
    for (uint32_t i = 0; i < source.get_subtype_count(); ++i) {
        if (!is_compatible_type_shape(source.get_sub_column(i), target.get_sub_column(i), false)) {
            return false;
        }
    }
    return true;
}

Status validate_mapping(const PChangelogWriteColumnMappings& mapping) {
    if (!mapping.has_need_historical_value()) {
        return Status::InvalidArgument("changelog mapping misses need_historical_value");
    }
    std::unordered_set<int32_t> source_uids;
    std::unordered_set<int32_t> target_uids;
    for (const auto& entry : mapping.entries()) {
        if (!entry.has_source_column_unique_id() || !entry.has_current_column_unique_id()) {
            return Status::InvalidArgument("changelog mapping misses source/current UID");
        }
        if (!source_uids.insert(entry.source_column_unique_id()).second ||
            !target_uids.insert(entry.current_column_unique_id()).second) {
            return Status::InvalidArgument("duplicate changelog source/current UID");
        }
        if (entry.has_before_column_unique_id() &&
            (!mapping.need_historical_value() ||
             !target_uids.insert(entry.before_column_unique_id()).second)) {
            return Status::InvalidArgument("invalid or duplicate changelog before UID");
        }
    }
    return Status::OK();
}

Status mapping_from_thrift(const TChangelogWriteColumnMappings& input,
                           PChangelogWriteColumnMappings* output) {
    if (!input.__isset.entries || !input.__isset.need_historical_value) {
        return Status::InvalidArgument("changelog mapping misses entries/historical flag");
    }
    output->Clear();
    output->set_need_historical_value(input.need_historical_value);
    for (const auto& entry : input.entries) {
        if (!entry.__isset.source_column_unique_id || !entry.__isset.current_column_unique_id) {
            return Status::InvalidArgument("changelog mapping misses source/current UID");
        }
        auto* encoded = output->add_entries();
        encoded->set_source_column_unique_id(entry.source_column_unique_id);
        encoded->set_current_column_unique_id(entry.current_column_unique_id);
        if (entry.__isset.before_column_unique_id) {
            encoded->set_before_column_unique_id(entry.before_column_unique_id);
        }
    }
    return validate_mapping(*output);
}

Status resolve_layout(const TabletSchemaSPtr& source, const TabletSchemaSPtr& target,
                      const PChangelogWriteColumnMappings& mapping,
                      std::shared_ptr<const ResolvedChangelogLayout>* result) {
    RETURN_IF_ERROR(validate_mapping(mapping));
    if (source == nullptr || target == nullptr) {
        return Status::InvalidArgument("changelog layout requires source and target schemas");
    }
    auto layout = std::make_shared<ResolvedChangelogLayout>();
    layout->source_schema = source;
    layout->target_schema = target;
    layout->need_historical_value = mapping.need_historical_value();
    const int tso = target->changelog_tso_col_idx();
    const int lsn = target->changelog_lsn_col_idx();
    const int op = target->changelog_op_col_idx();
    if (tso < 0 || lsn < 0 || op < 0) {
        return Status::InvalidArgument("changelog schema misses its hidden columns");
    }
    layout->tso_cid = tso;
    layout->lsn_cid = lsn;
    layout->op_cid = op;
    if (target->column(tso).type() != FieldType::OLAP_FIELD_TYPE_BIGINT ||
        !target->column(tso).is_nullable() ||
        target->column(lsn).type() != FieldType::OLAP_FIELD_TYPE_BIGINT ||
        target->column(lsn).is_nullable() ||
        target->column(op).type() != FieldType::OLAP_FIELD_TYPE_BIGINT) {
        return Status::InvalidArgument("invalid changelog system column types");
    }
    std::unordered_map<int32_t, uint32_t> source_cids;
    std::unordered_map<int32_t, uint32_t> target_cids;
    for (uint32_t cid = 0; cid < source->num_columns(); ++cid) {
        const auto& column = source->column(cid);
        // Hidden non-key execution columns (delete sign, sequence, TSO) have
        // no changelog image and are outside the mapping's source namespace.
        if (!column.visible() && !column.is_key()) {
            continue;
        }
        if (!source_cids.emplace(column.unique_id(), cid).second) {
            return Status::InvalidArgument("duplicate source schema UID");
        }
    }
    for (uint32_t cid = 0; cid < target->num_columns(); ++cid) {
        if (!target_cids.emplace(target->column(cid).unique_id(), cid).second) {
            return Status::InvalidArgument("duplicate changelog schema UID");
        }
    }
    std::unordered_set<uint32_t> mapped_target = {layout->tso_cid, layout->lsn_cid, layout->op_cid};
    for (const auto& entry : mapping.entries()) {
        auto src = source_cids.find(entry.source_column_unique_id());
        auto dst = target_cids.find(entry.current_column_unique_id());
        if (src == source_cids.end() || dst == target_cids.end()) {
            return Status::InvalidArgument("unknown changelog source/current UID: {}/{}",
                                           entry.source_column_unique_id(),
                                           entry.current_column_unique_id());
        }
        const auto& source_col = source->column(src->second);
        const auto& target_col = target->column(dst->second);
        if (source_col.is_key() != target_col.is_key() ||
            !is_compatible_type_shape(source_col, target_col, !source_col.is_key()) ||
            !mapped_target.insert(dst->second).second) {
            return Status::InvalidArgument("invalid changelog current mapping for UID {}",
                                           entry.source_column_unique_id());
        }
        layout->current_source_cids.push_back(src->second);
        layout->current_target_cids.push_back(dst->second);
        layout->current_nullable_widening.push_back(!source_col.is_nullable() &&
                                                    target_col.is_nullable());
        const bool needs_before = mapping.need_historical_value() && !source_col.is_key();
        if (entry.has_before_column_unique_id() != needs_before) {
            return Status::InvalidArgument("incomplete changelog before mapping for UID {}",
                                           entry.source_column_unique_id());
        }
        if (needs_before) {
            auto before = target_cids.find(entry.before_column_unique_id());
            if (before == target_cids.end() || !mapped_target.insert(before->second).second) {
                return Status::InvalidArgument("unknown or reused changelog before UID {}",
                                               entry.before_column_unique_id());
            }
            const auto& before_col = target->column(before->second);
            if (before_col.is_key() || !before_col.is_nullable() ||
                !is_compatible_type_shape(source_col, before_col, true)) {
                return Status::InvalidArgument("invalid changelog before type for UID {}",
                                               entry.before_column_unique_id());
            }
            layout->before_source_cids.push_back(src->second);
            layout->before_target_cids.push_back(before->second);
        }
    }
    // validate_mapping guarantees unique source UIDs, and every entry resolved above.
    // Equal counts therefore prove coverage of all visible/key source columns.
    if (static_cast<size_t>(mapping.entries_size()) != source_cids.size()) {
        return Status::InvalidArgument("unmapped changelog source columns: expected {}, actual {}",
                                       source_cids.size(), mapping.entries_size());
    }
    if (mapped_target.size() != target->num_columns()) {
        return Status::InvalidArgument("unmapped changelog target columns");
    }
    *result = std::move(layout);
    return Status::OK();
}
} // namespace doris::changelog
