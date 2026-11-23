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

#include "storage/segment/variant/variant_desc_fingerprint.h"

#include <bvar/bvar.h>
#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"
#include "exec/common/variant_util.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/coding.h"
#include "util/json/path_in_data.h"
#include "util/sha.h"

namespace doris::variant_util {

namespace {

constexpr uint32_t fingerprint_format_version = 1;
constexpr size_t sha256_hex_size = 2 * SHA256_DIGEST_LENGTH;

bvar::Adder<int64_t> desc_rowsets_read("variant_desc_fingerprint_rowsets_read");
bvar::Adder<int64_t> desc_rowsets_skipped("variant_desc_fingerprint_rowsets_skipped");

std::string sha256(std::string_view input) {
    SHA256Digest digest;
    digest.reset(input.data(), input.size());
    return std::string(digest.digest());
}

bool is_sha256_hex(std::string_view value) {
    return value.size() == sha256_hex_size && std::ranges::all_of(value, [](char c) {
               return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
           });
}

// Keep the type information used by DataTypeFactory and the physical Variant
// path interpretation. Offsets, column ordinals, counts and statistics vary
// between files with the same schema.
void project_column(const segment_v2::ColumnMetaPB& input, segment_v2::ColumnMetaPB* result) {
    result->set_type(input.type());
    result->set_is_nullable(input.is_nullable());
    result->set_precision(input.precision());
    result->set_frac(input.frac());
    result->set_result_is_nullable(input.result_is_nullable());
    result->set_function_name(input.function_name());
    result->set_be_exec_version(input.be_exec_version());
    result->set_variant_path_encoding(input.variant_path_encoding());
    result->set_variant_enable_doc_mode(input.variant_enable_doc_mode());
    if (input.has_column_path_info()) {
        result->mutable_column_path_info()->CopyFrom(input.column_path_info());
    }
    for (const auto& child : input.children_columns()) {
        project_column(child, result->add_children_columns());
    }
}

std::string physical_segment_set_digest(const RowsetMeta& rowset) {
    std::string input;
    put_length_prefixed_slice(&input, Slice(rowset.rowset_id().to_string()));
    put_length_prefixed_slice(&input, Slice(rowset.resource_id()));
    put_fixed64_le(&input, rowset.tablet_id());
    put_fixed64_le(&input, rowset.num_segments());
    for (auto segment : rowset.segments()) {
        put_fixed64_le(&input, segment.id());
    }
    return sha256(input);
}

std::string_view validated_rowset_digest(const RowsetMeta& rowset) {
    const auto& fingerprint = rowset.variant_desc_fingerprint();
    if (fingerprint.format_version() != fingerprint_format_version ||
        !is_sha256_hex(fingerprint.layout_sha256()) ||
        fingerprint.segment_set_sha256() != physical_segment_set_digest(rowset)) {
        return {};
    }
    return fingerprint.layout_sha256();
}

} // namespace

std::string build_segment_desc_fingerprint(const segment_v2::SegmentFooterPB& footer,
                                           const TabletSchema& schema) {
    if (schema.num_variant_columns() == 0) {
        return {};
    }
    std::set<int32_t> root_ids;
    std::set<std::pair<int32_t, std::string>> paths;
    std::vector<std::string> entries;
    for (const auto& column : footer.columns()) {
        const bool root = column.type() == static_cast<int>(FieldType::OLAP_FIELD_TYPE_VARIANT);
        if (!root && !column.has_column_path_info()) {
            continue;
        }

        const auto uid = root ? static_cast<int32_t>(column.unique_id())
                              : column.column_path_info().parrent_column_unique_id();
        const auto parent = schema.field_index(uid);
        if (parent < 0 || !schema.column(parent).is_variant_type() ||
            column.sparse_columns_size() != 0) {
            return {};
        }

        PathInData full_path;
        if (column.has_column_path_info()) {
            full_path.from_protobuf(column.column_path_info());
        }
        const auto relative = full_path.copy_pop_front();
        if (root) {
            if (!relative.empty() || !root_ids.emplace(uid).second) {
                return {};
            }
        } else {
            const auto path = relative.get_path();
            if (relative.empty() || relative.has_nested_part() ||
                segment_v2::contains_nested_group_marker(path) ||
                !paths.emplace(uid, path).second) {
                return {};
            }
        }

        segment_v2::ColumnMetaPB projected;
        project_column(column, &projected);
        if (root) {
            projected.set_unique_id(column.unique_id());
        }
        entries.emplace_back(TabletSchema::deterministic_string_serialize(projected));
    }

    if (root_ids.size() != schema.num_variant_columns()) {
        return {};
    }
    for (const auto& [uid, path] : paths) {
        // The reader's tree can resolve a leaf and its descendants based on insertion order.
        const auto child = paths.lower_bound({uid, path + "."});
        if (child != paths.end() && child->first == uid && child->second.starts_with(path + ".")) {
            return {};
        }
    }

    std::ranges::sort(entries);
    std::string input;
    put_fixed32_le(&input, fingerprint_format_version);
    put_fixed32_le(&input, static_cast<uint32_t>(schema.storage_format()));
    for (const auto& entry : entries) {
        put_length_prefixed_slice(&input, Slice(entry));
    }
    return sha256(input);
}

void set_rowset_desc_fingerprint(RowsetMeta& rowset,
                                 const std::vector<std::pair<int64_t, std::string>>& segments) {
    rowset.clear_variant_desc_fingerprint();
    if (segments.empty() || segments.size() != static_cast<size_t>(rowset.num_segments())) {
        return;
    }

    std::vector<int64_t> segment_ids;
    std::vector<std::string_view> layouts;
    segment_ids.reserve(segments.size());
    layouts.reserve(segments.size());
    for (const auto& [id, digest] : segments) {
        if (!is_sha256_hex(digest)) {
            return;
        }
        segment_ids.push_back(id);
        layouts.emplace_back(digest);
    }
    std::ranges::sort(segment_ids);
    for (auto segment : rowset.segments()) {
        if (segment_ids[segment.pos()] != segment.id()) {
            return;
        }
    }

    // A layout produces the same observations regardless of its position or the
    // number of segments that realize it.
    std::ranges::sort(layouts);
    const auto duplicates = std::ranges::unique(layouts);
    layouts.erase(duplicates.begin(), duplicates.end());
    std::string input;
    input.reserve(layouts.size() * sha256_hex_size);
    for (auto layout : layouts) {
        input.append(layout);
    }
    auto* fingerprint = rowset.mutable_variant_desc_fingerprint();
    fingerprint->set_format_version(fingerprint_format_version);
    fingerprint->set_layout_sha256(sha256(input));
    fingerprint->set_segment_set_sha256(physical_segment_set_digest(rowset));
}

static Result<bool> read_rowset_desc_schemas(const RowsetSharedPtr& rs,
                                             std::vector<TabletSchemaSPtr>& schemas) {
    const auto& tablet_schema = rs->tablet_schema();
    bool complete = true;
    SegmentCacheHandle segment_cache;
    auto st = SegmentLoader::instance()->load_segments(std::static_pointer_cast<BetaRowset>(rs),
                                                       &segment_cache);
    if (!st.ok()) {
        return ResultError(std::move(st));
    }
    for (const auto& segment : segment_cache.get_segments()) {
        TabletSchemaSPtr schema = tablet_schema->copy_without_variant_extracted_columns();
        for (const auto& column : tablet_schema->columns()) {
            if (!column->is_variant_type()) {
                continue;
            }
            std::shared_ptr<segment_v2::VariantColumnReader> variant_column_reader;
            OlapReaderStatistics stats;
            StorageReadOptions read_options(stats);
            st = segment->get_variant_root_reader(*column, read_options, &variant_column_reader);
            if (st.is<ErrorCode::NOT_FOUND>()) {
                // Old segments predating an ALTER-added VARIANT have no physical subcolumn
                // metadata, so only newer segments contribute to the calculated schema.
                complete = false;
                continue;
            }
            if (!st.ok()) {
                LOG(WARNING) << "Failed to get VARIANT root reader for column: " << column->name()
                             << " error: " << st.to_string();
                // Extended-schema collection is best-effort per segment. Skip this column but
                // preserve metadata already collected from the remaining readable segments.
                complete = false;
                continue;
            }
            DORIS_CHECK(variant_column_reader != nullptr);
            // load external meta before getting subcolumn meta info
            st = variant_column_reader->load_external_meta_once();
            if (!st.ok()) {
                LOG(WARNING) << "Failed to load external meta for column: " << column->name()
                             << " error: " << st.to_string();
                complete = false;
                continue;
            }
            const auto* subcolumn_meta_info = variant_column_reader->get_subcolumns_meta_info();
            for (const auto& entry : *subcolumn_meta_info) {
                if (entry->path.empty()) {
                    continue;
                }
                const std::string& column_name =
                        column->name_lower_case() + "." + entry->path.get_path();
                const DataTypePtr& data_type = entry->data.file_column_type;
                PathInDataBuilder full_path_builder;
                auto full_path = full_path_builder.append(column->name_lower_case(), false)
                                         .append(entry->path.get_parts(), false)
                                         .build();
                TabletColumn subcolumn =
                        get_column_by_type(data_type, column_name,
                                           ExtraInfo {.unique_id = -1,
                                                      .parent_unique_id = column->unique_id(),
                                                      .path_info = full_path});
                schema->append_column(subcolumn);
            }
        }
        schemas.emplace_back(schema);
    }
    return complete;
}

TabletSchemaSPtr VariantCompactionUtil::calculate_variant_extended_schema(
        const std::vector<RowsetSharedPtr>& rowsets, const TabletSchemaSPtr& base_schema) {
    if (rowsets.empty()) {
        return nullptr;
    }

    std::vector<TabletSchemaSPtr> schemas;
    std::unordered_map<const TabletSchema*, std::unordered_set<std::string_view>> seen;
    for (const auto& rs : rowsets) {
        if (rs->num_segments() == 0) {
            continue;
        }
        const auto& tablet_schema = rs->tablet_schema();
        const auto digest = validated_rowset_digest(*rs->rowset_meta());
        if (!digest.empty() && seen[tablet_schema.get()].contains(digest)) {
            desc_rowsets_skipped << 1;
            continue;
        }

        desc_rowsets_read << 1;
        const auto complete = read_rowset_desc_schemas(rs, schemas);
        if (!complete.has_value()) {
            return base_schema;
        }
        if (complete.value() && !digest.empty()) {
            seen[tablet_schema.get()].insert(digest);
        }
    }
    TabletSchemaSPtr least_common_schema;
    auto st = get_least_common_schema(schemas, base_schema, least_common_schema, false);
    if (!st.ok()) {
        return base_schema;
    }
    return least_common_schema;
}

} // namespace doris::variant_util
