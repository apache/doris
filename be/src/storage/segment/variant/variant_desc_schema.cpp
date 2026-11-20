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

#include "storage/segment/variant/variant_desc_schema.h"

#include <bvar/bvar.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>

#include <map>
#include <set>
#include <string_view>
#include <utility>

#include "core/data_type/data_type_factory.hpp"
#include "exec/common/variant_util.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_cache.h"
#include "util/sha.h"
#include "util/url_coding.h"

namespace doris::variant_util {

static bvar::Adder<int64_t> desc_summary_rowsets("variant_desc_summary_rowsets");
static bvar::Adder<int64_t> desc_fallback_rowsets("variant_desc_fallback_rowsets");

static TabletColumn desc_column(const TabletColumn& root, const PathInData& relative_path,
                                const DataTypePtr& type) {
    PathInDataBuilder path;
    path.append(root.name_lower_case(), false).append(relative_path.get_parts(), false);
    return get_column_by_type(type, root.name_lower_case() + "." + relative_path.get_path(),
                              ExtraInfo {.unique_id = -1,
                                         .parent_unique_id = root.unique_id(),
                                         .path_info = path.build()});
}

using DescColumns = std::map<std::string, ColumnPB>;

static void add_desc_column(DescColumns& columns, ColumnPB column) {
    auto key = TabletSchema::deterministic_string_serialize(column);
    columns.try_emplace(std::move(key), std::move(column));
}

static std::optional<std::string> serialize_desc_columns(DescColumns columns) {
    VariantDescSummaryPB summary;
    for (auto& [key, column] : columns) {
        summary.add_columns()->Swap(&column);
    }
    // Keep the optional summary at most 64 KiB after encoding. Wide schemas
    // can still use the segment reader and its bounded in-memory cache.
    constexpr size_t max_summary_bytes = 48 * 1024;
    if (summary.ByteSizeLong() > max_summary_bytes) {
        return std::nullopt;
    }
    // The tablet metadata endpoint exposes this field in JSON.
    std::string encoded;
    base64_encode(TabletSchema::deterministic_string_serialize(summary), &encoded);
    return encoded;
}

static bool parse_desc_columns(const std::string& encoded, VariantDescSummaryPB* summary) {
    std::string decoded;
    return base64_decode(encoded, &decoded) && summary->ParseFromString(decoded);
}

std::optional<std::string> build_segment_desc_summary(const segment_v2::SegmentFooterPB& footer,
                                                      const TabletSchema& schema) {
    if (schema.num_variant_columns() == 0) {
        return std::nullopt;
    }
    const bool external =
            schema.storage_format() == TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3;
    DescColumns columns;
    std::set<std::pair<int32_t, std::string>> paths;
    for (const auto& column : footer.columns()) {
        if (!column.has_column_path_info()) {
            continue;
        }
        PathInData full_path;
        full_path.from_protobuf(column.column_path_info());
        auto path = full_path.copy_pop_front();
        if (path.empty()) {
            continue;
        }
        auto path_text = path.get_path();
        const bool sparse = external ? path_text.find(SPARSE_COLUMN_PATH) != std::string::npos
                                     : path_text == SPARSE_COLUMN_PATH ||
                                               path_text.starts_with(SPARSE_COLUMN_PATH + ".b");
        if (sparse || path_text.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) {
            continue;
        }
        // The legacy reader applies layout-specific rules to nested groups and
        // colliding paths. Leave those layouts to the reader until they can be
        // represented by a summary with the same semantics.
        if (segment_v2::contains_nested_group_marker(path_text) || path.has_nested_part() ||
            column.type() == static_cast<int>(FieldType::OLAP_FIELD_TYPE_VARIANT)) {
            return std::nullopt;
        }
        auto parent_uid = column.column_path_info().parrent_column_unique_id();
        const auto parent_index = schema.field_index(parent_uid);
        if (parent_index < 0 || !schema.column(parent_index).is_variant_type()) {
            return std::nullopt;
        }
        if (!paths.emplace(parent_uid, path_text).second) {
            return std::nullopt;
        }
        if (!external && !schema.variant_escape_dots_in_keys() && !path.get_is_typed()) {
            path = PathInData(path_text);
        }
        const auto& root = schema.column(parent_index);
        auto observation =
                desc_column(root, path, DataTypeFactory::instance().create_data_type(column));
        ColumnPB pb;
        observation.to_schema_pb(&pb);
        add_desc_column(columns, std::move(pb));
    }
    for (const auto& [uid, path] : paths) {
        // A tree can resolve a leaf and one of its descendants differently
        // depending on insertion order. A summary is deliberately unordered.
        auto child = paths.lower_bound({uid, path + "."});
        if (child != paths.end() && child->first == uid && child->second.starts_with(path + ".")) {
            return std::nullopt;
        }
    }
    return serialize_desc_columns(std::move(columns));
}

std::optional<std::string> merge_desc_summaries(const std::vector<std::string>& summaries) {
    DescColumns columns;
    std::set<std::string_view> distinct(summaries.begin(), summaries.end());
    if (distinct.size() == 1) {
        return std::string(*distinct.begin());
    }
    for (auto summary : distinct) {
        VariantDescSummaryPB pb;
        if (!parse_desc_columns(std::string(summary), &pb)) {
            return std::nullopt;
        }
        for (auto& column : *pb.mutable_columns()) {
            add_desc_column(columns, std::move(column));
        }
    }
    return serialize_desc_columns(std::move(columns));
}

static bool read_desc_segments(const RowsetSharedPtr& rs, TabletSchema& schema, bool& complete) {
    complete = true;
    const auto& tablet_schema = rs->tablet_schema();
    SegmentCacheHandle segment_cache;
    auto st = SegmentLoader::instance()->load_segments(std::static_pointer_cast<BetaRowset>(rs),
                                                       &segment_cache);
    if (!st.ok()) {
        return false;
    }
    for (const auto& segment : segment_cache.get_segments()) {
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
                schema.append_column(
                        desc_column(*column, entry->path, entry->data.file_column_type));
            }
        }
    }
    return true;
}

static bool append_persisted_desc_summary(const Rowset& rowset, TabletSchema& output) {
    VariantDescSummaryPB summary;
    if (!parse_desc_columns(rowset.rowset_meta()->variant_desc_summary(), &summary)) {
        return false;
    }
    const auto& schema = *rowset.tablet_schema();
    for (const auto& pb : summary.columns()) {
        TabletColumn column(pb);
        const auto index = schema.field_index(column.parent_unique_id());
        if (index < 0 || !schema.column(index).is_variant_type() || !column.has_path_info()) {
            return false;
        }
        auto relative = column.path_info_ptr()->copy_pop_front();
        const auto& root = schema.column(index);
        PathInDataBuilder path;
        path.append(root.name_lower_case(), false).append(relative.get_parts(), false);
        column.set_path_info(path.build());
        column.set_name(root.name_lower_case() + "." + relative.get_path());
        output.append_column(std::move(column));
    }
    return true;
}

static std::string desc_cache_key(std::string_view prefix, std::string_view input) {
    SHA256Digest digest;
    digest.reset(input.data(), input.size());
    std::string key(prefix);
    key.append(digest.digest());
    return key;
}

static TabletSchemaSPtr find_desc_schema(const std::string& key) {
    auto* cache = TabletSchemaCache::instance();
    auto [handle, schema] = cache->lookup_schema(key);
    if (handle != nullptr) {
        cache->release(handle);
    }
    return schema;
}

static TabletSchemaSPtr cache_desc_schema(const std::string& key, TabletSchemaSPtr schema) {
    auto* cache = TabletSchemaCache::instance();
    auto [handle, result] = cache->insert(key, std::move(schema));
    cache->release(handle);
    return result;
}

static std::string legacy_cache_key(const Rowset& rowset, std::string_view schema_key) {
    std::string input = rowset.rowset_id().to_string();
    input.push_back(':');
    for (auto segment : rowset.rowset_meta()->segments()) {
        input.append(std::to_string(segment.id())).push_back(':');
    }
    input.append(schema_key);
    return desc_cache_key("VariantDescRowset:", input);
}

TabletSchemaSPtr calculate_variant_extended_schema(const std::vector<RowsetSharedPtr>& rowsets,
                                                   const TabletSchemaSPtr& base_schema) {
    if (rowsets.empty()) {
        return nullptr;
    }

    std::vector<TabletSchemaSPtr> schemas;
    std::map<const TabletSchema*, std::set<std::string_view>> seen_summaries;
    bool complete_summaries = true;
    std::map<const TabletSchema*, std::string> schema_keys;
    const auto schema_key = [&](const TabletSchema* schema) -> const std::string& {
        auto [it, inserted] = schema_keys.try_emplace(schema);
        if (inserted) {
            it->second = schema->to_key();
        }
        return it->second;
    };
    for (const auto& rs : rowsets) {
        if (rs->num_segments() == 0) {
            continue;
        }

        if (rs->rowset_meta()->has_variant_desc_summary()) {
            auto& seen = seen_summaries[rs->tablet_schema().get()];
            const auto& summary = rs->rowset_meta()->variant_desc_summary();
            if (seen.contains(summary)) {
                desc_summary_rowsets << 1;
                continue;
            }
            auto observations = std::make_shared<TabletSchema>();
            if (append_persisted_desc_summary(*rs, *observations)) {
                seen.insert(summary);
                desc_summary_rowsets << 1;
                schemas.emplace_back(std::move(observations));
                continue;
            }
        }

        complete_summaries = false;
        const auto legacy_key = legacy_cache_key(*rs, schema_key(rs->tablet_schema().get()));
        auto observations = find_desc_schema(legacy_key);
        if (!observations) {
            desc_fallback_rowsets << 1;
            observations = rs->tablet_schema()->copy_without_variant_extracted_columns();
            bool complete;
            if (!read_desc_segments(rs, *observations, complete)) {
                return base_schema;
            }
            if (complete) {
                observations = cache_desc_schema(legacy_key, std::move(observations));
            }
        }
        schemas.emplace_back(std::move(observations));
    }
    std::string cache_key;
    if (complete_summaries && !seen_summaries.empty()) {
        std::map<std::string, std::set<std::string_view>> ordered;
        for (const auto& [schema, summaries] : seen_summaries) {
            auto& same_schema = ordered[schema_key(schema)];
            same_schema.insert(summaries.begin(), summaries.end());
        }
        std::string key_input;
        const auto append = [&](std::string_view value) {
            key_input.append(std::to_string(value.size())).push_back(':');
            key_input.append(value);
        };
        append(base_schema->to_key());
        for (const auto& [schema, summaries] : ordered) {
            append(schema);
            append(std::to_string(summaries.size()));
            for (auto summary : summaries) {
                append(summary);
            }
        }
        cache_key = desc_cache_key("VariantDesc:", key_input);
        if (auto schema = find_desc_schema(cache_key)) {
            return schema;
        }
    }
    TabletSchemaSPtr merged_schema;
    auto status = get_least_common_schema(schemas, base_schema, merged_schema, false);
    if (!status.ok()) {
        return base_schema;
    }
    if (!cache_key.empty()) {
        return cache_desc_schema(cache_key, std::move(merged_schema));
    }
    return merged_schema;
}

} // namespace doris::variant_util
