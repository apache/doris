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

#include "olap/rowset/segment_v2/variant/variant_ext_meta_writer.h"

#include <string>
#include <utility>
#include <vector>

#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "vec/common/variant_util.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

Status VariantExtMetaWriter::_ensure_inited(Writers* w) {
    if (w->inited) {
        return Status::OK();
    }

    // key writer: VARCHAR, value index ON, ordinal index ON
    IndexedColumnWriterOptions dict_opts;
    dict_opts.write_value_index = true;
    dict_opts.write_ordinal_index = true;
    dict_opts.encoding = PREFIX_ENCODING;
    dict_opts.compression = _comp;
    const TypeInfo* dict_type = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
    w->key_writer = std::make_unique<IndexedColumnWriter>(dict_opts, dict_type, _fw);
    RETURN_IF_ERROR(w->key_writer->init());

    w->inited = true;
    return Status::OK();
}

Status VariantExtMetaWriter::add(int32_t root_uid, const Slice& key) {
    auto& w = _writers_by_uid[root_uid];
    RETURN_IF_ERROR(_ensure_inited(&w));

    // Only store the relative path string as key. The concrete column_id in
    // Column Meta Region will be derived on the reader side using:
    //   col_id = root_column_id + 1 + key_ordinal
    // based on the agreed layout of ColumnMetaEntryPB in SegmentFooterPB.
    RETURN_IF_ERROR(w.key_writer->add(&key));
    ++w.count;
    return Status::OK();
}

Status VariantExtMetaWriter::flush_to_footer(SegmentFooterPB* footer) {
    for (auto& [uid, w] : _writers_by_uid) {
        if (!w.inited || w.count == 0) {
            continue;
        }
        doris::segment_v2::IndexedColumnMetaPB key_meta;
        RETURN_IF_ERROR(w.key_writer->finish(&key_meta));

        // keys
        std::string k = std::string("variant_meta_keys.") + std::to_string(uid);
        std::string v;
        key_meta.AppendToString(&v);
        auto* p1 = footer->add_file_meta_datas();
        p1->set_key(k);
        p1->set_value(v);
    }
    _writers_by_uid.clear();
    return Status::OK();
}

// Helper struct to manage column classification
struct ColumnClassification {
    std::vector<ColumnMetaPB> top_level_columns;
    std::unordered_map<int32_t, std::vector<std::pair<std::string, ColumnMetaPB>>> pending_subcols;
    std::unordered_map<int32_t, std::vector<ColumnMetaPB>> pending_sparse;
    std::unordered_map<int32_t, std::vector<ColumnMetaPB>> pending_doc_value;
    size_t externalized_count = 0;
};

// Separate function to classify columns
static void classify_columns(const SegmentFooterPB& footer, ColumnClassification& classification) {
    classification.top_level_columns.reserve(footer.columns_size());

    for (int i = 0; i < footer.columns_size(); ++i) {
        const ColumnMetaPB& col = footer.columns(i);
        bool is_subcol = false;
        if (col.has_column_path_info()) {
            vectorized::PathInData full_path;
            full_path.from_protobuf(col.column_path_info());
            vectorized::PathInData rel = full_path.copy_pop_front();
            if (!rel.empty()) {
                is_subcol = true;
                std::string rel_path = rel.get_path();
                int32_t root_uid = col.column_path_info().parrent_column_unique_id();

                if (rel_path.find("__DORIS_VARIANT_SPARSE__") != std::string::npos) {
                    classification.pending_sparse[root_uid].emplace_back(col);
                } else if (rel_path.find("__DORIS_VARIANT_DOC_VALUE__") != std::string::npos) {
                    classification.pending_doc_value[root_uid].emplace_back(col);
                } else {
                    classification.pending_subcols[root_uid].emplace_back(rel_path, col);
                    classification.externalized_count++;
                }
            }
        }

        if (!is_subcol) {
            classification.top_level_columns.emplace_back(col);
        }
    }
}

Status VariantExtMetaWriter::externalize_from_footer(SegmentFooterPB* footer,
                                                     std::vector<ColumnMetaPB>* out_metas) {
    // Variant meta pre-processing and reorganization:
    // 1. Identify Variant Root columns and their subcolumns.
    // 2. Collect sparse subcolumns to embed in Root.
    // 3. Collect non-sparse subcolumns to externalize to Meta Region.
    // 4. Rebuild the column list (out_metas) ensuring subcolumns follow their Root.
    // 5. Record mapping (Path -> Index in out_metas) for externalized subcolumns.

    ColumnClassification classification;
    classify_columns(*footer, classification);

    // Rebuild out_metas
    out_metas->clear();
    out_metas->reserve(footer->columns_size()); // Rough estimate

    for (auto& col : classification.top_level_columns) {
        // Add Top Level Column
        out_metas->emplace_back(col);
        // Keep it in footer columns as well
        ColumnMetaPB* current_root_meta = &out_metas->back();

        // Check if it is a Variant Root
        if (col.type() == int(FieldType::OLAP_FIELD_TYPE_VARIANT)) {
            int32_t root_uid = col.unique_id();

            // 1. Embed sparse subcolumns
            if (auto it = classification.pending_sparse.find(root_uid);
                it != classification.pending_sparse.end()) {
                for (const auto& sparse_meta : it->second) {
                    current_root_meta->add_children_columns()->CopyFrom(sparse_meta);
                }
            }

            // 2. Embed doc snapshot subcolumns
            if (auto it = classification.pending_doc_value.find(root_uid);
                it != classification.pending_doc_value.end()) {
                for (const auto& doc_value_meta : it->second) {
                    current_root_meta->add_children_columns()->CopyFrom(doc_value_meta);
                }
            }

            // 3. Append non-sparse subcolumns to out_metas and record path-only keys
            if (auto it = classification.pending_subcols.find(root_uid);
                it != classification.pending_subcols.end()) {
                // Sort by path for consistent index order
                std::sort(it->second.begin(), it->second.end(),
                          [](const auto& a, const auto& b) { return a.first < b.first; });

                for (auto& [path, meta] : it->second) {
                    // Append to out_metas
                    out_metas->emplace_back(meta);
                    // Mark as subcolumn placeholder for footer logic (unique_id = -1)
                    out_metas->back().set_unique_id(-1);

                    // Record path key for this subcolumn. Column id will be derived
                    // by reader as (root_col_id + 1 + ordinal).
                    RETURN_IF_ERROR(add(root_uid, Slice(path)));
                }
            }
        }
    }

    // Write the index to footer
    RETURN_IF_ERROR(flush_to_footer(footer));

    footer->clear_columns();

    VLOG_DEBUG << "VariantExtMetaWriter::externalize_from_footer, externalized subcolumns: "
               << classification.externalized_count
               << ", total meta entries: " << out_metas->size();
    return Status::OK();
}

} // namespace doris::segment_v2
