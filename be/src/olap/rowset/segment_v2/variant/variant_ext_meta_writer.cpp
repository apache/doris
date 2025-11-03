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

#include <utility>

#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "vec/common/schema_util.h"

namespace doris::segment_v2 {

Status VariantExtMetaWriter::_ensure_inited(Writers* w) {
    if (w->inited) {
        return Status::OK();
    }

    // key writer: VARCHAR, value index ON, ordinal index OFF
    IndexedColumnWriterOptions dict_opts;
    dict_opts.write_value_index = true;
    dict_opts.write_ordinal_index = false;
    dict_opts.encoding = PREFIX_ENCODING;
    dict_opts.compression = _comp;
    const TypeInfo* dict_type = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
    w->key_writer = std::make_unique<IndexedColumnWriter>(dict_opts, dict_type, _fw);
    RETURN_IF_ERROR(w->key_writer->init());

    // value writer: VARCHAR, value index OFF, ordinal index ON
    IndexedColumnWriterOptions vals_opts;
    vals_opts.write_value_index = false;
    vals_opts.write_ordinal_index = true;
    vals_opts.encoding = PLAIN_ENCODING;
    vals_opts.compression = _comp;
    const TypeInfo* vals_type = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
    w->val_writer = std::make_unique<IndexedColumnWriter>(vals_opts, vals_type, _fw);
    RETURN_IF_ERROR(w->val_writer->init());

    w->inited = true;
    return Status::OK();
}

Status VariantExtMetaWriter::add(int32_t root_uid, const Slice& key, const Slice& val) {
    auto& w = _writers_by_uid[root_uid];
    RETURN_IF_ERROR(_ensure_inited(&w));
    RETURN_IF_ERROR(w.key_writer->add(&key));
    RETURN_IF_ERROR(w.val_writer->add(&val));
    ++w.count;
    return Status::OK();
}

Status VariantExtMetaWriter::flush_to_footer(SegmentFooterPB* footer) {
    for (auto& [uid, w] : _writers_by_uid) {
        if (!w.inited || w.count == 0) {
            continue;
        }
        doris::segment_v2::IndexedColumnMetaPB key_meta;
        doris::segment_v2::IndexedColumnMetaPB val_meta;
        RETURN_IF_ERROR(w.key_writer->finish(&key_meta));
        RETURN_IF_ERROR(w.val_writer->finish(&val_meta));

        // keys
        std::string k = std::string("variant_meta_keys.") + std::to_string(uid);
        std::string v;
        key_meta.AppendToString(&v);
        auto* p1 = footer->add_file_meta_datas();
        p1->set_key(k);
        p1->set_value(v);

        // values
        std::string k2 = std::string("variant_meta_values.") + std::to_string(uid);
        std::string v2;
        val_meta.AppendToString(&v2);
        auto* p2 = footer->add_file_meta_datas();
        p2->set_key(k2);
        p2->set_value(v2);
    }
    _writers_by_uid.clear();
    return Status::OK();
}

Status VariantExtMetaWriter::externalize_from_footer(SegmentFooterPB* footer) {
    // Collect variant subcolumns first, then write in sorted order to keep stability.
    std::vector<ColumnMetaPB> kept;
    kept.reserve(footer->columns_size());
    std::unordered_map<int32_t, std::vector<std::pair<std::string, std::string>>>
            pending; // uid -> [(path, meta_bytes)]
    pending.reserve(8);
    size_t kept_count = 0;
    size_t externalized_count = 0;

    std::string meta_bytes;
    for (int i = 0; i < footer->columns_size(); ++i) {
        const ColumnMetaPB& col = footer->columns(i);
        if (!col.has_column_path_info()) {
            kept.emplace_back(col);
            kept_count++;
            continue;
        }
        vectorized::PathInData full_path;
        full_path.from_protobuf(col.column_path_info());
        vectorized::PathInData rel = full_path.copy_pop_front();
        if (rel.empty()) {
            kept.emplace_back(col);
            kept_count++;
            continue; // root variant column
        }
        std::string rel_path = rel.get_path();
        // Check if this is a sparse column or sub column
        // Treat both single sparse column and bucketized sparse columns (.b{i}) as sparse
        if (rel_path.find("__DORIS_VARIANT_SPARSE__") != std::string::npos) {
            kept.emplace_back(col);
            kept_count++;
            continue;
        }
        int32_t root_uid = col.column_path_info().parrent_column_unique_id();
        meta_bytes.clear();
        col.AppendToString(&meta_bytes);
        pending[root_uid].emplace_back(std::move(rel_path), meta_bytes);
        externalized_count++;
    }

    // Write keys/values per uid in sorted path order
    for (auto& [uid, vec] : pending) {
        std::sort(vec.begin(), vec.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });
        for (auto& kv : vec) {
            RETURN_IF_ERROR(add(uid, Slice(kv.first), Slice(kv.second)));
        }
    }
    RETURN_IF_ERROR(flush_to_footer(footer));

    // Replace columns with kept ones (prune externalized subcolumns)
    footer->clear_columns();
    for (const auto& c : kept) {
        auto* dst = footer->add_columns();
        dst->CopyFrom(c);
    }
    VLOG_DEBUG << "VariantExtMetaWriter::externalize_from_footer, externalized subcolumns: "
               << externalized_count << ", kept columns: " << kept_count
               << ", total columns: " << footer->columns_size();
    return Status::OK();
}

} // namespace doris::segment_v2
