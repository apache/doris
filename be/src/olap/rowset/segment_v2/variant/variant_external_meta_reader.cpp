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

#include "olap/rowset/segment_v2/variant/variant_external_meta_reader.h"

#include <gen_cpp/segment_v2.pb.h>

#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

Status VariantExternalMetaReader::_find_key_meta(const SegmentFooterPB& footer, int32_t root_uid,
                                                 const MetadataPairPB** keys_meta_pair) const {
    *keys_meta_pair = nullptr;
    // prefer suffixed pairs
    std::string suffix = "." + std::to_string(root_uid);
    for (const auto& m : footer.file_meta_datas()) {
        if (m.key() == std::string("variant_meta_keys") + suffix) {
            *keys_meta_pair = &m;
            return Status::OK();
        }
    }
    // fallback: legacy single-variant footer
    if (!*keys_meta_pair) {
        for (const auto& m : footer.file_meta_datas()) {
            if (m.key() == "variant_meta_keys") {
                *keys_meta_pair = &m;
                return Status::OK();
            }
        }
    }
    return Status::OK();
}

Status VariantExternalMetaReader::init_from_footer(std::shared_ptr<const SegmentFooterPB> footer,
                                                   const io::FileReaderSPtr& file_reader,
                                                   int32_t root_uid) {
    _footer = std::move(footer);
    _file_reader = file_reader;

    const MetadataPairPB* keys_meta_pair = nullptr;
    RETURN_IF_ERROR(_find_key_meta(*_footer, root_uid, &keys_meta_pair));
    // If keys meta is absent, external meta is considered unavailable.
    if (!keys_meta_pair) {
        return Status::OK();
    }

    // Parse external Column Meta Region pointers and uid->col_id map.
    RETURN_IF_ERROR(ExternalColMetaUtil::parse_external_meta_pointers(*_footer, &_meta_ptrs));
    std::unordered_map<int32_t, size_t> uid2colid;
    RETURN_IF_ERROR(ExternalColMetaUtil::parse_uid_to_colid_map(*_footer, _meta_ptrs, &uid2colid));
    auto it = uid2colid.find(root_uid);
    if (it == uid2colid.end()) {
        return Status::Corruption("root uid {} not found in external uid->col_id map", root_uid);
    }
    _root_col_id = static_cast<uint32_t>(it->second);

    // Open the path-key indexed column.
    doris::segment_v2::IndexedColumnMetaPB key_meta;
    if (!key_meta.ParseFromArray(keys_meta_pair->value().data(),
                                 static_cast<int>(keys_meta_pair->value().size()))) {
        return Status::Corruption("bad variant_meta_keys meta");
    }

    _key_reader = std::make_unique<segment_v2::IndexedColumnReader>(file_reader, key_meta);
    RETURN_IF_ERROR(_key_reader->load(true, false));
    return Status::OK();
}

Status VariantExternalMetaReader::lookup_meta_by_path(const std::string& rel_path,
                                                      ColumnMetaPB* out_meta) const {
    if (!available()) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("no external variant meta");
    }
    // 1. Seek for Path
    auto key_it = segment_v2::IndexedColumnIterator(_key_reader.get());
    bool exact = false;
    Status st = key_it.seek_at_or_after(&rel_path, &exact);
    if (st.is<ErrorCode::ENTRY_NOT_FOUND>()) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("variant meta key not found");
    }
    if (!st.ok()) {
        return st;
    }

    // Record ordinal immediately after seek; get_current_ordinal() is only valid
    // while iterator is in "seeked" state (before calling next_batch()).
    auto ordinal = static_cast<uint32_t>(key_it.get_current_ordinal());

    // 2. Read key and verify exact match (keys store pure relative path).
    size_t n = 1;
    auto col = vectorized::ColumnString::create();
    vectorized::MutableColumnPtr dst = std::move(col);
    RETURN_IF_ERROR(key_it.next_batch(&n, dst));
    if (n != 1) {
        return Status::Corruption("variant meta key read failed");
    }
    auto* s = assert_cast<vectorized::ColumnString*>(dst.get());
    auto ref = s->get_data_at(0);

    if (ref.size != rel_path.size() || memcmp(ref.data, rel_path.data(), ref.size) != 0) {
        // seek_at_or_after found something greater, but path doesn't match
        return Status::Error<ErrorCode::NOT_FOUND, false>("variant meta key not found");
    }

    // 3. Derive column id from root_col_id and key ordinal, then read ColumnMetaPB.
    auto col_id = _root_col_id + 1 + ordinal;
    return ExternalColMetaUtil::read_col_meta(_file_reader, *_footer, _meta_ptrs, col_id, out_meta);
}

Status VariantExternalMetaReader::load_all(SubcolumnColumnMetaInfo* out_meta_tree,
                                           VariantStatistics* out_stats) {
    DCHECK(available());
    auto key_it = segment_v2::IndexedColumnIterator(_key_reader.get());
    RETURN_IF_ERROR(key_it.seek_to_ordinal(0));
    auto total = static_cast<size_t>(_key_reader->num_values());
    size_t built = 0;

    while (built < total) {
        size_t n = total - built;
        auto col = vectorized::ColumnString::create();
        vectorized::MutableColumnPtr dst = std::move(col);
        RETURN_IF_ERROR(key_it.next_batch(&n, dst));
        if (n == 0) {
            break;
        }
        for (size_t i = 0; i < n; ++i) {
            // Derive column id for this key by its global ordinal.
            auto ordinal = static_cast<uint32_t>(built + i);
            auto col_id = _root_col_id + 1 + ordinal;

            ColumnMetaPB meta;
            RETURN_IF_ERROR(ExternalColMetaUtil::read_col_meta(_file_reader, *_footer, _meta_ptrs,
                                                               col_id, &meta));

            if (!meta.has_column_path_info()) {
                continue;
            }
            vectorized::PathInData full_path;
            full_path.from_protobuf(meta.column_path_info());
            auto relative_path = full_path.copy_pop_front();
            if (relative_path.empty()) {
                continue; // skip root
            }
            if (out_meta_tree->find_leaf(relative_path)) {
                continue; // already exists
            }
            if (meta.has_none_null_size() && out_stats != nullptr) {
                out_stats->subcolumns_non_null_size.emplace(relative_path.get_path(),
                                                            meta.none_null_size());
            }
            auto file_type = vectorized::DataTypeFactory::instance().create_data_type(meta);
            out_meta_tree->add(relative_path,
                               SubcolumnMeta {.file_column_type = file_type, .footer_ordinal = -1});
        }
        built += n;
    }
    return Status::OK();
}

Status VariantExternalMetaReader::has_prefix(const std::string& prefix, bool* out) const {
    // english only in comments
    DCHECK(out != nullptr);
    DCHECK(available());
    *out = false;
    // Empty prefix means everything matches; guard for safety
    if (prefix.empty()) {
        *out = true;
        return Status::OK();
    }

    segment_v2::IndexedColumnIterator key_it(_key_reader.get());
    bool exact = false;
    Status st = key_it.seek_at_or_after(&prefix, &exact);
    if (st.is<ErrorCode::ENTRY_NOT_FOUND>()) {
        *out = false;
        return Status::OK();
    }
    RETURN_IF_ERROR(st);
    size_t n = 1;
    auto col = vectorized::ColumnString::create();
    vectorized::MutableColumnPtr dst = std::move(col);
    RETURN_IF_ERROR(key_it.next_batch(&n, dst));
    if (n == 0) {
        *out = false;
        return Status::OK();
    }
    auto* s = assert_cast<vectorized::ColumnString*>(dst.get());
    auto ref = s->get_data_at(0);
    std::string_view key_sv(ref.data, ref.size);

    // starts_with check. Key is Path + ID.
    // If prefix is just path part, it matches.
    // If prefix contains ID part (unlikely use case), it also matches.
    *out = key_sv.size() >= prefix.size() && key_sv.starts_with(prefix);
    return Status::OK();
}

Status VariantExternalMetaReader::load_all_once(SubcolumnColumnMetaInfo* out_meta_tree,
                                                VariantStatistics* out_stats) {
    DCHECK(available());
    return _load_once_call.call([&]() -> Status {
        if (_loaded) {
            return Status::OK();
        }
        RETURN_IF_ERROR(load_all(out_meta_tree, out_stats));
        _loaded = true;
        return Status::OK();
    });
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
