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

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

Status VariantExternalMetaReader::_find_pairs(const SegmentFooterPB& footer, int32_t root_uid,
                                              const MetadataPairPB** keys_meta_pair,
                                              const MetadataPairPB** vals_meta_pair) const {
    *keys_meta_pair = nullptr;
    *vals_meta_pair = nullptr;
    // prefer suffixed pairs
    std::string suffix = "." + std::to_string(root_uid);
    for (const auto& m : footer.file_meta_datas()) {
        if (m.key() == std::string("variant_meta_keys") + suffix) {
            *keys_meta_pair = &m;
        }
        if (m.key() == std::string("variant_meta_values") + suffix) {
            *vals_meta_pair = &m;
        }
    }
    // fallback: legacy single-variant footer
    if (!*keys_meta_pair || !*vals_meta_pair) {
        for (const auto& m : footer.file_meta_datas()) {
            if (!*keys_meta_pair && m.key() == "variant_meta_keys") {
                *keys_meta_pair = &m;
            }
            if (!*vals_meta_pair && m.key() == "variant_meta_values") {
                *vals_meta_pair = &m;
            }
        }
    }
    return Status::OK();
}

Status VariantExternalMetaReader::init_from_footer(const SegmentFooterPB& footer,
                                                   const io::FileReaderSPtr& file_reader,
                                                   int32_t root_uid) {
    const MetadataPairPB* keys_meta_pair = nullptr;
    const MetadataPairPB* vals_meta_pair = nullptr;
    RETURN_IF_ERROR(_find_pairs(footer, root_uid, &keys_meta_pair, &vals_meta_pair));
    if (!keys_meta_pair || !vals_meta_pair) {
        // External meta not present, keep unavailable state.
        return Status::OK();
    }

    doris::segment_v2::IndexedColumnMetaPB key_meta;
    doris::segment_v2::IndexedColumnMetaPB val_meta;
    if (!key_meta.ParseFromArray(keys_meta_pair->value().data(),
                                 static_cast<int>(keys_meta_pair->value().size()))) {
        return Status::Corruption("bad variant_meta_keys meta");
    }
    if (!val_meta.ParseFromArray(vals_meta_pair->value().data(),
                                 static_cast<int>(vals_meta_pair->value().size()))) {
        return Status::Corruption("bad variant_meta_values meta");
    }

    _key_reader = std::make_unique<segment_v2::IndexedColumnReader>(file_reader, key_meta);
    _val_reader = std::make_unique<segment_v2::IndexedColumnReader>(file_reader, val_meta);
    RETURN_IF_ERROR(_key_reader->load(true, false));
    RETURN_IF_ERROR(_val_reader->load(true, false));
    return Status::OK();
}

Status VariantExternalMetaReader::lookup_meta_by_path(const std::string& rel_path,
                                                      ColumnMetaPB* out_meta) const {
    if (!available()) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("no external variant meta");
    }
    segment_v2::IndexedColumnIterator key_it(_key_reader.get());
    bool exact = false;
    Status st = key_it.seek_at_or_after(&rel_path, &exact);
    if (st.is<ErrorCode::ENTRY_NOT_FOUND>()) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("variant meta key not found");
    }
    if (!exact) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("variant meta key not found");
    }
    if (!st.ok()) {
        return st;
    }
    auto ord = key_it.get_current_ordinal();
    segment_v2::IndexedColumnIterator val_it(_val_reader.get());
    RETURN_IF_ERROR(val_it.seek_to_ordinal(ord));
    size_t n = 1;
    auto col = vectorized::ColumnString::create();
    vectorized::MutableColumnPtr dst = std::move(col);
    RETURN_IF_ERROR(val_it.next_batch(&n, dst));
    if (n != 1) {
        return Status::Corruption("variant meta value read failed");
    }
    auto* s = assert_cast<vectorized::ColumnString*>(dst.get());
    auto ref = s->get_data_at(0);
    if (!out_meta->ParseFromArray(ref.data, static_cast<int>(ref.size))) {
        return Status::Corruption("bad ColumnMetaPB in variant external meta");
    }
    return Status::OK();
}

Status VariantExternalMetaReader::load_all(SubcolumnColumnMetaInfo* out_meta_tree,
                                           VariantStatistics* out_stats) {
    segment_v2::IndexedColumnIterator val_it(_val_reader.get());
    RETURN_IF_ERROR(val_it.seek_to_ordinal(0));
    auto total = static_cast<size_t>(_val_reader->num_values());
    size_t built = 0;
    while (built < total) {
        size_t n = total - built;
        auto col = vectorized::ColumnString::create();
        vectorized::MutableColumnPtr dst = std::move(col);
        RETURN_IF_ERROR(val_it.next_batch(&n, dst));
        if (n == 0) {
            break;
        }
        auto* s = assert_cast<vectorized::ColumnString*>(dst.get());
        for (size_t i = 0; i < n; ++i) {
            auto ref = s->get_data_at(i);
            ColumnMetaPB meta;
            if (!meta.ParseFromArray(ref.data, static_cast<int>(ref.size))) {
                return Status::Corruption("bad ColumnMetaPB in variant external meta");
            }
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
        if (built < total) {
            RETURN_IF_ERROR(val_it.seek_to_ordinal(static_cast<ordinal_t>(built)));
        }
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
    if (!st.ok()) {
        return st;
    }

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
    // starts_with check
    *out = key_sv.size() >= prefix.size() && key_sv.starts_with(prefix);
    return Status::OK();
}

Status VariantExternalMetaReader::load_all_once(SubcolumnColumnMetaInfo* out_meta_tree,
                                                VariantStatistics* out_stats) {
    if (!available()) {
        return Status::OK();
    }
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
