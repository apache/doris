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

#include <gen_cpp/AgentService_types.h>
#include <stddef.h>

#include <functional>
#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "storage/cache/page_cache.h"
#include "storage/segment/options.h"
#include "util/slice.h"

namespace doris {

enum class FieldType;
class TabletColumn;

namespace segment_v2 {

class PageBuilder;
class PageDecoder;
struct PageBuilderOptions;
struct PageDecoderOptions;
enum EncodingTypePB : int;

// For better performance, some encodings (like BitShuffle) need to be decoded before being added to the PageCache.
class DataPagePreDecoder {
public:
    virtual Status decode(std::unique_ptr<DataPage>* page, Slice* page_slice, size_t size_of_tail,
                          bool _use_cache, segment_v2::PageTypePB page_type,
                          const std::string& file_path, size_t size_of_prefix = 0) = 0;
    virtual ~DataPagePreDecoder() = default;
};

class EncodingInfo {
public:
    // Look up the EncodingInfo for an already-resolved (type, encoding) pair.
    // Read paths use this directly; write paths first resolve a default via the
    // get_*_default_encoding helpers below.
    static Status get(FieldType type, EncodingTypePB encoding_type, const EncodingInfo** encoding);

    // Default encoding for IndexedColumn writers (PK index, variant ext meta key writer)
    // that need fast value-seek via BinaryPrefixPage.
    static EncodingTypePB get_index_column_encoding(FieldType type);

    // Resolve the default encoding for one column when populating a fresh ColumnMetaPB.
    // All write paths (top-level segment writer, aux child writers for null bitmap /
    // array length / map length, struct subcolumns, variant subcolumns) should route the
    // encoding decision through this helper. Centralizing it here means future
    // per-storage-format or per-column encoding policy lives in one place.
    static EncodingTypePB resolve_default_encoding(TabletStorageFormatPB storage_format,
                                                   const TabletColumn& column);

    Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) const {
        return _create_builder_func(opts, builder);
    }
    Status create_page_builder(const PageBuilderOptions& opts,
                               std::unique_ptr<PageBuilder>& builder) const;
    Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                               PageDecoder** decoder) const {
        return _create_decoder_func(data, opts, decoder);
    }
    Status create_page_decoder(const Slice& data, const PageDecoderOptions& opts,
                               std::unique_ptr<PageDecoder>& decoder) const;
    FieldType type() const { return _type; }
    EncodingTypePB encoding() const { return _encoding; }

    DataPagePreDecoder* get_data_page_pre_decoder() const { return _data_page_pre_decoder.get(); }

private:
    friend class EncodingInfoResolver;
    friend class EncodingInfoTest;
    friend class ColumnReaderCacheTest;

    // Per-storage-format defaults are an internal lookup table. Production write paths
    // go through resolve_default_encoding(); other callers (e.g., zone map index) hardcode
    // the encoding they want. Tests use the friend declarations above to read these tables.
    static EncodingTypePB get_v2_default_encoding(FieldType type);
    static EncodingTypePB get_v3_default_encoding(FieldType type);

    template <typename TypeEncodingTraits>
    explicit EncodingInfo(TypeEncodingTraits traits);

    using CreateBuilderFunc = std::function<Status(const PageBuilderOptions&, PageBuilder**)>;
    CreateBuilderFunc _create_builder_func;

    using CreateDecoderFunc =
            std::function<Status(const Slice&, const PageDecoderOptions& opts, PageDecoder**)>;
    CreateDecoderFunc _create_decoder_func;

    FieldType _type;
    EncodingTypePB _encoding;
    std::unique_ptr<DataPagePreDecoder> _data_page_pre_decoder;
};

struct EncodingMapHash {
    size_t operator()(const FieldType& type) const { return int(type); }
    size_t operator()(const std::pair<FieldType, EncodingTypePB>& pair) const {
        return (int(pair.first) << 6) ^ pair.second;
    }
};

class EncodingInfoResolver {
public:
    EncodingInfoResolver();
    ~EncodingInfoResolver();

    EncodingTypePB get_v2_default_encoding(FieldType type) const;
    EncodingTypePB get_v3_default_encoding(FieldType type) const;
    EncodingTypePB get_index_column_encoding(FieldType type) const;

    Status get(FieldType data_type, EncodingTypePB encoding_type, const EncodingInfo** out);

private:
    // Registration helpers used by the constructor. Not thread-safe.
    //
    // _register_supported_encoding: declare that this (type, encoding) is a supported combination,
    //                               inserting one EncodingInfo* into _encoding_map. CHECK-fails on
    //                               duplicate registration of the same key.
    // _set_v2_default:              mark this (type, encoding) as the default for the V2 (V1/V2)
    //                               write path. Only writes _v2_default_map; the caller must have
    //                               already _register_supported_encoding'd the same key, otherwise
    //                               EncodingInfo::get will return InternalError when first asked
    //                               about this combination.
    // _set_v3_default:              same, for the V3 write path.
    // _set_index_column_encoding:   same, for IndexedColumn value-seek writers (PK index).
    template <FieldType type, EncodingTypePB encoding>
    void _register_supported_encoding();
    template <FieldType type, EncodingTypePB encoding>
    void _set_v2_default();
    template <FieldType type, EncodingTypePB encoding>
    void _set_v3_default();
    template <FieldType type, EncodingTypePB encoding>
    void _set_index_column_encoding();

    static EncodingTypePB _lookup(
            const std::unordered_map<FieldType, EncodingTypePB, EncodingMapHash>& m, FieldType t) {
        auto it = m.find(t);
        return it != m.end() ? it->second : UNKNOWN_ENCODING;
    }

    std::unordered_map<FieldType, EncodingTypePB, EncodingMapHash> _v2_default_map;
    std::unordered_map<FieldType, EncodingTypePB, EncodingMapHash> _v3_default_map;
    std::unordered_map<FieldType, EncodingTypePB, EncodingMapHash> _index_column_encoding_map;

    std::unordered_map<std::pair<FieldType, EncodingTypePB>, EncodingInfo*, EncodingMapHash>
            _encoding_map;
};

template <FieldType type, EncodingTypePB encoding, typename CppType, typename Enabled = void>
struct TypeEncodingTraits {};

template <FieldType field_type, EncodingTypePB encoding_type>
struct EncodingTraits : TypeEncodingTraits<field_type, encoding_type,
                                           typename CppTypeTraits<field_type>::CppType> {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    static const FieldType type = field_type;
    static const EncodingTypePB encoding = encoding_type;
};

template <FieldType type, EncodingTypePB encoding>
void EncodingInfoResolver::_register_supported_encoding() {
    auto key = std::make_pair(type, encoding);
    CHECK(_encoding_map.find(key) == _encoding_map.end())
            << "duplicate _register_supported_encoding for (type=" << int(type)
            << ", encoding=" << encoding << ")";
    EncodingTraits<type, encoding> traits;
    _encoding_map.emplace(key, new EncodingInfo(traits));
}

// The _set_*_default helpers only write to the corresponding default map. The caller must
// _register_supported_encoding the same (type, encoding) separately; if not, EncodingInfo::get
// will return InternalError when that combination is first looked up.

template <FieldType type, EncodingTypePB encoding>
void EncodingInfoResolver::_set_v2_default() {
    DCHECK(_v2_default_map.find(type) == _v2_default_map.end())
            << "duplicate v2 default for type " << int(type);
    _v2_default_map[type] = encoding;
}

template <FieldType type, EncodingTypePB encoding>
void EncodingInfoResolver::_set_v3_default() {
    DCHECK(_v3_default_map.find(type) == _v3_default_map.end())
            << "duplicate v3 default for type " << int(type);
    _v3_default_map[type] = encoding;
}

template <FieldType type, EncodingTypePB encoding>
void EncodingInfoResolver::_set_index_column_encoding() {
    DCHECK(_index_column_encoding_map.find(type) == _index_column_encoding_map.end())
            << "duplicate index_column_encoding for type " << int(type);
    _index_column_encoding_map[type] = encoding;
}

} // namespace segment_v2
} // namespace doris
