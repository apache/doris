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

#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"

#include <gen_cpp/segment_v2.pb.h>
#include <string.h>

#include <algorithm>
#include <set>
#include <string>
#include <utility>

#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bloom_filter.h" // for BloomFilterOptions, BloomFilter
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "runtime/decimalv2_value.h"
#include "util/slice.h"
#include "util/types.h"

namespace doris {
namespace segment_v2 {

namespace {

template <typename CppType>
struct BloomFilterTraits {
    using ValueDict = std::set<CppType>;
};

template <>
struct BloomFilterTraits<Slice> {
    using ValueDict = std::set<Slice, Slice::Comparator>;
};

struct Int128Comparator {
    bool operator()(const int128_t& a, const int128_t& b) const { return a < b; }
};

template <>
struct BloomFilterTraits<int128_t> {
    using ValueDict = std::set<int128_t, Int128Comparator>;
};

// Builder for bloom filter. In doris, bloom filter index is used in
// high cardinality key columns and none-agg value columns for high selectivity and storage
// efficiency.
// This builder builds a bloom filter page by every data page, with a page id index.
// Meanwhile, It adds an ordinal index to load bloom filter index according to requirement.
//
template <FieldType field_type>
class BloomFilterIndexWriterImpl : public BloomFilterIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using ValueDict = typename BloomFilterTraits<CppType>::ValueDict;

    explicit BloomFilterIndexWriterImpl(const BloomFilterOptions& bf_options,
                                        const TypeInfo* type_info)
            : _bf_options(bf_options),
              _type_info(type_info),
              _has_null(false),
              _bf_buffer_size(0) {}

    ~BloomFilterIndexWriterImpl() override = default;

    void add_values(const void* values, size_t count) override {
        const CppType* v = (const CppType*)values;
        for (int i = 0; i < count; ++i) {
            if (_values.find(*v) == _values.end()) {
                if constexpr (_is_slice_type()) {
                    CppType new_value;
                    _type_info->deep_copy(&new_value, v, &_arena);
                    _values.insert(new_value);
                } else if constexpr (_is_int128()) {
                    int128_t new_value;
                    memcpy(&new_value, v, sizeof(PackedInt128));
                    _values.insert(new_value);
                } else {
                    _values.insert(*v);
                }
            }
            ++v;
        }
    }

    void add_nulls(uint32_t count) override { _has_null = true; }

    Status flush() override {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
        RETURN_IF_ERROR(bf->init(_values.size(), _bf_options.fpp, _bf_options.strategy));
        bf->set_has_null(_has_null);
        for (auto& v : _values) {
            if constexpr (_is_slice_type()) {
                Slice* s = (Slice*)&v;
                bf->add_bytes(s->data, s->size);
            } else {
                bf->add_bytes((char*)&v, sizeof(CppType));
            }
        }
        _bf_buffer_size += bf->size();
        _bfs.push_back(std::move(bf));
        _values.clear();
        _has_null = false;
        return Status::OK();
    }

    Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) override {
        if (_values.size() > 0) {
            RETURN_IF_ERROR(flush());
        }
        index_meta->set_type(BLOOM_FILTER_INDEX);
        BloomFilterIndexPB* meta = index_meta->mutable_bloom_filter_index();
        meta->set_hash_strategy(_bf_options.strategy);
        meta->set_algorithm(BLOCK_BLOOM_FILTER);

        // write bloom filters
        const auto* bf_type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = false;
        options.encoding = PLAIN_ENCODING;
        IndexedColumnWriter bf_writer(options, bf_type_info, file_writer);
        RETURN_IF_ERROR(bf_writer.init());
        for (auto& bf : _bfs) {
            Slice data(bf->data(), bf->size());
            static_cast<void>(bf_writer.add(&data));
        }
        RETURN_IF_ERROR(bf_writer.finish(meta->mutable_bloom_filter()));
        return Status::OK();
    }

    uint64_t size() override {
        uint64_t total_size = _bf_buffer_size;
        total_size += _arena.used_size();
        return total_size;
    }

private:
    // supported slice types are: FieldType::OLAP_FIELD_TYPE_CHAR|FieldType::OLAP_FIELD_TYPE_VARCHAR
    static constexpr bool _is_slice_type() {
        return field_type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
               field_type == FieldType::OLAP_FIELD_TYPE_CHAR ||
               field_type == FieldType::OLAP_FIELD_TYPE_STRING;
    }

    static constexpr bool _is_int128() { return field_type == FieldType::OLAP_FIELD_TYPE_LARGEINT; }

private:
    BloomFilterOptions _bf_options;
    const TypeInfo* _type_info;
    vectorized::Arena _arena;
    bool _has_null;
    uint64_t _bf_buffer_size;
    // distinct values
    ValueDict _values;
    std::vector<std::unique_ptr<BloomFilter>> _bfs;
};

} // namespace

void PrimaryKeyBloomFilterIndexWriterImpl::add_values(const void* values, size_t count) {
    const Slice* v = (const Slice*)values;
    for (int i = 0; i < count; ++i) {
        Slice new_value;
        _type_info->deep_copy(&new_value, v, &_arena);
        _values.push_back(new_value);
        ++v;
    }
}

Status PrimaryKeyBloomFilterIndexWriterImpl::flush() {
    std::unique_ptr<BloomFilter> bf;
    RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
    RETURN_IF_ERROR(bf->init(_values.size(), _bf_options.fpp, _bf_options.strategy));
    bf->set_has_null(_has_null);
    for (auto& v : _values) {
        Slice* s = (Slice*)&v;
        bf->add_bytes(s->data, s->size);
    }
    _bf_buffer_size += bf->size();
    _bfs.push_back(std::move(bf));
    _values.clear();
    _has_null = false;
    return Status::OK();
}

Status PrimaryKeyBloomFilterIndexWriterImpl::finish(io::FileWriter* file_writer,
                                                    ColumnIndexMetaPB* index_meta) {
    if (_values.size() > 0) {
        RETURN_IF_ERROR(flush());
    }
    index_meta->set_type(BLOOM_FILTER_INDEX);
    BloomFilterIndexPB* meta = index_meta->mutable_bloom_filter_index();
    meta->set_hash_strategy(_bf_options.strategy);
    meta->set_algorithm(BLOCK_BLOOM_FILTER);

    // write bloom filters
    const auto* bf_type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
    IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = false;
    options.encoding = PLAIN_ENCODING;
    IndexedColumnWriter bf_writer(options, bf_type_info, file_writer);
    RETURN_IF_ERROR(bf_writer.init());
    for (auto& bf : _bfs) {
        Slice data(bf->data(), bf->size());
        static_cast<void>(bf_writer.add(&data));
    }
    RETURN_IF_ERROR(bf_writer.finish(meta->mutable_bloom_filter()));
    return Status::OK();
}

uint64_t PrimaryKeyBloomFilterIndexWriterImpl::size() {
    uint64_t total_size = _bf_buffer_size;
    total_size += _arena.used_size();
    return total_size;
}

NGramBloomFilterIndexWriterImpl::NGramBloomFilterIndexWriterImpl(
        const BloomFilterOptions& bf_options, uint8_t gram_size, uint16_t bf_size)
        : _bf_options(bf_options),
          _gram_size(gram_size),
          _bf_size(bf_size),
          _bf_buffer_size(0),
          _token_extractor(gram_size) {
    static_cast<void>(BloomFilter::create(NGRAM_BLOOM_FILTER, &_bf, bf_size));
}

void NGramBloomFilterIndexWriterImpl::add_values(const void* values, size_t count) {
    const Slice* src = reinterpret_cast<const Slice*>(values);
    for (int i = 0; i < count; ++i, ++src) {
        if (src->size < _gram_size) {
            continue;
        }
        _token_extractor.string_to_bloom_filter(src->data, src->size, *_bf);
    }
}

Status NGramBloomFilterIndexWriterImpl::flush() {
    _bf_buffer_size += _bf->size();
    _bfs.emplace_back(std::move(_bf));
    // init new one
    RETURN_IF_ERROR(BloomFilter::create(NGRAM_BLOOM_FILTER, &_bf, _bf_size));
    return Status::OK();
}

Status NGramBloomFilterIndexWriterImpl::finish(io::FileWriter* file_writer,
                                               ColumnIndexMetaPB* index_meta) {
    index_meta->set_type(BLOOM_FILTER_INDEX);
    BloomFilterIndexPB* meta = index_meta->mutable_bloom_filter_index();
    meta->set_hash_strategy(CITY_HASH_64);
    meta->set_algorithm(NGRAM_BLOOM_FILTER);

    // write bloom filters
    const TypeInfo* bf_typeinfo = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_VARCHAR);
    IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = false;
    options.encoding = PLAIN_ENCODING;
    IndexedColumnWriter bf_writer(options, bf_typeinfo, file_writer);
    RETURN_IF_ERROR(bf_writer.init());
    for (auto& bf : _bfs) {
        Slice data(bf->data(), bf->size());
        static_cast<void>(bf_writer.add(&data));
    }
    RETURN_IF_ERROR(bf_writer.finish(meta->mutable_bloom_filter()));
    return Status::OK();
}

uint64_t NGramBloomFilterIndexWriterImpl::size() {
    uint64_t total_size = _bf_buffer_size;
    total_size += _arena.size();
    return total_size;
}

// TODO currently we don't support bloom filter index for tinyint/hll/float/double
Status BloomFilterIndexWriter::create(const BloomFilterOptions& bf_options,
                                      const TypeInfo* type_info,
                                      std::unique_ptr<BloomFilterIndexWriter>* res) {
    FieldType type = type_info->type();
    switch (type) {
#define M(TYPE)                                                                  \
    case TYPE:                                                                   \
        res->reset(new BloomFilterIndexWriterImpl<TYPE>(bf_options, type_info)); \
        break;
        M(FieldType::OLAP_FIELD_TYPE_SMALLINT)
        M(FieldType::OLAP_FIELD_TYPE_INT)
        M(FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT)
        M(FieldType::OLAP_FIELD_TYPE_BIGINT)
        M(FieldType::OLAP_FIELD_TYPE_LARGEINT)
        M(FieldType::OLAP_FIELD_TYPE_CHAR)
        M(FieldType::OLAP_FIELD_TYPE_VARCHAR)
        M(FieldType::OLAP_FIELD_TYPE_STRING)
        M(FieldType::OLAP_FIELD_TYPE_DATE)
        M(FieldType::OLAP_FIELD_TYPE_DATETIME)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL)
        M(FieldType::OLAP_FIELD_TYPE_DATEV2)
        M(FieldType::OLAP_FIELD_TYPE_DATETIMEV2)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL32)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL64)
        M(FieldType::OLAP_FIELD_TYPE_DECIMAL128I)
#undef M
    default:
        return Status::NotSupported("unsupported type for bitmap index: {}",
                                    std::to_string(int(type)));
    }
    return Status::OK();
}

Status NGramBloomFilterIndexWriterImpl::create(const BloomFilterOptions& bf_options,
                                               const TypeInfo* typeinfo, uint8_t gram_size,
                                               uint16_t gram_bf_size,
                                               std::unique_ptr<BloomFilterIndexWriter>* res) {
    FieldType type = typeinfo->type();
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_CHAR:
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
        res->reset(new NGramBloomFilterIndexWriterImpl(bf_options, gram_size, gram_bf_size));
        break;
    default:
        return Status::NotSupported("unsupported type for ngram bloom filter index:{}",
                                    std::to_string(int(type)));
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
