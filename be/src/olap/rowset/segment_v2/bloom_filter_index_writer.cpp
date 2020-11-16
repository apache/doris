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

#include <map>
#include <roaring/roaring.hh>

#include "env/env.h"
#include "olap/fs/block_manager.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/rowset/segment_v2/bloom_filter.h" // for BloomFilterOptions, BloomFilter
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

namespace {

template<typename CppType>
struct BloomFilterTraits {
    using ValueDict = std::set<CppType>;
};

template<>
struct BloomFilterTraits<Slice> {
    using ValueDict = std::set<Slice, Slice::Comparator>;
};

struct Int128Comparator {
    bool operator()(const PackedInt128& a, const PackedInt128& b) const {
        return a.value < b.value;
    }
};

template<>
struct BloomFilterTraits<int128_t> {
    using ValueDict = std::set<PackedInt128, Int128Comparator>;
};

// Builder for bloom filter. In doris, bloom filter index is used in
// high cardinality key columns and none-agg value columns for high selectivity and storage
// efficiency.
// This builder builds a bloom filter page by every data page, with a page id index.
// Meanswhile, It adds an ordinal index to load bloom filter index according to requirement.
//
template <FieldType field_type>
class BloomFilterIndexWriterImpl : public BloomFilterIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using ValueDict = typename BloomFilterTraits<CppType>::ValueDict;

    explicit BloomFilterIndexWriterImpl(const BloomFilterOptions& bf_options,
                                        const TypeInfo* typeinfo)
            : _bf_options(bf_options),
              _typeinfo(typeinfo),
              _tracker(new MemTracker(-1, "BloomFilterIndexWriterImpl")),
              _pool(_tracker.get()),
              _has_null(false),
              _bf_buffer_size(0) {}

    ~BloomFilterIndexWriterImpl() = default;

    void add_values(const void* values, size_t count) override {
        const CppType* v = (const CppType*)values;
        for (int i = 0; i < count; ++i) {
            if (_values.find(*v) == _values.end()) {
                if (_is_slice_type()) {
                    CppType new_value;
                    _typeinfo->deep_copy(&new_value, v, &_pool);
                    _values.insert(new_value);
                } else if (_is_int128()) {
                    PackedInt128 new_value;
                    memcpy(&new_value.value, v, sizeof(PackedInt128));
                    _values.insert((*reinterpret_cast<CppType*>(&new_value)));
                } else {
                    _values.insert(*v);
                }
            }
            ++v;
        }
    }

    void add_nulls(uint32_t count) override {
        _has_null = true;
    }

    Status flush() override {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
        RETURN_IF_ERROR(bf->init(_values.size(), _bf_options.fpp, _bf_options.strategy));
        bf->set_has_null(_has_null);
        for (auto& v : _values) {
            if (_is_slice_type()) {
                Slice* s = (Slice*)&v;
                bf->add_bytes(s->data, s->size);
            } else {
                bf->add_bytes((char*)&v, sizeof(CppType));
            }
        }
        _bf_buffer_size += bf->size();
        _bfs.push_back(std::move(bf));
        _values.clear();
        return Status::OK();
    }

    Status finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* index_meta) override {
        if (_values.size() > 0) {
            RETURN_IF_ERROR(flush());
        }
        index_meta->set_type(BLOOM_FILTER_INDEX);
        BloomFilterIndexPB* meta = index_meta->mutable_bloom_filter_index();
        meta->set_hash_strategy(_bf_options.strategy);
        meta->set_algorithm(BLOCK_BLOOM_FILTER);

        // write bloom filters
        const TypeInfo* bf_typeinfo = get_scalar_type_info(OLAP_FIELD_TYPE_VARCHAR);
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = false;
        options.encoding = PLAIN_ENCODING;
        IndexedColumnWriter bf_writer(options, bf_typeinfo, wblock);
        RETURN_IF_ERROR(bf_writer.init());
        for (auto& bf : _bfs) {
            Slice data(bf->data(), bf->size());
            bf_writer.add(&data);
        }
        RETURN_IF_ERROR(bf_writer.finish(meta->mutable_bloom_filter()));
        return Status::OK();
    }

    uint64_t size() override {
        uint64_t total_size = _bf_buffer_size;
        total_size += _pool.total_allocated_bytes();
        return total_size;
    }

private:
    // supported slice types are: OLAP_FIELD_TYPE_CHAR|OLAP_FIELD_TYPE_VARCHAR
    bool _is_slice_type() const {
        return field_type == OLAP_FIELD_TYPE_VARCHAR || field_type == OLAP_FIELD_TYPE_CHAR;
    }

    bool _is_int128() const {
        return field_type == OLAP_FIELD_TYPE_LARGEINT;
    }

private:
    BloomFilterOptions _bf_options;
    const TypeInfo* _typeinfo;
    std::shared_ptr<MemTracker> _tracker;
    MemPool _pool;
    bool _has_null;
    uint64_t _bf_buffer_size;
    // distinct values
    ValueDict _values;
    std::vector<std::unique_ptr<BloomFilter>> _bfs;
};

} // namespace

// TODO currently we don't support bloom filter index for tinyint/hll/float/double
Status BloomFilterIndexWriter::create(const BloomFilterOptions& bf_options,
        const TypeInfo* typeinfo, std::unique_ptr<BloomFilterIndexWriter>* res) {
    FieldType type = typeinfo->type();
    switch (type) {
        case OLAP_FIELD_TYPE_SMALLINT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_SMALLINT>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_INT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_INT>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_UNSIGNED_INT>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_BIGINT>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_LARGEINT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_LARGEINT>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_CHAR:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_CHAR>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_VARCHAR:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_VARCHAR>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_DATE:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DATE>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_DATETIME:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DATETIME>(bf_options, typeinfo));
            break;
        case OLAP_FIELD_TYPE_DECIMAL:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DECIMAL>(bf_options, typeinfo));
            break;
        default:
            return Status::NotSupported("unsupported type for bitmap index: " + std::to_string(type));
    }
    return Status::OK();
}

} // segment_v2
} // namespace doris
