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
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

namespace {

// Builder for bloom filter. In doris, bloom filter index is used in
// high cardinality key columns and none-agg value columns for high selectivity and storage
// efficiency.
// This builder builds bloom filter by every expected num records(not necessary distinct).
// Meanswhile, It adds an ordinal index to load bloom filter index according to requirement.
//
template <FieldType field_type>
class BloomFilterIndexWriterImpl : public BloomFilterIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    explicit BloomFilterIndexWriterImpl(const TypeInfo* typeinfo, WritableFile* file)
        : _bf_writer(nullptr) {
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = false;
        options.encoding = BLOOM_FILTER;
        options.compression = LZ4F;
        _bf_writer = new IndexedColumnWriter(options, typeinfo, file);
        _bf_writer->init();
    }

    ~BloomFilterIndexWriterImpl() {
        delete _bf_writer;
    }

    void add_values(const void* values, size_t count) override {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            _bf_writer->add(p);
            p++;
        }
    }

    void add_nulls(uint32_t count) override {
        for (size_t i = 0; i < count; ++i) {
            _bf_writer->add(nullptr);
        }
    }

    Status finish(BloomFilterIndexPB* meta) override {
        meta->set_hash_strategy(HASH_MURMUR3_X64_64);
        meta->set_algorithm(BLOCK_BLOOM_FILTER);
        RETURN_IF_ERROR(_bf_writer->finish(meta->mutable_bloom_filter()));
        return Status::OK();
    }

    uint64_t size() override {
        return _bf_writer->size();
    }

private:
    IndexedColumnWriter* _bf_writer;
};

} // namespace

// TODO currently we don't support bloom filter index for float/double/date/datetime/decimal/hll
Status BloomFilterIndexWriter::create(const TypeInfo* typeinfo, WritableFile* file, std::unique_ptr<BloomFilterIndexWriter>* res) {
    FieldType type = typeinfo->type();
    switch (type) {
        case OLAP_FIELD_TYPE_TINYINT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_TINYINT>(typeinfo, file));
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_SMALLINT>(typeinfo, file));
            break;
        case OLAP_FIELD_TYPE_INT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_INT>(typeinfo, file));
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_UNSIGNED_INT>(typeinfo, file));
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_BIGINT>(typeinfo, file));
            break;
        case OLAP_FIELD_TYPE_CHAR:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_CHAR>(typeinfo, file));
            break;
        case OLAP_FIELD_TYPE_VARCHAR:
            res->reset(new BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_VARCHAR>(typeinfo, file));
            break;
        default:
            return Status::NotSupported("unsupported type for bitmap index: " + std::to_string(type));
    }
    return Status::OK();
}

} // segment_v2
} // namespace doris
