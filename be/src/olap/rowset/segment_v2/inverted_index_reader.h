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

#include <CLucene/util/bkd/bkd_reader.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "olap/tablet_schema.h"
#include "runtime/primitive_type.h"
#include "util/once.h"

#define FINALIZE_INPUT(x) \
    if (x != nullptr) {   \
        x->close();       \
        _CLDELETE(x);     \
    }
#define FINALLY_FINALIZE_INPUT(x) \
    try {                         \
        FINALIZE_INPUT(x)         \
    } catch (...) {               \
    }

namespace lucene {
namespace store {
class Directory;
} // namespace store
namespace util {
namespace bkd {
class bkd_docid_set_iterator;
} // namespace bkd
} // namespace util
} // namespace lucene
namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class KeyCoder;
class TypeInfo;
struct OlapReaderStatistics;
class RuntimeState;

namespace segment_v2 {

class InvertedIndexIterator;
class InvertedIndexQueryCacheHandle;
class InvertedIndexFileReader;
struct InvertedIndexQueryInfo;
class InvertedIndexResultBitmap {
private:
    std::shared_ptr<roaring::Roaring> _data_bitmap = nullptr;
    std::shared_ptr<roaring::Roaring> _null_bitmap = nullptr;

public:
    // Default constructor
    InvertedIndexResultBitmap() = default;
    ~InvertedIndexResultBitmap() = default;

    // Constructor with arguments
    InvertedIndexResultBitmap(std::shared_ptr<roaring::Roaring> data_bitmap,
                              std::shared_ptr<roaring::Roaring> null_bitmap)
            : _data_bitmap(std::move(data_bitmap)), _null_bitmap(std::move(null_bitmap)) {}

    // Copy constructor
    InvertedIndexResultBitmap(const InvertedIndexResultBitmap& other)
            : _data_bitmap(std::make_shared<roaring::Roaring>(*other._data_bitmap)),
              _null_bitmap(std::make_shared<roaring::Roaring>(*other._null_bitmap)) {}

    // Move constructor
    InvertedIndexResultBitmap(InvertedIndexResultBitmap&& other) noexcept
            : _data_bitmap(std::move(other._data_bitmap)),
              _null_bitmap(std::move(other._null_bitmap)) {}

    // Copy assignment operator
    InvertedIndexResultBitmap& operator=(const InvertedIndexResultBitmap& other) {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = std::make_shared<roaring::Roaring>(*other._data_bitmap);
            _null_bitmap = std::make_shared<roaring::Roaring>(*other._null_bitmap);
        }
        return *this;
    }

    // Move assignment operator
    InvertedIndexResultBitmap& operator=(InvertedIndexResultBitmap&& other) noexcept {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = std::move(other._data_bitmap);
            _null_bitmap = std::move(other._null_bitmap);
        }
        return *this;
    }

    // Operator &=
    InvertedIndexResultBitmap& operator&=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && _null_bitmap && other._data_bitmap && other._null_bitmap) {
            auto new_null_bitmap = (*_data_bitmap & *other._null_bitmap) |
                                   (*_null_bitmap & *other._data_bitmap) |
                                   (*_null_bitmap & *other._null_bitmap);
            *_data_bitmap &= *other._data_bitmap;
            *_null_bitmap = std::move(new_null_bitmap);
        }
        return *this;
    }

    // Operator |=
    InvertedIndexResultBitmap& operator|=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && _null_bitmap && other._data_bitmap && other._null_bitmap) {
            auto new_null_bitmap = (*_null_bitmap | *other._null_bitmap) - *_data_bitmap;
            *_data_bitmap |= *other._data_bitmap;
            *_null_bitmap = std::move(new_null_bitmap);
        }
        return *this;
    }

    // NOT operation
    const InvertedIndexResultBitmap& op_not(const roaring::Roaring* universe) const {
        if (_data_bitmap && _null_bitmap) {
            *_data_bitmap = *universe - *_data_bitmap - *_null_bitmap;
            // The _null_bitmap remains unchanged.
        }
        return *this;
    }

    // Operator -=
    InvertedIndexResultBitmap& operator-=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && _null_bitmap && other._data_bitmap && other._null_bitmap) {
            *_data_bitmap -= *other._data_bitmap;
            *_data_bitmap -= *other._null_bitmap;
            *_null_bitmap -= *other._null_bitmap;
        }
        return *this;
    }

    void mask_out_null() {
        if (_data_bitmap && _null_bitmap) {
            *_data_bitmap -= *_null_bitmap;
        }
    }

    const std::shared_ptr<roaring::Roaring>& get_data_bitmap() const { return _data_bitmap; }

    const std::shared_ptr<roaring::Roaring>& get_null_bitmap() const { return _null_bitmap; }

    // Check if both bitmaps are empty
    bool is_empty() const { return (_data_bitmap == nullptr && _null_bitmap == nullptr); }
};

class InvertedIndexReader : public std::enable_shared_from_this<InvertedIndexReader>,
                            public MetadataAdder<InvertedIndexReader> {
public:
    explicit InvertedIndexReader(
            const TabletIndex* index_meta,
            std::shared_ptr<InvertedIndexFileReader> inverted_index_file_reader)
            : _inverted_index_file_reader(std::move(inverted_index_file_reader)),
              _index_meta(*index_meta) {}
    virtual ~InvertedIndexReader() = default;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                std::unique_ptr<InvertedIndexIterator>* iterator) = 0;
    virtual Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                         const std::string& column_name, const void* query_value,
                         InvertedIndexQueryType query_type,
                         std::shared_ptr<roaring::Roaring>& bit_map) = 0;
    virtual Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                             const void* query_value, InvertedIndexQueryType query_type,
                             uint32_t* count) = 0;

    Status read_null_bitmap(OlapReaderStatistics* stats,
                            InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr);

    virtual InvertedIndexReaderType type() = 0;

    [[nodiscard]] uint64_t get_index_id() const { return _index_meta.index_id(); }

    [[nodiscard]] const std::map<string, string>& get_index_properties() const {
        return _index_meta.properties();
    }

    [[nodiscard]] bool has_null() const { return _has_null; }
    void set_has_null(bool has_null) { _has_null = has_null; }

    static void get_analyse_result(std::vector<std::string>& analyse_result,
                                   lucene::util::Reader* reader,
                                   lucene::analysis::Analyzer* analyzer,
                                   const std::string& field_name, InvertedIndexQueryType query_type,
                                   bool drop_duplicates = true);

    static std::unique_ptr<lucene::util::Reader> create_reader(InvertedIndexCtx* inverted_index_ctx,
                                                               const std::string& value);
    static std::unique_ptr<lucene::analysis::Analyzer> create_analyzer(
            InvertedIndexCtx* inverted_index_ctx);

    virtual Status handle_query_cache(InvertedIndexQueryCache* cache,
                                      const InvertedIndexQueryCache::CacheKey& cache_key,
                                      InvertedIndexQueryCacheHandle* cache_handler,
                                      OlapReaderStatistics* stats,
                                      std::shared_ptr<roaring::Roaring>& bit_map) {
        if (cache->lookup(cache_key, cache_handler)) {
            stats->inverted_index_query_cache_hit++;
            SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_copy_timer);
            bit_map = cache_handler->get_bitmap();
            return Status::OK();
        }
        stats->inverted_index_query_cache_miss++;
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("cache miss");
    }

    virtual Status handle_searcher_cache(InvertedIndexCacheHandle* inverted_index_cache_handle,
                                         OlapReaderStatistics* stats);
    std::string get_index_file_path();
    static Status create_index_searcher(lucene::store::Directory* dir, IndexSearcherPtr* searcher,
                                        MemTracker* mem_tracker,
                                        InvertedIndexReaderType reader_type);

protected:
    Status match_index_search(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                              InvertedIndexQueryType query_type,
                              const InvertedIndexQueryInfo& query_info,
                              const FulltextIndexSearcherPtr& index_searcher,
                              const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    friend class InvertedIndexIterator;
    std::shared_ptr<InvertedIndexFileReader> _inverted_index_file_reader;
    TabletIndex _index_meta;
    bool _has_null = true;
};
using InvertedIndexReaderPtr = std::shared_ptr<InvertedIndexReader>;

class FullTextIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(FullTextIndexReader);

public:
    explicit FullTextIndexReader(
            const TabletIndex* index_meta,
            std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader)
            : InvertedIndexReader(index_meta, inverted_index_file_reader) {}
    ~FullTextIndexReader() override = default;

    Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                        std::unique_ptr<InvertedIndexIterator>* iterator) override;
    Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                 const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "FullTextIndexReader not support try_query");
    }

    InvertedIndexReaderType type() override;

    static void setup_analyzer_lowercase(std::unique_ptr<lucene::analysis::Analyzer>& analyzer,
                                         const std::map<string, string>& properties);
    static void setup_analyzer_use_stopwords(std::unique_ptr<lucene::analysis::Analyzer>& analyzer,
                                             const std::map<string, string>& properties);
};

class StringTypeInvertedIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(StringTypeInvertedIndexReader);

public:
    explicit StringTypeInvertedIndexReader(
            const TabletIndex* index_meta,
            std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader)
            : InvertedIndexReader(index_meta, inverted_index_file_reader) {}
    ~StringTypeInvertedIndexReader() override = default;

    Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                        std::unique_ptr<InvertedIndexIterator>* iterator) override;
    Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                 const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "StringTypeInvertedIndexReader not support try_query");
    }
    InvertedIndexReaderType type() override;
};

template <InvertedIndexQueryType QT>
class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    roaring::Roaring* _hits = nullptr;
    uint32_t _num_hits;
    bool _only_count;
    lucene::util::bkd::bkd_reader* _reader = nullptr;

public:
    std::string query_min;
    std::string query_max;

public:
    InvertedIndexVisitor(lucene::util::bkd::bkd_reader* r, roaring::Roaring* hits,
                         bool only_count = false);
    ~InvertedIndexVisitor() override = default;

    void set_reader(lucene::util::bkd::bkd_reader* r) { _reader = r; }
    lucene::util::bkd::bkd_reader* get_reader() { return _reader; }

    void visit(int row_id) override;
    void visit(roaring::Roaring& r) override;
    void visit(roaring::Roaring&& r) override;
    void visit(roaring::Roaring* doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(std::vector<char>& doc_id, std::vector<uint8_t>& packed_value) override;
    int visit(int row_id, std::vector<uint8_t>& packed_value) override;
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packed_value) override;
    int matches(uint8_t* packed_value);
    lucene::util::bkd::relation compare(std::vector<uint8_t>& min_packed,
                                        std::vector<uint8_t>& max_packed) override;
    lucene::util::bkd::relation compare_prefix(std::vector<uint8_t>& prefix) override;
    uint32_t get_num_hits() const { return _num_hits; }
};

class BkdIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(BkdIndexReader);

public:
    explicit BkdIndexReader(const TabletIndex* index_meta,
                            std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader)
            : InvertedIndexReader(index_meta, inverted_index_file_reader) {}
    ~BkdIndexReader() override = default;

    Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                        std::unique_ptr<InvertedIndexIterator>* iterator) override;

    Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                 const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     uint32_t* count) override;
    Status invoke_bkd_try_query(const void* query_value, InvertedIndexQueryType query_type,
                                std::shared_ptr<lucene::util::bkd::bkd_reader> r, uint32_t* count);
    Status invoke_bkd_query(const void* query_value, InvertedIndexQueryType query_type,
                            std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                            std::shared_ptr<roaring::Roaring>& bit_map);
    template <InvertedIndexQueryType QT>
    Status construct_bkd_query_value(const void* query_value,
                                     std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                     InvertedIndexVisitor<QT>* visitor);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(BKDIndexSearcherPtr& reader, OlapReaderStatistics* stats);

private:
    const TypeInfo* _type_info {};
    const KeyCoder* _value_key_coder {};
};

/**
 * @brief InvertedIndexQueryParamFactory is a factory class to create QueryValue object.
 * we need a template function to make predict class like in_list_predict template class to use.
 * also need a function with primitive type parameter to create inverted index query value. like some function expr: function_array_index
 * Now we just mapping field value in query engine to storage field value
 */
class InvertedIndexQueryParamFactory {
    ENABLE_FACTORY_CREATOR(InvertedIndexQueryParamFactory);

public:
    virtual ~InvertedIndexQueryParamFactory() = default;

    template <PrimitiveType PT>
    static Status create_query_value(const void* value,
                                     std::unique_ptr<InvertedIndexQueryParamFactory>& result_param);

    static Status create_query_value(
            const PrimitiveType& primitiveType, const void* value,
            std::unique_ptr<InvertedIndexQueryParamFactory>& result_param) {
        switch (primitiveType) {
#define M(TYPE)                                               \
    case TYPE: {                                              \
        return create_query_value<TYPE>(value, result_param); \
    }
            M(PrimitiveType::TYPE_BOOLEAN)
            M(PrimitiveType::TYPE_TINYINT)
            M(PrimitiveType::TYPE_SMALLINT)
            M(PrimitiveType::TYPE_INT)
            M(PrimitiveType::TYPE_BIGINT)
            M(PrimitiveType::TYPE_LARGEINT)
            M(PrimitiveType::TYPE_FLOAT)
            M(PrimitiveType::TYPE_DOUBLE)
            M(PrimitiveType::TYPE_DECIMALV2)
            M(PrimitiveType::TYPE_DECIMAL32)
            M(PrimitiveType::TYPE_DECIMAL64)
            M(PrimitiveType::TYPE_DECIMAL128I)
            M(PrimitiveType::TYPE_DECIMAL256)
            M(PrimitiveType::TYPE_DATE)
            M(PrimitiveType::TYPE_DATETIME)
            M(PrimitiveType::TYPE_CHAR)
            M(PrimitiveType::TYPE_VARCHAR)
            M(PrimitiveType::TYPE_STRING)
            M(PrimitiveType::TYPE_DATEV2)
            M(PrimitiveType::TYPE_DATETIMEV2)
#undef M
        default:
            return Status::NotSupported("Unsupported primitive type {} for inverted index reader",
                                        primitiveType);
        }
    };

    virtual const void* get_value() const {
        LOG_FATAL(
                "Execution reached an undefined behavior code path in "
                "InvertedIndexQueryParamFactory");
        __builtin_unreachable();
    };
};

template <PrimitiveType PT>
class InvertedIndexQueryParam : public InvertedIndexQueryParamFactory {
    ENABLE_FACTORY_CREATOR(InvertedIndexQueryParam);
    using storage_val = typename PrimitiveTypeTraits<PT>::StorageFieldType;

public:
    void set_value(const storage_val* value) {
        _value = *reinterpret_cast<const storage_val*>(value);
    }

    const void* get_value() const override { return &_value; }

private:
    storage_val _value;
};

class InvertedIndexIterator {
    ENABLE_FACTORY_CREATOR(InvertedIndexIterator);

public:
    InvertedIndexIterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                          std::shared_ptr<InvertedIndexReader> reader)
            : _stats(stats), _runtime_state(runtime_state), _reader(std::move(reader)) {}

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, uint32_t segment_num_rows,
                                    std::shared_ptr<roaring::Roaring>& bit_map,
                                    bool skip_try = false);
    Status try_read_from_inverted_index(const std::string& column_name, const void* query_value,
                                        InvertedIndexQueryType query_type, uint32_t* count);

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) {
        return _reader->read_null_bitmap(_stats, cache_handle, dir);
    }

    [[nodiscard]] InvertedIndexReaderType get_inverted_index_reader_type() const;
    [[nodiscard]] const std::map<string, string>& get_index_properties() const;
    [[nodiscard]] bool has_null() { return _reader->has_null(); };

    const InvertedIndexReaderPtr& reader() { return _reader; }

private:
    OlapReaderStatistics* _stats = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::shared_ptr<InvertedIndexReader> _reader;
};

} // namespace segment_v2
} // namespace doris
