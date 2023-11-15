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
#include "olap/rowset/segment_v2/inverted_index/query/inverted_index_query.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "olap/tablet_schema.h"
#include "runtime/type_limit.h"

namespace lucene {
namespace store {
class Directory;
} // namespace store
namespace util::bkd {
class bkd_docid_set_iterator;
} // namespace util::bkd
} // namespace lucene
namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class KeyCoder;
class TypeInfo;
struct OlapReaderStatistics;
class RuntimeState;
enum class PredicateType;

namespace segment_v2 {

class InvertedIndexIterator;
class InvertedIndexQueryCacheHandle;

enum class InvertedIndexReaderType {
    UNKNOWN = -1,
    FULLTEXT = 0,
    STRING_TYPE = 1,
    BKD = 2,
};

using IndexSearcherPtr = std::shared_ptr<lucene::search::IndexSearcher>;

class InvertedIndexReader : public std::enable_shared_from_this<InvertedIndexReader> {
public:
    explicit InvertedIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                 const TabletIndex* index_meta)
            : _fs(std::move(fs)), _path(path), _index_meta(*index_meta) {
        io::Path io_path(_path);
        auto index_dir = io_path.parent_path();
        auto index_file_name = InvertedIndexDescriptor::get_index_file_name(io_path.filename(),
                                                                            index_meta->index_id());
        auto index_file_path = index_dir / index_file_name;
        _file_full_path = index_file_path;
        _file_name = index_file_name;
        _file_dir = index_dir.c_str();
    }
    virtual Status handle_cache(InvertedIndexQueryCache* cache,
                                const InvertedIndexQueryCache::CacheKey& cache_key,
                                InvertedIndexQueryCacheHandle* cache_handler,
                                OlapReaderStatistics* stats, roaring::Roaring* bit_map) {
        if (cache->lookup(cache_key, cache_handler)) {
            stats->inverted_index_query_cache_hit++;
            SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_copy_timer);
            *bit_map = *cache_handler->get_bitmap();
            return Status::OK();
        }
        stats->inverted_index_query_cache_miss++;
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("cache miss");
    }
    virtual ~InvertedIndexReader() = default;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                std::unique_ptr<InvertedIndexIterator>* iterator) = 0;
    virtual Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                         const std::string& column_name, InvertedIndexQueryBase* query_value,
                         roaring::Roaring* bit_map) = 0;
    virtual Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                             InvertedIndexQueryBase* query_value, uint32_t* count) = 0;

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr);

    virtual InvertedIndexReaderType type() = 0;
    bool indexExists(io::Path& index_file_path);

    [[nodiscard]] uint32_t get_index_id() const { return _index_meta.index_id(); }

    [[nodiscard]] const std::map<string, string>& get_index_properties() const {
        return _index_meta.properties();
    }

    static std::vector<std::string> get_analyse_result(lucene::util::Reader* reader,
                                                       lucene::analysis::Analyzer* analyzer,
                                                       const std::string& field_name,
                                                       InvertedIndexQueryType query_type,
                                                       bool drop_duplicates = true);
    static std::unique_ptr<lucene::util::Reader> create_reader(InvertedIndexCtx* inverted_index_ctx,
                                                               const std::string& value);
    static std::unique_ptr<lucene::analysis::Analyzer> create_analyzer(
            InvertedIndexCtx* inverted_index_ctx);

protected:
    friend class InvertedIndexIterator;
    io::FileSystemSPtr _fs;
    const std::string& _path;
    TabletIndex _index_meta;
    io::Path _file_full_path;
    std::string _file_name;
    std::string _file_dir;
};

class FullTextIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(FullTextIndexReader);

private:
    InvertedIndexCtxSPtr _inverted_index_ctx {};

public:
    explicit FullTextIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                 const TabletIndex* index_meta)
            : InvertedIndexReader(fs, path, index_meta) {
        _inverted_index_ctx = std::make_shared<InvertedIndexCtx>();
        _inverted_index_ctx->parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_index_meta.properties()));
        _inverted_index_ctx->parser_mode =
                get_parser_mode_string_from_properties(_index_meta.properties());
        _inverted_index_ctx->char_filter_map =
                get_parser_char_filter_map_from_properties(_index_meta.properties());
    }
    ~FullTextIndexReader() override = default;

    Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                        std::unique_ptr<InvertedIndexIterator>* iterator) override;
    Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                 const std::string& column_name, InvertedIndexQueryBase* query_value,
                 roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     InvertedIndexQueryBase* query_value, uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "FullTextIndexReader not support try_query");
    }

    InvertedIndexReaderType type() override;

private:
    Status normal_index_search(OlapReaderStatistics* stats, InvertedIndexQueryType query_type,
                               const IndexSearcherPtr& index_searcher,
                               bool& null_bitmap_already_read,
                               const std::unique_ptr<lucene::search::Query>& query,
                               const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    Status match_all_index_search(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                  const std::wstring& field_ws,
                                  const std::vector<std::string>& analyse_result,
                                  const IndexSearcherPtr& index_searcher,
                                  const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    void check_null_bitmap(const IndexSearcherPtr& index_searcher, bool& null_bitmap_already_read);
    Status _query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                  const std::string& column_name, std::string& search_str,
                  InvertedIndexQueryType query_type, roaring::Roaring* bit_map);
};

class StringTypeInvertedIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(StringTypeInvertedIndexReader);

public:
    explicit StringTypeInvertedIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                           const TabletIndex* index_meta)
            : InvertedIndexReader(fs, path, index_meta) {}
    ~StringTypeInvertedIndexReader() override = default;

    Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                        std::unique_ptr<InvertedIndexIterator>* iterator) override;
    Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                 const std::string& column_name, InvertedIndexQueryBase* query_value,
                 roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     InvertedIndexQueryBase* query_value, uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "StringTypeInvertedIndexReader not support try_query");
    }
    Status handle_range_query(const std::string& column_name, OlapReaderStatistics* stats,
                              InvertedIndexRangeQueryI* query, roaring::Roaring* bit_map);
    Status handle_point_query(const std::string& column_name, OlapReaderStatistics* stats,
                              InvertedIndexPointQueryI* query, roaring::Roaring* bit_map);

    InvertedIndexReaderType type() override;
};

class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    roaring::Roaring* _hits;
    uint32_t _num_hits;
    bool _only_count;
    lucene::util::bkd::bkd_reader* _reader;
    PredicateType _low_op;
    PredicateType _high_op;

public:
    BinaryType query_min;
    BinaryType query_max;
    std::vector<BinaryType> query_points;

public:
    InvertedIndexVisitor(roaring::Roaring* hits, InvertedIndexQueryBase* query_value,
                         bool only_count = false);
    ~InvertedIndexVisitor() override = default;

    void set_reader(lucene::util::bkd::bkd_reader* r) { _reader = r; }

    void visit(int row_id) override;
    void visit(roaring::Roaring& r) override;
    void visit(roaring::Roaring&& r) override;
    void visit(roaring::Roaring* doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(std::vector<char>& doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(int row_id, std::vector<uint8_t>& packed_value) override;
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packed_value) override;
    bool matches(uint8_t* packed_value);
    bool _matches(const BinaryType& packed_value, const BinaryType& qmax, const BinaryType& qmin);
    lucene::util::bkd::relation compare(std::vector<uint8_t>& min_packed,
                                        std::vector<uint8_t>& max_packed) override;
    lucene::util::bkd::relation _compare(const BinaryType& min_packed, const BinaryType& max_packed,
                                         const BinaryType& qmax, const BinaryType& qmin);
    uint32_t get_num_hits() const { return _num_hits; }
};

class BkdIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(BkdIndexReader);

public:
    explicit BkdIndexReader(io::FileSystemSPtr fs, const std::string& path,
                            const TabletIndex* index_meta);
    ~BkdIndexReader() override {
        if (_compoundReader != nullptr) {
            try {
                _compoundReader->close();
            } catch (const CLuceneError& e) {
                // Handle exception, e.g., log it, but don't rethrow.
                LOG(ERROR) << "Exception caught in BkdIndexReader destructor: " << e.what()
                           << std::endl;
            } catch (...) {
                // Handle all other exceptions, but don't rethrow.
                LOG(ERROR) << "Unknown exception caught in BkdIndexReader destructor." << std::endl;
            }
        }
    }

    Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                        std::unique_ptr<InvertedIndexIterator>* iterator) override;

    Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                 const std::string& column_name, InvertedIndexQueryBase* query_value,
                 roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     InvertedIndexQueryBase* query_value, uint32_t* count) override;

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(std::shared_ptr<lucene::util::bkd::bkd_reader>* reader);

private:
    const TypeInfo* _type_info {};
    std::unique_ptr<DorisCompoundReader> _compoundReader;
};

class InvertedIndexIterator {
    ENABLE_FACTORY_CREATOR(InvertedIndexIterator);

public:
    InvertedIndexIterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                          std::shared_ptr<InvertedIndexReader> reader)
            : _stats(stats), _runtime_state(runtime_state), _reader(std::move(reader)) {}

    Status read_from_inverted_index(const std::string& column_name,
                                    InvertedIndexQueryBase* query_value, uint32_t segment_num_rows,
                                    roaring::Roaring* bit_map, bool skip_try = false);
    Status try_read_from_inverted_index(const std::string& column_name,
                                        InvertedIndexQueryBase* query_value, uint32_t* count);

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) {
        return _reader->read_null_bitmap(cache_handle, dir);
    }

    [[nodiscard]] InvertedIndexReaderType get_inverted_index_reader_type() const;
    [[nodiscard]] const std::map<string, string>& get_index_properties() const;

private:
    OlapReaderStatistics* _stats = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::shared_ptr<InvertedIndexReader> _reader;
};

} // namespace segment_v2
} // namespace doris
