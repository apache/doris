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
#include <stdint.h>

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

class InvertedIndexReader : public std::enable_shared_from_this<InvertedIndexReader> {
public:
    explicit InvertedIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                 const TabletIndex* index_meta)
            : _fs(fs), _path(path), _index_meta(*index_meta) {
        io::Path io_path(_path);
        _index_dir = io_path.parent_path();
        _index_file_name = InvertedIndexDescriptor::get_index_file_name(
                io_path.filename(), index_meta->index_id(), index_meta->get_index_suffix());
    }
    virtual ~InvertedIndexReader() = default;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                std::unique_ptr<InvertedIndexIterator>* iterator) = 0;
    virtual Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                         const std::string& column_name, const void* query_value,
                         InvertedIndexQueryType query_type, roaring::Roaring* bit_map) = 0;
    virtual Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                             const void* query_value, InvertedIndexQueryType query_type,
                             uint32_t* count) = 0;

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr);

    virtual InvertedIndexReaderType type() = 0;
    bool indexExists(io::Path& index_file_path);

    [[nodiscard]] uint32_t get_index_id() const { return _index_meta.index_id(); }

    [[nodiscard]] const std::map<string, string>& get_index_properties() const {
        return _index_meta.properties();
    }

    [[nodiscard]] bool has_null() const { return _has_null; }

    static void get_analyse_result(std::vector<std::string>& analyse_result,
                                   lucene::util::Reader* reader,
                                   lucene::analysis::Analyzer* analyzer,
                                   const std::string& field_name, InvertedIndexQueryType query_type,
                                   bool drop_duplicates = true);

    static std::unique_ptr<lucene::util::Reader> create_reader(InvertedIndexCtx* inverted_index_ctx,
                                                               const std::string& value);
    static std::unique_ptr<lucene::analysis::Analyzer> create_analyzer(
            InvertedIndexCtx* inverted_index_ctx);

protected:
    bool _is_range_query(InvertedIndexQueryType query_type);
    bool _is_match_query(InvertedIndexQueryType query_type);
    friend class InvertedIndexIterator;
    io::FileSystemSPtr _fs;
    const std::string& _path;
    TabletIndex _index_meta;
    std::string _index_file_name;
    io::Path _index_dir;
    bool _has_null {true};
};

class FullTextIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(FullTextIndexReader);

public:
    explicit FullTextIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                 const TabletIndex* index_meta)
            : InvertedIndexReader(fs, path, index_meta) {}
    ~FullTextIndexReader() override = default;

    Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                        std::unique_ptr<InvertedIndexIterator>* iterator) override;
    Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                 const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "FullTextIndexReader not support try_query");
    }

    InvertedIndexReaderType type() override;

private:
    Status normal_index_search(OlapReaderStatistics* stats, InvertedIndexQueryType query_type,
                               const FulltextIndexSearcherPtr& index_searcher,
                               bool& null_bitmap_already_read,
                               const std::unique_ptr<lucene::search::Query>& query,
                               const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    Status match_all_index_search(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                  const std::wstring& field_ws,
                                  const std::vector<std::string>& analyse_result,
                                  const FulltextIndexSearcherPtr& index_searcher,
                                  const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    Status match_phrase_prefix_index_search(
            OlapReaderStatistics* stats, RuntimeState* runtime_state, const std::wstring& field_ws,
            const std::vector<std::string>& analyse_result,
            const FulltextIndexSearcherPtr& index_searcher,
            const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    Status match_regexp_index_search(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                     const std::wstring& field_ws, const std::string& pattern,
                                     const FulltextIndexSearcherPtr& index_searcher,
                                     const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    void check_null_bitmap(const FulltextIndexSearcherPtr& index_searcher,
                           bool& null_bitmap_already_read);
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
                 const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override;
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
    InvertedIndexVisitor(roaring::Roaring* hits, bool only_count = false);
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
    explicit BkdIndexReader(io::FileSystemSPtr fs, const std::string& path,
                            const TabletIndex* index_meta)
            : InvertedIndexReader(fs, path, index_meta) {}
    ~BkdIndexReader() override = default;

    Status new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                        std::unique_ptr<InvertedIndexIterator>* iterator) override;

    Status query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                 const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     uint32_t* count) override;
    Status invoke_bkd_try_query(OlapReaderStatistics* stats, const std::string& column_name,
                                const void* query_value, InvertedIndexQueryType query_type,
                                std::shared_ptr<lucene::util::bkd::bkd_reader> r, uint32_t* count);
    Status invoke_bkd_query(OlapReaderStatistics* stats, const std::string& column_name,
                            const void* query_value, InvertedIndexQueryType query_type,
                            std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                            roaring::Roaring* bit_map);
    template <InvertedIndexQueryType QT>
    Status bkd_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                     InvertedIndexVisitor<QT>* visitor);

    Status handle_cache(InvertedIndexQueryCache* cache,
                        const InvertedIndexQueryCache::CacheKey& cache_key,
                        InvertedIndexQueryCacheHandle* cache_handler, OlapReaderStatistics* stats,
                        roaring::Roaring* bit_map);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(BKDIndexSearcherPtr& reader, OlapReaderStatistics* stats);

private:
    const TypeInfo* _type_info {};
    const KeyCoder* _value_key_coder {};
};

class InvertedIndexIterator {
    ENABLE_FACTORY_CREATOR(InvertedIndexIterator);

public:
    InvertedIndexIterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                          std::shared_ptr<InvertedIndexReader> reader)
            : _stats(stats), _runtime_state(runtime_state), _reader(reader) {}

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, uint32_t segment_num_rows,
                                    roaring::Roaring* bit_map, bool skip_try = false);
    Status try_read_from_inverted_index(const std::string& column_name, const void* query_value,
                                        InvertedIndexQueryType query_type, uint32_t* count);

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) {
        return _reader->read_null_bitmap(cache_handle, dir);
    }

    [[nodiscard]] InvertedIndexReaderType get_inverted_index_reader_type() const;
    [[nodiscard]] const std::map<string, string>& get_index_properties() const;
    [[nodiscard]] bool has_null() { return _reader->has_null(); };

private:
    OlapReaderStatistics* _stats = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::shared_ptr<InvertedIndexReader> _reader;
};

} // namespace segment_v2
} // namespace doris
