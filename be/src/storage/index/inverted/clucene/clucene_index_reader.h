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

#include "common/factory_creator.h"
#include "storage/index/inverted/clucene/inverted_index_searcher.h"
#include "storage/index/inverted/reader/text_index_reader.h"

namespace doris::segment_v2 {

// The CLucene text readers: an analyzed (FULLTEXT) or untokenized (STRING_TYPE) index answers a
// leaf over its searcher.
class CluceneTextIndexReader : public TextIndexReader {
public:
    using TextIndexReader::TextIndexReader;
    Status read_null_bitmap(const IndexQueryContextPtr& context,
                            InvertedIndexQueryCacheHandle* cache_handle) override;

protected:
    virtual Status handle_searcher_cache(const IndexQueryContextPtr& context,
                                         InvertedIndexCacheHandle* cache_handle,
                                         const std::string& index_file_key = {});
    Status _open_index(const IndexQueryContextPtr& context, std::unique_ptr<OpenedIndex>* out,
                       const std::string& index_file_key = {}) override;
    index_query::IndexSourcePtr _bind_source(const IndexQueryContextPtr& context,
                                             const std::wstring& field,
                                             OpenedIndex& index) override;
    Status _term_document_frequency(const std::string& column_name, OpenedIndex& index,
                                    const std::string& term, uint64_t* df,
                                    uint64_t* document_count) override;
    Status _run_leaf(const IndexQueryContextPtr& context, const std::string& column_name,
                     OpenedIndex& index, const index_query::logical::Node& leaf,
                     const roaring::Roaring* candidates, bool scoring,
                     std::shared_ptr<roaring::Roaring>* out) override;
};

// The null bitmap of a CLucene index, through the inverted index query cache. Reads it from
// `dir` when that is the index's open directory, otherwise opens the index.
Status read_clucene_null_bitmap(IndexFileReader& file_reader, const TabletIndex& index_meta,
                                const io::IOContext* io_ctx,
                                InvertedIndexQueryCacheHandle* cache_handle,
                                lucene::store::Directory* dir = nullptr);

// run_leaf on the CLucene field `field` of `searcher`, its errors reported as CLucene ones.
// `statistics_field` names the leaf inside the query, as _statistics_field does for an index.
Status run_clucene_leaf(const IndexQueryContextPtr& context, std::wstring field,
                        const std::wstring& statistics_field,
                        const index_query::logical::Node& leaf, const roaring::Roaring* candidates,
                        bool scoring, const FulltextIndexSearcherPtr& searcher,
                        const std::shared_ptr<roaring::Roaring>& result);
Status run_clucene_leaf(const IndexQueryContextPtr& context, std::wstring field,
                        const index_query::logical::Node& leaf, const roaring::Roaring* candidates,
                        bool scoring, const FulltextIndexSearcherPtr& searcher,
                        const std::shared_ptr<roaring::Roaring>& result);

class FullTextIndexReader : public CluceneTextIndexReader {
    ENABLE_FACTORY_CREATOR(FullTextIndexReader);

public:
    explicit FullTextIndexReader(const TabletIndex* index_meta,
                                 const std::shared_ptr<IndexFileReader>& index_file_reader,
                                 uint64_t rows_of_segment = 0, bool column_is_array = false)
            : CluceneTextIndexReader(index_meta, index_file_reader, rows_of_segment,
                                     column_is_array) {}
    ~FullTextIndexReader() override = default;

    InvertedIndexReaderType type() override { return InvertedIndexReaderType::FULLTEXT; }
};

// Range queries use CLucene directly; other query types use the shared MATCH path.
class StringTypeInvertedIndexReader : public CluceneTextIndexReader {
    ENABLE_FACTORY_CREATOR(StringTypeInvertedIndexReader);

public:
    explicit StringTypeInvertedIndexReader(
            const TabletIndex* index_meta,
            const std::shared_ptr<IndexFileReader>& index_file_reader, uint64_t rows_of_segment = 0,
            bool column_is_array = false)
            : CluceneTextIndexReader(index_meta, index_file_reader, rows_of_segment,
                                     column_is_array) {}
    ~StringTypeInvertedIndexReader() override = default;

    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status query_with_null_bitmap(const IndexQueryContextPtr& context,
                                  const std::string& column_name, const Field& query_value,
                                  InvertedIndexQueryType query_type,
                                  std::shared_ptr<roaring::Roaring>& bit_map,
                                  InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
                                  const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    InvertedIndexReaderType type() override { return InvertedIndexReaderType::STRING_TYPE; }
};

template <InvertedIndexQueryType QT>
class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    const void* _io_ctx = nullptr;
    roaring::Roaring* _hits = nullptr;
    uint32_t _num_hits;
    bool _only_count;
    lucene::util::bkd::bkd_reader* _reader = nullptr;

public:
    std::string query_min;
    std::string query_max;

    InvertedIndexVisitor(const void* io_ctx, lucene::util::bkd::bkd_reader* r,
                         roaring::Roaring* hits, bool only_count = false);
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
    const void* get_io_context() override { return _io_ctx; }
};

class BkdIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(BkdIndexReader);

public:
    explicit BkdIndexReader(const TabletIndex* index_meta,
                            const std::shared_ptr<IndexFileReader>& index_file_reader)
            : InvertedIndexReader(index_meta, index_file_reader) {}
    ~BkdIndexReader() override = default;
    Status read_null_bitmap(const IndexQueryContextPtr& context,
                            InvertedIndexQueryCacheHandle* cache_handle) override;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override;
    Status invoke_bkd_try_query(const IndexQueryContextPtr& context, const Field& query_value,
                                InvertedIndexQueryType query_type,
                                std::shared_ptr<lucene::util::bkd::bkd_reader> r, size_t* count);
    Status invoke_bkd_query(const IndexQueryContextPtr& context, const Field& query_value,
                            InvertedIndexQueryType query_type,
                            std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                            std::shared_ptr<roaring::Roaring>& bit_map);
    template <InvertedIndexQueryType QT>
    Status construct_bkd_query_value(const Field& query_value,
                                     std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                     InvertedIndexVisitor<QT>* visitor);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(const IndexQueryContextPtr& context, BKDIndexSearcherPtr& reader);

private:
    FieldType _type = FieldType::OLAP_FIELD_TYPE_NONE;
    const KeyCoder* _value_key_coder {};
};

Status create_index_searcher(IndexSearcherBuilder* builder, lucene::store::Directory* dir,
                             IndexSearcherPtr* searcher, size_t& reader_size);

} // namespace doris::segment_v2
