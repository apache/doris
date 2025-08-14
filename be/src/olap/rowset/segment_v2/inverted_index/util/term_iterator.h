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

#include <CLucene.h> // IWYU pragma: keep

#include <memory>
#include <string>

#include "CLucene/index/Terms.h"
#include "common/be_mock_util.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"

CL_NS_USE(index)

namespace doris::io {
struct IOContext;
} // namespace doris::io

namespace doris::segment_v2 {

struct CLuceneDeleter {
    void operator()(TermDocs* p) const {
        if (p) {
            _CLDELETE(p);
        }
    }
};

class TermIterator;
using TermIterPtr = std::shared_ptr<TermIterator>;

class TermIterator {
public:
    using TermDocsPtr = std::unique_ptr<TermDocs, CLuceneDeleter>;

    TermIterator() = default;
    TermIterator(std::wstring term, TermDocsPtr term_docs)
            : term_(std::move(term)), term_docs_(std::move(term_docs)) {}
    virtual ~TermIterator() = default;

    MOCK_FUNCTION const std::wstring& term() const { return term_; }

    MOCK_FUNCTION int32_t doc_id() const {
        int32_t docId = term_docs_->doc();
        return docId >= INT_MAX ? INT_MAX : docId;
    }

    MOCK_FUNCTION int32_t freq() const { return term_docs_->freq(); }

    MOCK_FUNCTION int32_t norm() const { return term_docs_->norm(); }

    MOCK_FUNCTION int32_t next_doc() const {
        if (term_docs_->next()) {
            return term_docs_->doc();
        }
        return INT_MAX;
    }

    MOCK_FUNCTION int32_t advance(int32_t target) const {
        if (term_docs_->skipTo(target)) {
            return term_docs_->doc();
        }
        return INT_MAX;
    }

    MOCK_FUNCTION int32_t doc_freq() const { return term_docs_->docFreq(); }

    MOCK_FUNCTION bool read_range(DocRange* docRange) const {
        return term_docs_->readRange(docRange);
    }

    static TermIterPtr create(const io::IOContext* io_ctx, bool is_similarity,
                              lucene::index::IndexReader* reader, const std::wstring& field_name,
                              const std::string& term) {
        return create(io_ctx, is_similarity, reader, field_name,
                      StringUtil::string_to_wstring(term));
    }

    static TermIterPtr create(const io::IOContext* io_ctx, bool is_similarity,
                              lucene::index::IndexReader* reader, const std::wstring& field_name,
                              const std::wstring& ws_term) {
        auto t = make_term(field_name, ws_term);
        auto* term_pos = reader->termDocs(t.get(), is_similarity, io_ctx);
        return std::make_shared<TermIterator>(ws_term, TermDocsPtr(term_pos, CLuceneDeleter {}));
    }

protected:
    static TermPtr make_term(const std::wstring& field_name, const std::wstring& ws_term) {
        return TermPtr(new lucene::index::Term(field_name.c_str(), ws_term.c_str()),
                       TermDeleter {});
    }

    std::wstring term_;
    TermDocsPtr term_docs_;
};

} // namespace doris::segment_v2