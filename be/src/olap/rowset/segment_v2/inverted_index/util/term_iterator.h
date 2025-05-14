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

#include "CLucene/index/Terms.h"

CL_NS_USE(index)

namespace doris::segment_v2 {

class TermIterator {
public:
    TermIterator() = default;
    TermIterator(TermDocs* term_docs) : term_docs_(term_docs, TermDocsDeleter {}) {}
    virtual ~TermIterator() = default;

    bool is_empty() const { return term_docs_ == nullptr; }

    int32_t doc_id() const {
        int32_t docId = term_docs_->doc();
        return docId >= INT_MAX ? INT_MAX : docId;
    }

    int32_t freq() const { return term_docs_->freq(); }

    int32_t next_doc() const {
        if (term_docs_->next()) {
            return term_docs_->doc();
        }
        return INT_MAX;
    }

    int32_t advance(int32_t target) const {
        if (term_docs_->skipTo(target)) {
            return term_docs_->doc();
        }
        return INT_MAX;
    }

    int64_t doc_freq() const { return term_docs_->docFreq(); }

    bool read_range(DocRange* docRange) const { return term_docs_->readRange(docRange); }

    static TermDocs* ensure_term_doc(IndexReader* reader, const std::wstring& field_name,
                                     const std::string& term) {
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        return ensure_term_doc(reader, field_name, ws_term);
    }

    static TermDocs* ensure_term_doc(IndexReader* reader, const std::wstring& field_name,
                                     const std::wstring& ws_term) {
        auto* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        auto* term_pos = reader->termDocs(t);
        _CLDECDELETE(t);
        return term_pos;
    }

private:
    struct TermDocsDeleter {
        void operator()(TermDocs* p) const {
            if (p) {
                _CLDELETE(p);
            }
        }
    };

protected:
    std::shared_ptr<TermDocs> term_docs_;
};

} // namespace doris::segment_v2