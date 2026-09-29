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

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::index_query {

class TermPattern;
class IndexSource;
using IndexSourcePtr = std::shared_ptr<IndexSource>;

// One part of a source split by segment, with the first docid its documents map to.
struct IndexSegment {
    IndexSourcePtr source;
    uint32_t doc_base = 0;
};

// One field's index as the engine reads it: postings by term, the dictionary by pattern and
// the documents still alive. A format adapter implements it over one segment of one field.
class IndexSource {
public:
    virtual ~IndexSource() = default;

    // The number of documents, the bound of every docid the source lists.
    virtual uint32_t doc_count() const = 0;

    // Resolves a leaf's UTF-8 terms ahead of their opens, in the batches the format allows.
    virtual Status prepare_terms(std::span<const std::string> terms) {
        (void)terms;
        return Status::OK();
    }

    // The postings of a UTF-8 term: with positions, and with frequencies and norms, as asked.
    // A term the dictionary lacks yields a null cursor.
    virtual Status open_term(std::string_view term, bool positions, bool scoring,
                             std::unique_ptr<PostingsCursor>* out) = 0;

    // The terms `pattern` matches, in dictionary order, at most `max_expansions` of them when
    // that is positive.
    virtual Status expand_terms(TermPattern& pattern, int32_t max_expansions,
                                std::vector<std::string>* out) = 0;

    // Whether the source reads in batched rounds: a leaf then opens all its terms at once and
    // the engine lists them term at a time, materializing what it needs, instead of driving
    // one document at a time across them.
    virtual bool batches_reads() const { return false; }

    // Opens several terms at once, so a batching source resolves them in one round and reads
    // their preludes in one round when a cursor first needs one; a term the dictionary lacks
    // yields a null cursor at its index.
    virtual Status open_terms(std::span<const std::string> terms, bool positions, bool scoring,
                              std::vector<std::unique_ptr<PostingsCursor>>* out) {
        out->clear();
        for (const std::string& term : terms) {
            std::unique_ptr<PostingsCursor> cursor;
            RETURN_IF_ERROR(open_term(term, positions, scoring, &cursor));
            out->push_back(std::move(cursor));
        }
        return Status::OK();
    }

    // Whether the index may hold `term`, answered without reading its dictionary: false only
    // when it surely does not. A source with no such test answers true.
    virtual Status may_hold(std::string_view /*term*/, bool* held) {
        *held = true;
        return Status::OK();
    }

    // Issues the reads the cursors opened together registered since the last call, in one
    // round; a source that reads on demand has nothing to issue.
    virtual Status fetch_pending() { return Status::OK(); }

    // Whether the segment still holds the document.
    virtual bool is_live(uint32_t doc) const {
        (void)doc;
        return true;
    }

    // The parts of a source split by segment, in docid order; empty when it is one segment.
    virtual std::vector<IndexSegment> segments() const { return {}; }
};

} // namespace doris::index_query
