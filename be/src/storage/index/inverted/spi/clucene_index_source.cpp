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

#include "storage/index/inverted/spi/clucene_index_source.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Woverloaded-virtual"
#pragma clang diagnostic ignored "-Winconsistent-missing-override"
#pragma clang diagnostic ignored "-Wreorder-ctor"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif
#include <CLucene.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/MultiReader.h>
#include <CLucene/index/Term.h>
#include <CLucene/index/_MultiSegmentReader.h>
#ifdef __clang__
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#include <boost/locale/encoding_utf.hpp>

#include "storage/index/inverted/inverted_index_common_impl.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "storage/index/inverted/spi/clucene_postings_cursor.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/query/term_pattern.h"

namespace doris::segment_v2 {

namespace {

const lucene::util::ArrayBase<lucene::index::IndexReader*>* sub_readers(
        lucene::index::IndexReader* reader) {
    if (auto* multi_segment = dynamic_cast<lucene::index::MultiSegmentReader*>(reader)) {
        return multi_segment->getSubReaders();
    }
    if (auto* multi = dynamic_cast<lucene::index::MultiReader*>(reader)) {
        return multi->getSubReaders();
    }
    return nullptr;
}

std::unique_ptr<ClucenePostingsCursor> open_postings(lucene::index::IndexReader* reader,
                                                     lucene::index::Term* term, bool positions,
                                                     bool scoring, const io::IOContext* io_ctx) {
    if (positions) {
        auto iter = make_term_positions_ptr(reader, scoring, io_ctx);
        if (iter != nullptr) {
            iter->seek(term);
            return std::make_unique<ClucenePostingsCursor>(std::move(iter));
        }
    } else {
        auto iter = make_term_doc_ptr(reader, scoring, io_ctx);
        if (iter != nullptr) {
            iter->seek(term);
            return std::make_unique<ClucenePostingsCursor>(std::move(iter));
        }
    }
    return nullptr;
}

// Maps leaf blocks to global IDs while positions stay on their leaf cursor.
class PartitionedClucenePostings final : public index_query::PostingsCursor {
public:
    void add(std::unique_ptr<ClucenePostingsCursor> cursor, uint32_t begin, uint32_t end) {
        if (cursor != nullptr && cursor->doc_freq() != 0) {
            _doc_freq += cursor->doc_freq();
            _partitions.push_back({std::move(cursor), begin, end});
        }
    }

    uint32_t doc_freq() const override { return _doc_freq; }

    Status next_block(index_query::PostingsBlock* block, bool* eof) override {
        while (_current < _partitions.size()) {
            auto& part = _partitions[_current];
            RETURN_IF_ERROR(part.cursor->next_block(block, eof));
            if (!*eof) {
                _globalize(block, part.begin);
                return Status::OK();
            }
            ++_current;
        }
        *block = {};
        *eof = true;
        return Status::OK();
    }

    Status seek_block(uint32_t target, index_query::PostingsBlock* block, bool* eof) override {
        bool moved = false;
        RETURN_IF_ERROR(shallow_seek(target, &moved));
        return next_block(block, eof);
    }

    Status shallow_seek(uint32_t target, bool* moved) override {
        const size_t previous = _current;
        while (_current < _partitions.size() && target >= _partitions[_current].end) {
            ++_current;
        }
        *moved = _current != previous;
        if (_current < _partitions.size()) {
            auto& part = _partitions[_current];
            bool leaf_moved = false;
            RETURN_IF_ERROR(part.cursor->shallow_seek(target > part.begin ? target - part.begin : 0,
                                                      &leaf_moved));
            *moved = *moved || leaf_moved;
        }
        return Status::OK();
    }

    index_query::BlockBound current_block_bound() const override {
        if (_current == _partitions.size()) {
            return {};
        }
        const auto& part = _partitions[_current];
        auto bound = part.cursor->current_block_bound();
        bound.last_doc = bound.last_doc_known ? bound.last_doc + part.begin : part.end - 1;
        bound.last_doc_known = true;
        return bound;
    }

    Status open_positions(uint32_t ordinal, index_query::PositionCursor** out) override {
        return _partitions[_current].cursor->open_positions(ordinal, out);
    }

    Status open_position_stream(uint32_t ordinal, std::span<uint32_t> first_chunk, size_t* count,
                                index_query::PositionCursor** out) override {
        return _partitions[_current].cursor->open_position_stream(ordinal, first_chunk, count, out);
    }

    Status append_positions(uint32_t ordinal, uint32_t offset,
                            std::vector<uint32_t>& output) override {
        return _partitions[_current].cursor->append_positions(ordinal, offset, output);
    }

private:
    void _globalize(index_query::PostingsBlock* block, uint32_t begin) {
        if (begin == 0) {
            return;
        }
        _docs.resize(block->docs.size());
        for (size_t i = 0; i < block->docs.size(); ++i) {
            _docs[i] = block->docs[i] + begin;
        }
        block->docs = _docs;
        if (block->dense) {
            block->range_begin += begin;
            block->range_end += begin;
        }
    }

    struct Partition {
        std::unique_ptr<ClucenePostingsCursor> cursor;
        uint32_t begin;
        uint32_t end;
    };

    std::vector<Partition> _partitions;
    std::vector<uint32_t> _docs;
    size_t _current = 0;
    uint32_t _doc_freq = 0;
};

} // namespace

CluceneIndexSource::CluceneIndexSource(std::shared_ptr<lucene::index::IndexReader> reader,
                                       std::wstring field, const io::IOContext* io_ctx)
        : _reader(std::move(reader)), _field(std::move(field)), _io_ctx(io_ctx) {
    if (const auto* subs = sub_readers(_reader.get()); subs != nullptr && subs->length != 0) {
        _append_partitions(_reader.get(), 0);
    }
}

void CluceneIndexSource::_append_partitions(lucene::index::IndexReader* reader, uint32_t begin) {
    if (const auto* subs = sub_readers(reader); subs != nullptr && subs->length != 0) {
        for (size_t i = 0; i < subs->length; ++i) {
            auto* child = (*subs)[i];
            _append_partitions(child, begin);
            begin += static_cast<uint32_t>(child->maxDoc());
        }
    } else {
        _partitions.push_back({reader, begin, begin + static_cast<uint32_t>(reader->maxDoc())});
    }
}

uint32_t CluceneIndexSource::doc_count() const {
    return static_cast<uint32_t>(_reader->maxDoc());
}

Status CluceneIndexSource::open_term(std::string_view term, bool positions, bool scoring,
                                     std::unique_ptr<index_query::PostingsCursor>* out) {
    out->reset();
    const std::wstring text =
            boost::locale::conv::utf_to_utf<wchar_t>(term.data(), term.data() + term.size());
    try {
        auto t = make_term_ptr(_field.c_str(), text.c_str());
        if (_partitions.empty()) {
            *out = open_postings(_reader.get(), t.get(), positions, scoring, _io_ctx);
        } else {
            auto cursor = std::make_unique<PartitionedClucenePostings>();
            for (const auto& part : _partitions) {
                cursor->add(open_postings(part.reader, t.get(), positions, scoring, _io_ctx),
                            part.begin, part.end);
            }
            *out = std::move(cursor);
        }
    } catch (CLuceneError& e) {
        return clucene_error_status(e.what());
    }
    return Status::OK();
}

Status CluceneIndexSource::expand_terms(index_query::TermPattern& pattern, int32_t max_expansions,
                                        std::vector<std::string>* out) {
    out->clear();
    if (!pattern.can_match()) {
        return Status::OK();
    }
    const std::string& prefix = pattern.enumeration_prefix();
    const std::wstring start_text = inverted_index::StringHelper::to_wstring(prefix);
    // A term without the text every match holds is skipped before it is converted.
    const std::wstring required = inverted_index::StringHelper::to_wstring(pattern.required_text());
    lucene::index::Term start(_field.c_str(), start_text.c_str());
    ErrorContext error_context;
    auto close = [&](lucene::index::TermEnum* enumerator) {
        FINALLY_CLOSE(enumerator);
        _CLDELETE(enumerator);
    };
    std::unique_ptr<lucene::index::TermEnum, decltype(close)> enumerator(nullptr, close);
    try {
        enumerator.reset(_reader->terms(&start, _io_ctx));
        do {
            // The enumerator keeps its current term until next(), so no reference is taken.
            const lucene::index::Term* term = enumerator->term(false);
            if (term == nullptr || _field != term->field()) {
                break;
            }
            const std::wstring_view chars(term->text(), term->textLength());
            if (!required.empty() && chars.find(required) == std::wstring_view::npos) {
                continue;
            }
            std::string text = boost::locale::conv::utf_to_utf<char>(chars.data(),
                                                                     chars.data() + chars.size());
            if (!text.starts_with(prefix)) {
                break;
            }
            if (pattern.matches(text)) {
                out->push_back(std::move(text));
                if (max_expansions > 0 && out->size() == static_cast<size_t>(max_expansions)) {
                    break;
                }
            }
        } while (enumerator->next());
    } catch (CLuceneError& e) {
        error_context.eptr = std::current_exception();
        error_context.err_msg = e.what();
    }
    enumerator.reset();
    FINALLY({});
    return Status::OK();
}

std::span<const float> CluceneIndexSource::norm_lengths() const {
    return BM25Similarity::lucene_norm_lengths();
}

bool CluceneIndexSource::is_live(uint32_t doc) const {
    return !_reader->isDeleted(static_cast<int32_t>(doc));
}

std::shared_ptr<CluceneIndexSource> clucene_index_source(
        std::shared_ptr<lucene::index::IndexReader> reader, std::wstring field,
        const io::IOContext* io_ctx) {
    return std::make_shared<CluceneIndexSource>(std::move(reader), std::move(field), io_ctx);
}

} // namespace doris::segment_v2
