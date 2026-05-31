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

#include "storage/index/inverted/spimi/spimi_searcher_builder.h"

// clang-format off
// `-Wshadow-field` is clang-only (CLucene headers trip it under clang
// strict builds); guard the whole block for __clang__ so g++ does not
// fail under -Werror=pragmas (matches inverted_index_searcher.h).
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/search/IndexSearcher.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif
// clang-format on

#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/spimi/query_index_reader.h"
#include "storage/index/inverted/spimi/segment_infos_reader.h"

namespace doris::segment_v2 {

namespace {

// Opens a single file from the directory and reads its entire
// contents into a vector. The CLucene IndexInput interface
// supports `length()` + `readBytes(buf, len)` so we can size the
// buffer exactly. Files are guaranteed to fit in memory because
// SPIMI emits one segment at a time and the byte readers it feeds
// expect whole-file inputs anyway.
Status ReadAllBytes(lucene::store::Directory* dir, const char* name, std::vector<uint8_t>* out) {
    if (!dir->fileExists(name)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "SPIMI segment missing required file '{}'", name);
    }
    lucene::store::IndexInput* in = nullptr;
    CLuceneError clu_err;
    if (!dir->openInput(name, in, clu_err)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "SPIMI openInput('{}') failed: {}", name, clu_err.what());
    }
    const int64_t len = in->length();
    DCHECK_GE(len, 0);
    out->resize(static_cast<size_t>(len));
    try {
        in->readBytes(out->data(), static_cast<int32_t>(len));
    } catch (const CLuceneError& e) {
        in->close();
        _CLDELETE(in);
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "SPIMI readBytes('{}', len={}) failed: {}", name, len, e.what());
    }
    in->close();
    _CLDELETE(in);
    return Status::OK();
}

} // namespace

Status SpimiSearcherBuilder::build(lucene::store::Directory* directory,
                                   OptionalIndexSearcherPtr& output_searcher) {
    if (directory == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "SpimiSearcherBuilder: directory is null");
    }
    std::vector<uint8_t> tis, tii, fnm, segs;
    // V4 = pure SPIMI. The V4 writer (P37c-3) emits segment files
    // under the canonical `_0.*` names — there is no CLucene
    // primary competing for them. SegmentInfos lives at the
    // standard `segments_1` (no `spimi_` prefix).
    //
    // The SMALL dictionary/manifest/field-info files (.tis/.tii/.fnm/
    // segments_1) stay FULLY RESIDENT (the hotcache the reader
    // random-accesses constantly). Both `.frq` AND `.prx` are kept as OPEN
    // IndexInputs (below) so a selective phrase/proximity query range-reads
    // only the `.frq`/`.prx` window(s) covering the docs it touches instead
    // of slurping the whole term.
    RETURN_IF_ERROR(ReadAllBytes(directory, "_0.tis", &tis));
    RETURN_IF_ERROR(ReadAllBytes(directory, "_0.tii", &tii));
    RETURN_IF_ERROR(ReadAllBytes(directory, "_0.fnm", &fnm));
    RETURN_IF_ERROR(ReadAllBytes(directory, "segments_1", &segs));

    inverted_index::spimi::SegmentInfosReader::Manifest manifest;
    try {
        manifest = inverted_index::spimi::SegmentInfosReader::Read(segs);
    } catch (const ::doris::Exception& e) {
        // Pure SPIMI readers throw `doris::Exception` with
        // `INVERTED_INDEX_FILE_CORRUPTED` on malformed bytes — no
        // longer `CLuceneError`. Preserve the message.
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "SPIMI segments_1 parse failed: {}", e.what());
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "SPIMI segments_1 parse failed: {}", e.what());
    } catch (...) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "SPIMI segments_1 parse failed (unknown error)");
    }
    if (manifest.segments.empty()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "SPIMI segments_1 carries no segments");
    }
    const int32_t max_doc = manifest.segments[0].doc_count;

    // Open `.frq` as a template IndexInput (clone source for per-thread
    // positioned reads) — done LAST so no error path sits between the open and
    // handing ownership to the reader (avoids leaking the open input).
    // `openInput` rejects a zero-length file (CL_ERR_EmptyIndexSegment); a
    // segment with zero postings has no `.frq` body and is not queryable —
    // surface a clear corruption error.
    if (!directory->fileExists("_0.frq")) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "SPIMI segment missing required file '_0.frq'");
    }
    lucene::store::IndexInput* frq_input_raw = nullptr;
    CLuceneError frq_err;
    if (!directory->openInput("_0.frq", frq_input_raw, frq_err)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "SPIMI openInput('_0.frq') failed: {}", frq_err.what());
    }
    // Wrap in the self-deleting owner immediately so the input is released on
    // ANY error before/inside reader construction (exception-safe transfer).
    inverted_index::spimi::OwnedFrqInput frq_input(frq_input_raw);

    // Open `.prx` as a template IndexInput (clone source for per-thread
    // positioned position reads). `openInput` rejects a zero-length file
    // (CL_ERR_EmptyIndexSegment): a segment whose only fields are omit_tfap has
    // an empty (or absent) `.prx` and never serves positions — pass a null
    // `.prx` input in that case (NOT an error). A non-empty `.prx` that fails
    // to open IS an error.
    inverted_index::spimi::OwnedPrxInput prx_input;
    if (directory->fileExists("_0.prx")) {
        lucene::store::IndexInput* prx_input_raw = nullptr;
        CLuceneError prx_err;
        if (directory->openInput("_0.prx", prx_input_raw, prx_err)) {
            prx_input.reset(prx_input_raw);
        } else if (prx_err.number() != CL_ERR_EmptyIndexSegment) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "SPIMI openInput('_0.prx') failed: {}", prx_err.what());
        }
        // CL_ERR_EmptyIndexSegment → leave prx_input null (omit_tfap-only).
    }

    std::unique_ptr<lucene::index::IndexReader> spimi_reader;
    try {
        spimi_reader = std::make_unique<inverted_index::spimi::SpimiQueryIndexReader>(
                std::move(tis), std::move(tii), std::move(frq_input), std::move(prx_input),
                directory, std::move(fnm), max_doc);
    } catch (const ::doris::Exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "SpimiQueryIndexReader build error: {}", e.what());
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "SpimiQueryIndexReader build error: {}", e.what());
    }
    reader_size = static_cast<size_t>(spimi_reader->getTermInfosRAMUsed());
    // `std::make_shared` either returns a valid pointer or throws
    // bad_alloc — it cannot return a null shared_ptr. The previous
    // `if (!searcher)` guard was dead-code defensive programming
    // that violated the CLAUDE.md "assert correctness, no
    // defensive if" rule. Remove it; bad_alloc propagates out as
    // expected.
    output_searcher = std::make_shared<lucene::search::IndexSearcher>(spimi_reader.release(),
                                                                      /*close_reader=*/true);
    return Status::OK();
}

} // namespace doris::segment_v2
