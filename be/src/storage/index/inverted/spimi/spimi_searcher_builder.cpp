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
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow-field"
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/search/IndexSearcher.h>
#pragma GCC diagnostic pop
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
    std::vector<uint8_t> tis, tii, frq, prx, fnm, segs;
    // V4 = pure SPIMI. The V4 writer (P37c-3) emits segment files
    // under the canonical `_0.*` names — there is no CLucene
    // primary competing for them. SegmentInfos lives at the
    // standard `segments_1` (no `spimi_` prefix).
    RETURN_IF_ERROR(ReadAllBytes(directory, "_0.tis", &tis));
    RETURN_IF_ERROR(ReadAllBytes(directory, "_0.tii", &tii));
    RETURN_IF_ERROR(ReadAllBytes(directory, "_0.frq", &frq));
    RETURN_IF_ERROR(ReadAllBytes(directory, "_0.prx", &prx));
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

    std::unique_ptr<lucene::index::IndexReader> spimi_reader;
    try {
        spimi_reader = std::make_unique<inverted_index::spimi::SpimiQueryIndexReader>(
                std::move(tis), std::move(tii), std::move(frq), std::move(prx), std::move(fnm),
                max_doc);
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
