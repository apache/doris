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

// Blob logical index write path (SniiCompoundWriter::add_blob_index): opaque
// named-file entries (BKD / ANN carriers) registered before finish() and laid
// out by finish() as
//   [text posting|dict ...][norms|null|bsbf ...][blob COLD files ...]
//   [text metadata groups ...][blob HOT files ...][directory][tail]
// Registration never writes a byte; all blob bytes stream through read_fn
// during finish(). Any copy failure poisons the container (no tail).

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/format/tail_pointer.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/term_posting_source.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::writer;
using doris::Status;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_cw_blob_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

std::vector<uint8_t> ReadAll(const std::string& path) {
    io::LocalFileReader r;
    EXPECT_TRUE(r.open(path).ok());
    std::vector<uint8_t> out;
    EXPECT_TRUE(r.read_at(0, r.size(), &out).ok());
    return out;
}

// Deterministic pseudo-random payload so byte-compare failures are meaningful.
std::vector<uint8_t> Pattern(size_t n, uint8_t seed) {
    std::vector<uint8_t> out(n);
    for (size_t i = 0; i < n; ++i) {
        out[i] = static_cast<uint8_t>(seed + i * 7 + (i >> 8));
    }
    return out;
}

// BlobFileSource over an in-memory payload (the staged-directory stand-in).
BlobFileSource MemorySource(std::string name, std::vector<uint8_t> payload) {
    const auto data = std::make_shared<std::vector<uint8_t>>(std::move(payload));
    BlobFileSource source;
    source.name = std::move(name);
    source.length = data->size();
    source.read_fn = [data](uint64_t offset, size_t len, uint8_t* out) -> Status {
        if (offset > data->size() || len > data->size() - offset) {
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "test blob source: read past end");
        }
        std::memcpy(out, data->data() + offset, len);
        return Status::OK();
    };
    return source;
}

BlobFileSource FailingSource(std::string name, uint64_t length) {
    BlobFileSource source;
    source.name = std::move(name);
    source.length = length;
    source.read_fn = [](uint64_t, size_t, uint8_t*) -> Status {
        return Status::Error<doris::ErrorCode::IO_ERROR, false>("test blob source: injected");
    };
    return source;
}

// Feeds one materialized term through a streamed session (the compaction merge
// entry point), mirroring the helper in snii_streamed_session_test.
Status push_materialized(SniiStreamedIndexSession* session, TermPostings postings) {
    SpanTermPostingSource source(postings.docids, postings.freqs, postings.positions_flat);
    return session->push_term(StreamedTermPostings {.term = std::move(postings.term),
                                                    .retain_positions = postings.retain_positions,
                                                    .source = &source});
}

TermPostings MakeTerm(const std::string& term, const std::vector<uint32_t>& docids) {
    TermPostings tp;
    tp.term = term;
    tp.docids = docids;
    tp.freqs.assign(docids.size(), 1);
    for (size_t i = 0; i < docids.size(); ++i) {
        tp.positions_flat.push_back(0);
    }
    return tp;
}

SniiIndexInput TextIndex(uint64_t index_id, const std::string& suffix) {
    SniiIndexInput in;
    in.index_id = index_id;
    in.index_suffix = suffix;
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = 8;
    in.terms.push_back(MakeTerm("apple", {0, 3, 5}));
    in.terms.push_back(MakeTerm("zebra", {1, 7}));
    return in;
}

// Reads the container back through the production segment reader, which
// validates tail + directory crc + per-entry structure.
Status OpenSegment(io::LocalFileReader* reader, const std::string& path,
                   reader::SniiSegmentReader* segment) {
    RETURN_IF_ERROR(reader->open(path));
    return reader::SniiSegmentReader::open(reader, segment);
}

void ExpectFileBytes(const std::vector<uint8_t>& container, const NamedBlobFileRef& ref,
                     const std::vector<uint8_t>& want) {
    ASSERT_EQ(want.size(), ref.length) << ref.name;
    ASSERT_LE(ref.offset + ref.length, container.size()) << ref.name;
    EXPECT_EQ(0, std::memcmp(container.data() + ref.offset, want.data(), want.size())) << ref.name;
    EXPECT_EQ(doris::snii::crc32c(Slice(want)), ref.crc32c) << ref.name;
}

const NamedBlobFileRef* FindFile(const LogicalIndexMetadataRef& entry, const std::string& name) {
    for (const auto& file : entry.files) {
        if (file.name == name) return &file;
    }
    return nullptr;
}

TEST(SniiCompoundWriterBlob, RegistrationWritesNothingAndFinishLaysOutColdThenHot) {
    const std::string path = TempPath();
    const auto bkd_cold = Pattern(3000, 11);
    const auto bkd_meta = Pattern(12, 23);
    // > one 64 KiB copy chunk, so the streaming loop takes more than one pass.
    const auto ann_cold = Pattern((100U << 10) + 37, 41);

    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound(&writer);
        ASSERT_TRUE(compound.add_logical_index(TextIndex(7, "text")).ok());

        const uint64_t before = writer.bytes_written();
        ASSERT_TRUE(
                compound.add_blob_index(
                                9, "bkd", LogicalIndexKind::kBkd, {MemorySource("bkd", bkd_cold)},
                                {MemorySource("bkd_meta", bkd_meta), MemorySource("bkd_index", {})})
                        .ok());
        ASSERT_TRUE(compound.add_blob_index(10, "", LogicalIndexKind::kAnn,
                                            {MemorySource("ann.faiss", ann_cold)}, {})
                            .ok());
        // Registration is bookkeeping only -- blob bytes stream during finish().
        EXPECT_EQ(before, writer.bytes_written());
        ASSERT_TRUE(compound.finish().ok());
    }

    const auto container = ReadAll(path);
    io::LocalFileReader reader;
    reader::SniiSegmentReader segment;
    ASSERT_TRUE(OpenSegment(&reader, path, &segment).ok());
    ASSERT_EQ(3U, segment.n_logical_indexes());

    // The text index must remain fully readable next to blob entries.
    reader::LogicalIndexReader text;
    ASSERT_TRUE(segment.open_index(7, "text", &text).ok());

    TailPointer tail;
    {
        std::vector<uint8_t> tail_bytes(
                container.end() - static_cast<std::ptrdiff_t>(tail_pointer_size()),
                container.end());
        ASSERT_TRUE(decode_tail_pointer(Slice(tail_bytes), &tail).ok());
    }
    MetadataDirectory directory;
    ASSERT_TRUE(MetadataDirectory::decode(Slice(container.data() + tail.directory_offset,
                                                static_cast<size_t>(tail.directory_length)),
                                          &directory)
                        .ok());

    const auto* text_entry = directory.find(7, "text");
    const auto* bkd_entry = directory.find(9, "bkd");
    const auto* ann_entry = directory.find(10, "");
    ASSERT_NE(nullptr, text_entry);
    ASSERT_NE(nullptr, bkd_entry);
    ASSERT_NE(nullptr, ann_entry);
    EXPECT_EQ(LogicalIndexKind::kInverted, text_entry->kind);
    EXPECT_EQ(LogicalIndexKind::kBkd, bkd_entry->kind);
    EXPECT_EQ(LogicalIndexKind::kAnn, ann_entry->kind);

    // Byte-exact payloads + recorded crc32c.
    const auto* f_bkd = FindFile(*bkd_entry, "bkd");
    const auto* f_meta = FindFile(*bkd_entry, "bkd_meta");
    const auto* f_index = FindFile(*bkd_entry, "bkd_index");
    const auto* f_ann = FindFile(*ann_entry, "ann.faiss");
    ASSERT_NE(nullptr, f_bkd);
    ASSERT_NE(nullptr, f_meta);
    ASSERT_NE(nullptr, f_index);
    ASSERT_NE(nullptr, f_ann);
    ExpectFileBytes(container, *f_bkd, bkd_cold);
    ExpectFileBytes(container, *f_meta, bkd_meta);
    ExpectFileBytes(container, *f_index, {});
    ExpectFileBytes(container, *f_ann, ann_cold);

    // Layout: every COLD byte lives after the text index's physical sections
    // and strictly before the first metadata group; every HOT file sits after
    // the LAST text metadata group, adjacent within its entry, before the
    // directory.
    SectionRefs text_refs;
    ASSERT_TRUE(segment.section_refs_for_index(7, "text", &text_refs).ok());
    uint64_t text_section_end = 0;
    for (const RegionRef& region : {text_refs.dict_region, text_refs.posting_region,
                                    text_refs.norms, text_refs.null_bitmap, text_refs.bsbf}) {
        text_section_end = std::max(text_section_end, region.offset + region.length);
    }
    const uint64_t text_group_begin = text_entry->core_metadata.offset;
    const uint64_t text_group_end =
            text_entry->dict_block_directory.offset + text_entry->dict_block_directory.length;

    EXPECT_GE(f_bkd->offset, text_section_end);
    EXPECT_GE(f_ann->offset, f_bkd->offset + f_bkd->length); // cold in add order
    EXPECT_LE(f_ann->offset + f_ann->length, text_group_begin);

    EXPECT_GE(f_meta->offset, text_group_end);
    EXPECT_EQ(f_meta->offset + f_meta->length, f_index->offset); // hot adjacency
    EXPECT_LE(f_index->offset + f_index->length, tail.directory_offset);
}

TEST(SniiCompoundWriterBlob, BlobOnlyContainerRoundTrips) {
    const std::string path = TempPath();
    const auto payload = Pattern(200, 3);
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound(&writer);
        ASSERT_TRUE(compound.add_blob_index(5, "", LogicalIndexKind::kAnn,
                                            {MemorySource("ann.faiss", payload)}, {})
                            .ok());
        ASSERT_TRUE(compound.finish().ok());
    }
    io::LocalFileReader reader;
    reader::SniiSegmentReader segment;
    ASSERT_TRUE(OpenSegment(&reader, path, &segment).ok());
    ASSERT_EQ(1U, segment.n_logical_indexes());
    bool exists = false;
    ASSERT_TRUE(segment.index_exists(5, "", &exists).ok());
    EXPECT_TRUE(exists);
    std::remove(path.c_str());
}

TEST(SniiCompoundWriterBlob, RejectsInvertedKindAndDuplicateRegistrations) {
    io::LocalFileWriter writer;
    const std::string path = TempPath();
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);

    EXPECT_TRUE(compound.add_blob_index(5, "", LogicalIndexKind::kInverted,
                                        {MemorySource("x", Pattern(1, 1))}, {})
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(compound.add_blob_index(5, "", LogicalIndexKind::kBkd, {}, {})
                        .is<doris::ErrorCode::INVALID_ARGUMENT>()); // empty file table
    EXPECT_TRUE(compound.add_blob_index(5, "", LogicalIndexKind::kBkd,
                                        {MemorySource("dup", Pattern(1, 1)),
                                         MemorySource("dup", Pattern(1, 1))},
                                        {})
                        .is<doris::ErrorCode::INVALID_ARGUMENT>()); // duplicate file name

    ASSERT_TRUE(compound.add_blob_index(5, "", LogicalIndexKind::kBkd,
                                        {MemorySource("bkd", Pattern(1, 1))}, {})
                        .ok());
    // Same (index_id, suffix) registered twice.
    EXPECT_TRUE(compound.add_blob_index(5, "", LogicalIndexKind::kAnn,
                                        {MemorySource("ann.faiss", Pattern(1, 1))}, {})
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
    ASSERT_TRUE(compound.finish().ok());
    std::remove(path.c_str());
}

// The compaction-target shape (design 4.3): ONE writer takes a streamed text
// merge session AND blob registrations at the same time. Any SNII table with
// both a text and a numeric/vector index produces this on every compaction.
TEST(SniiCompoundWriterBlob, RegistersDuringStreamedSessionWithoutDisturbingIt) {
    const std::string path = TempPath();
    const auto payload = Pattern(5000, 61);
    SniiIndexInput streamed = TextIndex(7, "text");
    std::vector<TermPostings> terms = std::move(streamed.terms);
    streamed.terms.clear();

    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound(&writer);
        SniiStreamedIndexSession* session = nullptr;
        ASSERT_TRUE(compound.begin_streamed_index(std::move(streamed), &session).ok());
        ASSERT_TRUE(push_materialized(session, std::move(terms[0])).ok());

        // Registration mid-session must not move the write cursor: the session's
        // posting region is streaming into the container right now, and a byte
        // written here would land in the middle of it.
        const uint64_t before = writer.bytes_written();
        ASSERT_TRUE(compound.add_blob_index(9, "bkd", LogicalIndexKind::kBkd,
                                            {MemorySource("bkd", payload)}, {})
                            .ok());
        EXPECT_EQ(before, writer.bytes_written());

        ASSERT_TRUE(push_materialized(session, std::move(terms[1])).ok());
        ASSERT_TRUE(session->finish().ok());
        ASSERT_TRUE(compound.finish().ok());
    }

    const auto container = ReadAll(path);
    io::LocalFileReader reader;
    reader::SniiSegmentReader segment;
    ASSERT_TRUE(OpenSegment(&reader, path, &segment).ok());
    ASSERT_EQ(2U, segment.n_logical_indexes());

    // The streamed text index survived intact...
    reader::LogicalIndexReader text;
    ASSERT_TRUE(segment.open_index(7, "text", &text).ok());
    SectionRefs text_refs;
    ASSERT_TRUE(segment.section_refs_for_index(7, "text", &text_refs).ok());

    // ...and every blob byte landed after its posting+dict regions, unsplit.
    const LogicalIndexMetadataRef* blob = nullptr;
    ASSERT_TRUE(segment.blob_entry(9, "bkd", &blob).ok());
    const auto* file = FindFile(*blob, "bkd");
    ASSERT_NE(nullptr, file);
    EXPECT_GE(file->offset, text_refs.posting_region.offset + text_refs.posting_region.length);
    EXPECT_GE(file->offset, text_refs.dict_region.offset + text_refs.dict_region.length);
    ExpectFileBytes(container, *file, payload);
    std::remove(path.c_str());
}

TEST(SniiCompoundWriterBlob, RejectsKeyCollisionAgainstTextIndexAtRegistration) {
    // A duplicate key would let MetadataDirectory::find (first match wins)
    // silently shadow one of the two indexes -- the worst failure this format
    // has. It must be caught at registration, not after the blob is on disk.
    const std::string path = TempPath();
    io::LocalFileWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    ASSERT_TRUE(compound.add_logical_index(TextIndex(7, "text")).ok());

    const uint64_t before = writer.bytes_written();
    const Status collision = compound.add_blob_index(7, "text", LogicalIndexKind::kBkd,
                                                     {MemorySource("bkd", Pattern(64, 3))}, {});
    EXPECT_TRUE(collision.is<doris::ErrorCode::INVALID_ARGUMENT>()) << collision;
    EXPECT_EQ(before, writer.bytes_written()); // rejected before any copy
    // The plan was broken, so the container must never seal.
    EXPECT_FALSE(compound.finish().ok());
    std::remove(path.c_str());
}

TEST(SniiCompoundWriterBlob, RejectsUnknownKindAtRegistration) {
    const std::string path = TempPath();
    io::LocalFileWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    const Status status = compound.add_blob_index(9, "", static_cast<LogicalIndexKind>(7),
                                                  {MemorySource("blob", Pattern(64, 5))}, {});
    // INVALID_ARGUMENT (a caller bug), not the encoder's Unsupported after the
    // bytes are already written.
    EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status;
    EXPECT_EQ(0U, writer.bytes_written());
    std::remove(path.c_str());
}

TEST(SniiCompoundWriterBlob, CopyFailurePoisonsContainerAndSealsNothing) {
    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        ASSERT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound(&writer);
        ASSERT_TRUE(compound.add_logical_index(TextIndex(7, "text")).ok());
        ASSERT_TRUE(compound.add_blob_index(9, "bkd", LogicalIndexKind::kBkd,
                                            {FailingSource("bkd", 4096)}, {})
                            .ok());
        const Status finish = compound.finish();
        EXPECT_TRUE(finish.is<doris::ErrorCode::IO_ERROR>()) << finish;
        // Poisoned for good: a retry may not seal a container missing the blob.
        EXPECT_FALSE(compound.finish().ok());
    }
    // Whatever bytes hit the disk must not parse as a sealed container.
    io::LocalFileReader reader;
    reader::SniiSegmentReader segment;
    EXPECT_FALSE(OpenSegment(&reader, path, &segment).ok());
    std::remove(path.c_str());
}

} // namespace
