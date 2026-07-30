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

// End-to-end proof that an UNMODIFIED CLucene BKD index round-trips through an
// SNII container as an opaque blob logical index (design 2026-07-28, G1):
//   bkd_writer -> staged FSDirectory -> SniiCompoundWriter::add_blob_index ->
//   container -> SniiSegmentReader::blob_entry -> SniiBlobDirectory ->
//   bkd_reader (zero changes) -> range queries == the staged-directory answers.
// Also pins the empty-BKD contract (§3.4): a 0-point segment stores 0-byte
// `bkd`/`bkd_index` files and bkd_reader::open() must return false -- not
// throw -- when served from the container.

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/NumericUtils.h>
#include <CLucene/util/bkd/bkd_reader.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_blob_directory.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"

using doris::Status;
using doris::segment_v2::snii_doris::DorisSniiFileReader;
using doris::segment_v2::snii_doris::SniiBlobDirectory;
using doris::snii::format::LogicalIndexKind;
using doris::snii::format::LogicalIndexMetadataRef;
using lucene::util::bkd::bkd_reader;
using lucene::util::bkd::bkd_writer;
using lucene::util::bkd::relation;

namespace {

constexpr int kDocCount = 2000;
constexpr uint64_t kIndexId = 9;

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_blob_bkd_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

// 1D int range visitor over sortable bytes; collects hits into a set.
class RangeVisitor : public bkd_reader::intersect_visitor {
public:
    RangeVisitor(int query_min, int query_max, std::set<int>* hits) : hits_(hits) {
        min_bytes_.resize(4);
        max_bytes_.resize(4);
        lucene::util::NumericUtils::intToSortableBytes(query_min, min_bytes_, 0);
        lucene::util::NumericUtils::intToSortableBytes(query_max, max_bytes_, 0);
    }

    void visit(int docid) override { hits_->insert(docid); }
    void visit(roaring::Roaring& docids) override {
        for (const auto docid : docids) {
            hits_->insert(static_cast<int>(docid));
        }
    }
    void visit(roaring::Roaring&& docids) override { visit(docids); }
    int visit(int docid, std::vector<uint8_t>& packed) override {
        if (matches(packed.data())) {
            hits_->insert(docid);
        }
        return 0;
    }
    void visit(std::vector<char>& docids, std::vector<uint8_t>& packed) override {
        if (!matches(packed.data())) {
            return;
        }
        auto bitmap = roaring::Roaring::read(docids.data(), false);
        visit(bitmap);
    }
    void visit(roaring::Roaring* docids, std::vector<uint8_t>& packed) override {
        if (matches(packed.data())) {
            visit(*docids);
        }
    }
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packed) override {
        if (!matches(packed.data())) {
            return;
        }
        int32_t docid = iter->docid_set->nextDoc();
        while (docid != lucene::util::bkd::bkd_docid_set::NO_MORE_DOCS) {
            hits_->insert(docid);
            docid = iter->docid_set->nextDoc();
        }
    }
    relation compare(std::vector<uint8_t>& min_packed, std::vector<uint8_t>& max_packed) override {
        if (std::memcmp(max_packed.data(), min_bytes_.data(), 4) < 0 ||
            std::memcmp(min_packed.data(), max_bytes_.data(), 4) > 0) {
            return relation::CELL_OUTSIDE_QUERY;
        }
        if (std::memcmp(min_packed.data(), min_bytes_.data(), 4) >= 0 &&
            std::memcmp(max_packed.data(), max_bytes_.data(), 4) <= 0) {
            return relation::CELL_INSIDE_QUERY;
        }
        return relation::CELL_CROSSES_QUERY;
    }
    relation compare_prefix(std::vector<uint8_t>& /*prefix*/) override {
        return relation::CELL_CROSSES_QUERY;
    }

private:
    bool matches(const uint8_t* packed) const {
        return std::memcmp(packed, min_bytes_.data(), 4) >= 0 &&
               std::memcmp(packed, max_bytes_.data(), 4) <= 0;
    }

    std::vector<uint8_t> min_bytes_;
    std::vector<uint8_t> max_bytes_;
    std::set<int>* hits_;
};

// Builds a 1D int BKD index (docid < 1000 -> 100, else 200) through the real
// bkd_writer -- an EMPTY one (0 points, 0-byte bkd/bkd_index) when doc_count
// == 0 -- and returns the three staged files' bytes. A RAMDirectory stands in
// for the production staging directory.
using StagedFiles = std::map<std::string, std::vector<uint8_t>>;

void BuildBkdPayloads(int doc_count, StagedFiles* files) {
    auto dir = std::make_unique<lucene::store::RAMDirectory>();

    // bkd_writer requires totalPointCount >= maxPointsInLeafNode (it sizes the
    // in-heap sort by totalPointCount), so the 0-point case still declares a
    // roomy capacity -- exactly like the production writer, which sizes by the
    // segment row count regardless of how many values actually arrive.
    const int capacity = std::max(doc_count, 1024);
    auto writer = std::make_shared<bkd_writer>(capacity, 1, 1, 4, 512, 100.0, capacity, true);
    writer->docs_seen_ = doc_count;
    std::vector<uint8_t> scratch(4);
    for (int docid = 0; docid < doc_count; ++docid) {
        lucene::util::NumericUtils::intToSortableBytes(docid < 1000 ? 100 : 200, scratch, 0);
        writer->add(scratch.data(), scratch.size(), docid);
    }

    {
        std::unique_ptr<lucene::store::IndexOutput> out(dir->createOutput("bkd"));
        std::unique_ptr<lucene::store::IndexOutput> index_out(dir->createOutput("bkd_index"));
        std::unique_ptr<lucene::store::IndexOutput> meta_out(dir->createOutput("bkd_meta"));
        const int64_t index_fp = writer->finish(out.get(), index_out.get());
        writer->meta_finish(meta_out.get(), index_fp, 0);
        out->close();
        index_out->close();
        meta_out->close();
    }

    for (const char* name : {"bkd", "bkd_meta", "bkd_index"}) {
        const int64_t length = dir->fileLength(name);
        std::vector<uint8_t> bytes(static_cast<size_t>(length));
        if (length > 0) {
            lucene::store::IndexInput* input = nullptr;
            CLuceneError err;
            ASSERT_TRUE(dir->openInput(name, input, err)) << err.what();
            input->readBytes(bytes.data(), static_cast<int32_t>(length));
            input->close();
            _CLDELETE(input);
        }
        (*files)[name] = std::move(bytes);
    }
    dir->close();
}

doris::snii::writer::BlobFileSource MemorySource(std::string name, std::vector<uint8_t> payload) {
    const auto data = std::make_shared<std::vector<uint8_t>>(std::move(payload));
    doris::snii::writer::BlobFileSource source;
    source.name = std::move(name);
    source.length = data->size();
    source.read_fn = [data](uint64_t offset, size_t len, uint8_t* out) -> Status {
        if (offset > data->size() || len > data->size() - offset) {
            return Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "staged blob source: read past end");
        }
        std::memcpy(out, data->data() + offset, len);
        return Status::OK();
    };
    return source;
}

// Packs the three staged BKD files into a fresh SNII container (cold: bkd;
// hot: bkd_meta + bkd_index) and returns its path.
std::string PackContainer(const StagedFiles& files) {
    const std::string path = TempPath();
    doris::snii::io::LocalFileWriter file_writer;
    EXPECT_TRUE(file_writer.open(path).ok());
    doris::snii::writer::SniiCompoundWriter compound(&file_writer);
    EXPECT_TRUE(compound.add_blob_index(kIndexId, "", LogicalIndexKind::kBkd,
                                        {MemorySource("bkd", files.at("bkd"))},
                                        {MemorySource("bkd_meta", files.at("bkd_meta")),
                                         MemorySource("bkd_index", files.at("bkd_index"))})
                        .ok());
    EXPECT_TRUE(compound.finish().ok());
    return path;
}

using RamDirPtr = std::unique_ptr<lucene::store::RAMDirectory, doris::segment_v2::DirectoryDeleter>;

// Rebuilds a RAMDirectory holding the staged files -- the baseline the
// container-served reader is compared against.
RamDirPtr MakeRamDirectory(const StagedFiles& files) {
    RamDirPtr dir(_CLNEW lucene::store::RAMDirectory());
    for (const auto& [name, bytes] : files) {
        std::unique_ptr<lucene::store::IndexOutput> out(dir->createOutput(name.c_str()));
        if (!bytes.empty()) {
            out->writeBytes(bytes.data(), static_cast<int32_t>(bytes.size()));
        }
        out->close();
    }
    return dir;
}

// Opens the container's blob entry as a lucene Directory.
Status OpenBlobDirectory(const std::string& path, std::shared_ptr<DorisSniiFileReader>* reader,
                         doris::segment_v2::snii_doris::SniiBlobDirectoryPtr* dir) {
    doris::io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(doris::io::global_local_filesystem()->open_file(path, &file_reader));
    *reader = std::make_shared<DorisSniiFileReader>(std::move(file_reader));
    doris::snii::reader::SniiSegmentReader segment;
    RETURN_IF_ERROR(doris::snii::reader::SniiSegmentReader::open(reader->get(), &segment));
    const LogicalIndexMetadataRef* entry = nullptr;
    RETURN_IF_ERROR(segment.blob_entry(kIndexId, "", &entry));
    return SniiBlobDirectory::open(*reader, *entry, segment.directory_offset(), dir);
}

std::set<int> RunRangeQuery(bkd_reader* reader, int query_min, int query_max) {
    std::set<int> hits;
    RangeVisitor visitor(query_min, query_max, &hits);
    reader->intersect(&visitor);
    return hits;
}

TEST(SniiBlobBkdRoundTrip, UnmodifiedBkdReaderAnswersRangeQueriesFromContainer) {
    StagedFiles files;
    BuildBkdPayloads(kDocCount, &files);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    const std::string path = PackContainer(files);

    // Baseline: the same index queried straight from the staged files.
    RamDirPtr ram = MakeRamDirectory(files);
    auto baseline = std::make_shared<bkd_reader>(ram.get(), /*close_directory=*/false);
    ASSERT_TRUE(baseline->open());

    std::shared_ptr<DorisSniiFileReader> reader;
    doris::segment_v2::snii_doris::SniiBlobDirectoryPtr blob_dir;
    ASSERT_TRUE(OpenBlobDirectory(path, &reader, &blob_dir).ok());
    auto packed = std::make_shared<bkd_reader>(blob_dir.get(), /*close_directory=*/false);
    ASSERT_TRUE(packed->open());

    EXPECT_EQ(baseline->point_count_, packed->point_count_);
    EXPECT_EQ(baseline->doc_count_, packed->doc_count_);
    EXPECT_EQ(baseline->num_leaves_, packed->num_leaves_);
    EXPECT_EQ(baseline->bytes_per_dim_, packed->bytes_per_dim_);
    EXPECT_EQ(baseline->min_packed_value_, packed->min_packed_value_);
    EXPECT_EQ(baseline->max_packed_value_, packed->max_packed_value_);

    for (const auto& [lo, hi] : std::vector<std::pair<int, int>> {
                 {150, 250}, {50, 100}, {100, 200}, {201, 999}, {-5, 42}}) {
        const auto expected = RunRangeQuery(baseline.get(), lo, hi);
        const auto actual = RunRangeQuery(packed.get(), lo, hi);
        EXPECT_EQ(expected, actual) << "range [" << lo << "," << hi << "]";
    }
    // Sanity against absolute truth, not just baseline parity.
    EXPECT_EQ(1000U, RunRangeQuery(packed.get(), 150, 250).size()); // docids 1000..1999
    EXPECT_EQ(2000U, RunRangeQuery(packed.get(), 100, 200).size()); // all docs
    EXPECT_TRUE(RunRangeQuery(packed.get(), 201, 999).empty());

    packed.reset();
    baseline.reset();
    ram->close();
    blob_dir->close();
    std::remove(path.c_str());
}

TEST(SniiBlobBkdRoundTrip, EmptyBkdSegmentOpensAsEmptyNotCorrupt) {
    StagedFiles files;
    BuildBkdPayloads(0, &files);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    // §2.3(c): the empty segment REALLY produces 0-byte bkd / bkd_index plus a
    // 12-byte sentinel meta -- the very shape the directory must accept.
    EXPECT_TRUE(files.at("bkd").empty());
    EXPECT_TRUE(files.at("bkd_index").empty());
    EXPECT_FALSE(files.at("bkd_meta").empty());
    const std::string path = PackContainer(files);

    std::shared_ptr<DorisSniiFileReader> reader;
    doris::segment_v2::snii_doris::SniiBlobDirectoryPtr blob_dir;
    ASSERT_TRUE(OpenBlobDirectory(path, &reader, &blob_dir).ok());
    EXPECT_EQ(0, blob_dir->fileLength("bkd"));
    EXPECT_EQ(0, blob_dir->fileLength("bkd_index"));

    auto packed = std::make_shared<bkd_reader>(blob_dir.get(), /*close_directory=*/false);
    bool opened = true;
    EXPECT_NO_THROW({ opened = packed->open(); });
    EXPECT_FALSE(opened); // "empty index", NOT an exception / corruption

    packed.reset();
    blob_dir->close();
    std::remove(path.c_str());
}

} // namespace
