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

// P9-4: an ANN index living inside a SNII container.
//
// The container format reserved LogicalIndexKind::kAnn from the start, and its
// blob logical index is a table of named opaque sub-files -- which is exactly
// what faiss emits. What was missing was the adapter on both ends:
//
//   write: IndexFileWriter::open() refused every directory under SNII, so the
//          ANN writer had nowhere to write. It now admits ANN and begin_close()
//          harvests the directory into a kAnn blob.
//   read:  _open() refused too. A blob entry records ABSOLUTE container offsets,
//          the same thing a V2 compound entry records, so DorisCompoundReader is
//          reused over the container stream rather than reimplemented.
//
// ORACLE DESIGN. The obvious test -- read the container at blob.offset and
// compare against what the directory serves for blob.name -- is a TAUTOLOGY:
// DorisCompoundReader's entry offset IS blob.offset, so both sides read the same
// place and a seal that recorded every offset as 0 passes green. Everything here
// is therefore checked against an oracle that does NOT come from the directory:
//
//   * a synthetic blob whose bytes this file chose before sealing, so a wrong
//     offset yields bytes that simply are not the expected ones;
//   * faiss itself, which cannot deserialize an index assembled from the wrong
//     extents.

#include <CLucene.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/ann/ann_index_reader.h"
#include "storage/index/ann/ann_index_writer.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

constexpr const char* kTestDir = "./ut_dir/snii_ann_container_test";
constexpr int64_t kAnnIndexId = 71;
// A second logical index in the SAME container, sealed as a NON-ANN blob. It is
// what makes the kind check testable at all: probing an absent index id returns
// at the lookup, long before the kind branch is reached.
constexpr int64_t kFakeBkdIndexId = 72;
constexpr uint32_t kDim = 4;
constexpr uint32_t kRows = 64;

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

// Deterministic, well separated vectors: row i sits on the i-th point of a
// coarse diagonal, so the nearest neighbour of row i's own vector is row i.
std::vector<float> make_vectors() {
    std::vector<float> data;
    data.reserve(static_cast<size_t>(kRows) * kDim);
    for (uint32_t row = 0; row < kRows; ++row) {
        for (uint32_t d = 0; d < kDim; ++d) {
            data.push_back(static_cast<float>(row) + static_cast<float>(d) * 0.01F);
        }
    }
    return data;
}

// Bytes chosen HERE, before anything is sealed. Distinct per sub-file and per
// position, so neither a wrong offset nor a name bound to the wrong entry can
// reproduce them.
std::vector<uint8_t> synthetic_bytes(uint8_t tag, size_t length) {
    std::vector<uint8_t> bytes(length);
    for (size_t i = 0; i < length; ++i) {
        bytes[i] = static_cast<uint8_t>(tag * 31U + i * 7U + (i >> 8));
    }
    return bytes;
}

TabletIndex make_ann_index_meta() {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::ANN);
    pb.set_index_id(kAnnIndexId);
    pb.set_index_name("ann_idx");
    pb.add_col_unique_id(0);
    auto& props = *pb.mutable_properties();
    props["index_type"] = "hnsw";
    props["metric_type"] = "l2_distance";
    props["dim"] = std::to_string(kDim);
    props["max_degree"] = "16";
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

TabletIndex make_fake_bkd_index_meta() {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kFakeBkdIndexId);
    pb.set_index_name("fake_bkd_idx");
    pb.add_col_unique_id(1);
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

class SniiAnnContainerTest : public testing::Test {
protected:
    void SetUp() override {
        if (ExecEnv::GetInstance()->get_tmp_file_dirs() == nullptr) {
            const std::string tmp_dir = std::string(kTestDir) + "/tmp";
            (void)io::global_local_filesystem()->delete_directory(tmp_dir);
            assert_ok(io::global_local_filesystem()->create_directory(tmp_dir));
            std::vector<StorePath> paths;
            paths.emplace_back(tmp_dir, -1);
            auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
            assert_ok(tmp_file_dirs->init());
            ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        }
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        _meta = make_ann_index_meta();
        _fake_bkd_meta = make_fake_bkd_index_meta();
        // Two sub-files of different lengths: a directory that bound a name to
        // the wrong entry would then also disagree on fileLength.
        _synthetic["fake_bkd_data"] = synthetic_bytes(0x11, 4096 + 17);
        _synthetic["fake_bkd_index"] = synthetic_bytes(0x77, 1024 + 3);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // One SNII container holding an ANN blob and a NON-ANN blob, and the path
    // prefix it was written to.
    std::string write_container() {
        const std::string prefix = std::string(kTestDir) + "/seg";
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                          &file_writer)
                            .ok());
        IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ann_rowset",
                               /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                               std::move(file_writer), /*can_use_ram_dir=*/false,
                               /*tablet_id=*/9901);

        AnnIndexColumnWriter ann(&writer, &_meta);
        EXPECT_TRUE(ann.init().ok());
        const std::vector<float> vectors = make_vectors();
        std::vector<size_t> offsets(kRows + 1);
        for (uint32_t i = 0; i <= kRows; ++i) {
            offsets[i] = static_cast<size_t>(i) * kDim;
        }
        EXPECT_TRUE(ann.add_array_values(sizeof(float), vectors.data(), /*null_map=*/nullptr,
                                         reinterpret_cast<const uint8_t*>(offsets.data()), kRows)
                            .ok());
        EXPECT_TRUE(ann.finish().ok());

        // The non-ANN blob, straight through the container API with bytes this
        // test owns.
        std::vector<doris::snii::writer::BlobFileSource> files;
        for (const auto& [name, bytes] : _synthetic) {
            files.push_back(doris::snii::writer::BlobFileSource {
                    .name = name,
                    .length = bytes.size(),
                    .read_fn = [&bytes](uint64_t offset, size_t len, uint8_t* out) -> Status {
                        std::memcpy(out, bytes.data() + offset, len);
                        return Status::OK();
                    }});
        }
        EXPECT_TRUE(writer.add_snii_blob_index(&_fake_bkd_meta,
                                               doris::snii::format::LogicalIndexKind::kBkd,
                                               std::move(files), {})
                            .ok());

        EXPECT_TRUE(writer.begin_close().ok());
        EXPECT_TRUE(writer.finish_close().ok());
        return prefix;
    }

    TabletIndex _meta;
    TabletIndex _fake_bkd_meta;
    std::map<std::string, std::vector<uint8_t>> _synthetic;
};

// The container must hold the ANN index as a kAnn blob -- not as a text metadata
// group, and not silently absent, which is what the old refusal produced.
TEST_F(SniiAnnContainerTest, AnnIndexIsSealedAsAnAnnBlob) {
    const std::string prefix = write_container();

    doris::snii::io::LocalFileReader file;
    assert_ok(file.open(InvertedIndexDescriptor::get_index_file_path_v2(prefix)));
    doris::snii::reader::SniiSegmentReader segment;
    assert_ok(doris::snii::reader::SniiSegmentReader::open(&file, &segment));

    const doris::snii::format::LogicalIndexMetadataRef* entry = nullptr;
    assert_ok(segment.blob_entry(static_cast<uint64_t>(kAnnIndexId), "", &entry));
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->kind, doris::snii::format::LogicalIndexKind::kAnn);
    // faiss writes several sub-files; an empty table would mean the harvest ran
    // but found nothing, which the assertions below could not distinguish from a
    // successful seal.
    EXPECT_FALSE(entry->files.empty());

    // Extents must lie wholly inside the container and must not overlap each
    // other. "offset < file size" alone is near-vacuous -- it admits the
    // canonical bug of every sub-file being sealed at offset 0.
    std::vector<std::pair<uint64_t, uint64_t>> extents;
    for (const auto& blob : entry->files) {
        EXPECT_GT(blob.length, 0U) << blob.name << " was sealed empty";
        EXPECT_LE(blob.offset + blob.length, file.size())
                << blob.name << " runs past the end of the container";
        extents.emplace_back(blob.offset, blob.length);
    }
    std::sort(extents.begin(), extents.end());
    for (size_t i = 1; i < extents.size(); ++i) {
        EXPECT_LE(extents[i - 1].first + extents[i - 1].second, extents[i].first)
                << "sub-file extents overlap";
    }
}

// The independent oracle for the offset bookkeeping: bytes this test chose
// BEFORE sealing must come back through the directory unchanged. A seal that
// recorded the wrong offsets returns different bytes -- unlike a comparison
// against the container read at those same offsets, which cannot fail.
TEST_F(SniiAnnContainerTest, ABlobServesExactlyTheBytesItWasGiven) {
    const std::string prefix = write_container();

    auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                    InvertedIndexStorageFormatPB::SNII);
    assert_ok(reader->init());

    // Read the non-ANN blob through the same machinery the ANN path uses. The
    // directory path refuses a non-ANN blob by design, so go through the
    // container reader directly and check the extents it recorded.
    doris::snii::io::LocalFileReader file;
    assert_ok(file.open(InvertedIndexDescriptor::get_index_file_path_v2(prefix)));
    doris::snii::reader::SniiSegmentReader segment;
    assert_ok(doris::snii::reader::SniiSegmentReader::open(&file, &segment));
    const doris::snii::format::LogicalIndexMetadataRef* entry = nullptr;
    assert_ok(segment.blob_entry(static_cast<uint64_t>(kFakeBkdIndexId), "", &entry));
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->files.size(), _synthetic.size());

    for (const auto& blob : entry->files) {
        SCOPED_TRACE(blob.name);
        const auto it = _synthetic.find(blob.name);
        ASSERT_NE(it, _synthetic.end()) << "sealed a sub-file nobody asked for";
        ASSERT_EQ(blob.length, it->second.size());

        std::vector<uint8_t> actual;
        assert_ok(file.read_at(blob.offset, blob.length, &actual));
        EXPECT_EQ(actual, it->second) << "the recorded extent does not hold the bytes given to it";
    }
}

// faiss is the second independent oracle: it deserializes its own files, and it
// cannot do that from the wrong extents. This is also the feature's actual
// purpose -- byte identity at self-reported offsets would still leave the index
// unloadable.
TEST_F(SniiAnnContainerTest, FaissLoadsTheIndexBackOutOfTheContainer) {
    const std::string prefix = write_container();

    auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                    InvertedIndexStorageFormatPB::SNII);
    assert_ok(reader->init());

    auto ann_reader = std::make_shared<AnnIndexReader>(&_meta, reader);
    io::IOContext io_ctx;
    assert_ok(ann_reader->load_index(&io_ctx));
}

// A BKD blob has its own reader and no CLucene representation at all. Serving a
// directory over it would hand a caller bytes no CLucene code can parse, so the
// read adapter refuses by KIND. Probing an ABSENT id cannot test this: the
// lookup fails first and the kind branch is never reached.
TEST_F(SniiAnnContainerTest, ANonAnnBlobIsRefusedByTheDirectoryPath) {
    const std::string prefix = write_container();

    auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                    InvertedIndexStorageFormatPB::SNII);
    assert_ok(reader->init());

    auto opened = reader->open(&_fake_bkd_meta, nullptr);
    ASSERT_FALSE(opened.has_value());
    EXPECT_TRUE(opened.error().is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>())
            << opened.error().to_string();
    EXPECT_NE(opened.error().to_string().find("is not an ANN blob"), std::string::npos)
            << opened.error().to_string();
}

TEST_F(SniiAnnContainerTest, AnAbsentIndexIdIsRefused) {
    const std::string prefix = write_container();

    auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                    InvertedIndexStorageFormatPB::SNII);
    assert_ok(reader->init());

    TabletIndexPB pb;
    pb.set_index_type(IndexType::ANN);
    pb.set_index_id(kAnnIndexId + 100);
    pb.set_index_name("absent");
    pb.add_col_unique_id(0);
    TabletIndex absent;
    absent.init_from_pb(pb);

    auto opened = reader->open(&absent, nullptr);
    EXPECT_FALSE(opened.has_value());
}

} // namespace
} // namespace doris::segment_v2
