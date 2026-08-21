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
//   write: the ANN writer had nowhere to write, because SNII opens no CLucene
//          filesystem directory. It now gets a bounded, file-backed staging
//          directory (open_ann_directory) that begin_close() seals into a kAnn
//          blob.
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
#ifdef ADDRESS_SANITIZER
#include <sanitizer/allocator_interface.h>
#endif

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/ann/ann_index_reader.h"
#include "storage/index/ann/ann_index_writer.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_blob_staging_directory.h"
#include "storage/index/snii/staged_file_probe.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {
namespace {

constexpr const char* kTestDir = "./ut_dir/snii_ann_container_test";
constexpr int64_t kAnnIndexId = 71;
// A second logical index in the SAME container, sealed as a NON-ANN blob. It is
// what makes the kind check testable at all: probing an absent index id returns
// at the lookup, long before the kind branch is reached.
constexpr int64_t kFakeBkdIndexId = 72;
constexpr int64_t kIvfOnDiskIndexId = 73;
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

// IVF_ON_DISK is the ONLY ANN configuration that emits more than one sub-file,
// and faiss writes them in the order ann.ivfdata, ann.faiss -- the REVERSE of
// the name order the container must seal them in. It is therefore the only shape
// in which the ordering contract can fail, and the shape the ordering test uses.
TabletIndex make_ivf_on_disk_index_meta() {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::ANN);
    pb.set_index_id(kIvfOnDiskIndexId);
    pb.set_index_name("ann_ivf_on_disk_idx");
    pb.add_col_unique_id(2);
    auto& props = *pb.mutable_properties();
    props["index_type"] = "ivf_on_disk";
    props["metric_type"] = "l2_distance";
    props["dim"] = std::to_string(kDim);
    // nlist drives the training minimum (faiss needs at least one training point
    // per cluster), so it must stay well under kRows.
    props["nlist"] = "4";
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

// Faiss opens its sub-files as ann.faiss / ann.ivfdata, so this tag selects
// exactly the ANN staging and leaves the native BKD staging of other suites
// alone. Observed on the filesystem rather than through the directory's own
// bookkeeping: the bookkeeping is what the retention below disagrees with.
constexpr const char* kAnnStageTag = "ann.";

std::set<std::string> staged_ann_files() {
    return doris::snii_test::snii_staged_files(kAnnStageTag);
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
        _ivf_on_disk_meta = make_ivf_on_disk_index_meta();
        // Two sub-files of different lengths: a directory that bound a name to
        // the wrong entry would then also disagree on fileLength.
        _synthetic["fake_bkd_data"] = synthetic_bytes(0x11, 4096 + 17);
        _synthetic["fake_bkd_index"] = synthetic_bytes(0x77, 1024 + 3);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // Feeds the standard vector set into an ANN writer the CALLER owns. The
    // producer therefore outlives the call -- which is the shape both a failed
    // segment flush and the whole ADD INDEX path leave behind, and the shape
    // finish_ann_index() below cannot express.
    static Status feed_and_finish(AnnIndexColumnWriter* ann) {
        const std::vector<float> vectors = make_vectors();
        std::vector<size_t> offsets(kRows + 1);
        for (uint32_t i = 0; i <= kRows; ++i) {
            offsets[i] = static_cast<size_t>(i) * kDim;
        }
        RETURN_IF_ERROR(ann->add_array_values(sizeof(float), vectors.data(), /*null_map=*/nullptr,
                                              reinterpret_cast<const uint8_t*>(offsets.data()),
                                              kRows));
        return ann->finish();
    }

    // NOLINTNEXTLINE(readability-non-const-parameter): AnnIndexColumnWriter's
    // constructor takes a mutable IndexFileWriter*, so this cannot be const.
    Status finish_ann_index(IndexFileWriter* writer, const TabletIndex* meta) {
        AnnIndexColumnWriter ann(writer, meta);
        RETURN_IF_ERROR(ann.init());
        return feed_and_finish(&ann);
    }

    // Builds one ANN index on `writer` and seals it into the writer's staging
    // area, i.e. everything up to (but not including) begin_close().
    void feed_ann_index(IndexFileWriter* writer, const TabletIndex* meta) {
        assert_ok(finish_ann_index(writer, meta));
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

        feed_ann_index(&writer, &_meta);

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
    TabletIndex _ivf_on_disk_meta;
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

// The ANN staging area under SNII must not be a filesystem directory.
//
// A CLucene filesystem directory is created eagerly and is removed by exactly
// one thing -- an explicit deleteDirectory() on the success path of
// begin_close(). Any earlier return (a failed seal, a failed finish) leaves it
// on disk until the next BE restart wipes the whole tmp dir. Rather than adding
// cleanup to every exit, SNII must not create the directory in the first place.
TEST_F(SniiAnnContainerTest, AnnStagingUnderSniiCreatesNoFilesystemDirectory) {
    const std::string prefix = std::string(kTestDir) + "/no_fs_dir_seg";
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    // can_use_ram_dir=false is the configuration that makes the leak observable:
    // it is what config::inverted_index_ram_dir_enable=false produces in
    // production, and it is what the whole-directory harvest was written for.
    IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ann_no_fs_dir_rowset",
                           /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                           /*can_use_ram_dir=*/false,
                           /*tablet_id=*/9902);
    feed_ann_index(&writer, &_meta);
    ASSERT_FALSE(testing::Test::HasFatalFailure());

    // debug_string() prints every registered directory's toString(). A
    // filesystem directory reports "DorisFSDirectory@<path>" -- and that path is
    // a real directory on disk from the moment it is opened.
    const std::string described = writer.debug_string();
    constexpr const char* kFsMarker = "DorisFSDirectory@";
    const size_t marker = described.find(kFsMarker);
    if (marker != std::string::npos) {
        const std::string leaked = described.substr(marker + std::strlen(kFsMarker));
        std::error_code ec;
        EXPECT_FALSE(std::filesystem::exists(leaked, ec))
                << "an on-disk scratch directory was created for the ANN index: " << leaked;
    }
    EXPECT_EQ(marker, std::string::npos)
            << "SNII staged the ANN index in a filesystem directory: " << described;

    assert_ok(writer.begin_close());
    assert_ok(writer.finish_close());
}

// ANN serialization can be much larger than the final rowset writer's other
// resident state. Staging must therefore retain only a fixed-size write buffer,
// not a second in-memory copy of the complete faiss output.
TEST_F(SniiAnnContainerTest, LargeAnnOutputUsesBoundedHeapForStaging) {
#ifndef ADDRESS_SANITIZER
    GTEST_SKIP() << "heap sampling requires the ASAN allocator interface";
#else
    constexpr size_t kChunkBytes = 64U << 10;
    constexpr size_t kStagedBytes = 64U << 20;
    // The ASAN counter is process-wide, so leave ample room for unrelated
    // background allocations. The old vector staging retained the complete
    // 64 MiB payload and therefore still exceeds this limit by a wide margin.
    constexpr size_t kMaximumHeapGrowth = kStagedBytes / 2;

    snii_doris::SniiBlobStagingDirectory staging;
    std::unique_ptr<lucene::store::IndexOutput> output(staging.createOutput("ann.faiss"));
    std::array<uint8_t, kChunkBytes> chunk {};

    const size_t baseline = __sanitizer_get_current_allocated_bytes();
    size_t peak = baseline;
    for (size_t written = 0; written < kStagedBytes; written += chunk.size()) {
        output->writeBytes(chunk.data(), static_cast<int32_t>(chunk.size()));
        peak = std::max(peak, __sanitizer_get_current_allocated_bytes());
    }
    output->close();
    peak = std::max(peak, __sanitizer_get_current_allocated_bytes());

    EXPECT_EQ(staging.staged_bytes(), kStagedBytes);
    EXPECT_LT(peak - baseline, kMaximumHeapGrowth)
            << "ANN staging retained heap proportional to the serialized index";
#endif
}

// Scoped debug-point switch: enables the fault injection sites for one test and
// restores the process-wide config afterwards.
class ScopedDebugPoints {
public:
    ScopedDebugPoints() : _was_enabled(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->clear();
    }
    ~ScopedDebugPoints() {
        DebugPoints::instance()->clear();
        config::enable_debug_points = _was_enabled;
    }
    void enable(const std::string& name) { DebugPoints::instance()->add(name); }

private:
    const bool _was_enabled;
};

TEST_F(SniiAnnContainerTest, AnnStagingFinalizeFailureIsReturnedFromFinish) {
    ScopedDebugPoints debug_points;
    debug_points.enable("StagedBlobFile::finalize_error");

    const std::string prefix = std::string(kTestDir) + "/finalize_failure_seg";
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ann_finalize_failure",
                           /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                           /*can_use_ram_dir=*/false,
                           /*tablet_id=*/9904);

    Status finish_status = Status::OK();
    ASSERT_NO_THROW({ finish_status = finish_ann_index(&writer, &_meta); });
    EXPECT_FALSE(finish_status.ok());
    EXPECT_NE(finish_status.to_string().find("injected blob staging finalize failure"),
              std::string::npos)
            << finish_status.to_string();
}

TEST_F(SniiAnnContainerTest, FinalBufferedAppendFailureIsReturnedFromFinish) {
    ScopedDebugPoints debug_points;
    debug_points.enable("StagedBlobFile::append_error");

    const std::string prefix = std::string(kTestDir) + "/append_failure_seg";
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ann_append_failure",
                           /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                           /*can_use_ram_dir=*/false,
                           /*tablet_id=*/9905);

    Status finish_status = Status::OK();
    ASSERT_NO_THROW({ finish_status = finish_ann_index(&writer, &_meta); });
    EXPECT_FALSE(finish_status.ok());
    EXPECT_NE(finish_status.to_string().find("injected blob staging append failure"),
              std::string::npos)
            << finish_status.to_string();
}

TEST_F(SniiAnnContainerTest, IvfDataWriteFailuresAreReturnedFromFinish) {
    struct FaultCase {
        const char* debug_point;
        const char* message;
        const char* suffix;
        int64_t tablet_id;
    };
    const std::array faults {
            FaultCase {"StagedBlobFile::append_error", "injected blob staging append failure",
                       "append", 9906},
            FaultCase {"StagedBlobFile::finalize_error", "injected blob staging finalize failure",
                       "finalize", 9907},
    };

    for (const auto& fault : faults) {
        SCOPED_TRACE(fault.debug_point);
        ScopedDebugPoints debug_points;
        debug_points.enable(fault.debug_point);

        const std::string prefix = std::string(kTestDir) + "/ivf_" + fault.suffix + "_failure_seg";
        io::FileWriterPtr file_writer;
        assert_ok(io::global_local_filesystem()->create_file(
                InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
        IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ivf_write_failure",
                               /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                               std::move(file_writer),
                               /*can_use_ram_dir=*/false, fault.tablet_id);

        Status finish_status = Status::OK();
        ASSERT_NO_THROW({ finish_status = finish_ann_index(&writer, &_ivf_on_disk_meta); });
        EXPECT_FALSE(finish_status.ok());
        EXPECT_NE(finish_status.to_string().find("Failed to close IVF data output"),
                  std::string::npos)
                << finish_status.to_string();
        EXPECT_NE(finish_status.to_string().find(fault.message), std::string::npos)
                << finish_status.to_string();
    }
}

// A failed faiss serialization must unlink its staging file THERE, not whenever
// the writers happen to be destroyed. Both owners are still alive at that point
// -- the ANN writer through _dir, the IndexFileWriter through _indices_dirs --
// and neither caller unwinds them: VerticalSegmentWriter::finalize_columns_index()
// returns before clear() and close_inverted_index(), and IndexBuilder's SNII ADD
// INDEX path keeps every producer alive until the whole rowset has been closed.
// So both writers are held here on purpose; the failure tests above scope their
// producer out inside finish_ann_index() and cannot see this.
TEST_F(SniiAnnContainerTest, AFailedSaveUnlinksItsStagingFile) {
    struct Case {
        const char* what;
        const TabletIndex* meta;
        int64_t tablet_id;
    };
    const std::array cases {
            Case {.what = "hnsw", .meta = &_meta, .tablet_id = 9908},
            Case {.what = "ivf_on_disk", .meta = &_ivf_on_disk_meta, .tablet_id = 9909},
    };

    for (const auto& one : cases) {
        SCOPED_TRACE(one.what);
        const std::set<std::string> before = staged_ann_files();
        ScopedDebugPoints debug_points;
        // append(), not the seal: a write is the staging failure production can
        // actually hit (ENOSPC on the scratch volume), and the seal makes no
        // durability call any more.
        debug_points.enable("StagedBlobFile::append_error");

        const std::string prefix = std::string(kTestDir) + "/failed_save_" + one.what + "_seg";
        io::FileWriterPtr file_writer;
        assert_ok(io::global_local_filesystem()->create_file(
                InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
        IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ann_failed_save_rowset",
                               /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                               std::move(file_writer), /*can_use_ram_dir=*/false, one.tablet_id);

        AnnIndexColumnWriter ann(&writer, one.meta);
        assert_ok(ann.init());
        Status finish_status = Status::OK();
        ASSERT_NO_THROW({ finish_status = feed_and_finish(&ann); });
        ASSERT_FALSE(finish_status.ok()) << "the injected fault must fail the save";
        // The specific fault, not just any error: a save that failed for some
        // other reason would not prove anything about the staging file.
        ASSERT_NE(finish_status.to_string().find("injected blob staging append failure"),
                  std::string::npos)
                << finish_status.to_string();

        EXPECT_EQ(staged_ann_files(), before)
                << "the failed save left its staging file on the temp filesystem while the ANN "
                   "writer and the IndexFileWriter are both still alive";
    }
}

// Sealing hands the staged files to the container, so a producer that outlives
// the seal must not keep them pinned. IndexBuilder's SNII ADD INDEX path
// (_handle_single_rowset_snii) is exactly that shape: it builds every segment,
// closes every IndexFileWriter, and only then clears _index_column_writers. If
// the handover does not happen, one whole rowset's ANN staging sits on the temp
// filesystem at once -- which is precisely what SniiCompoundWriter::finish()
// releases per blob to avoid.
TEST_F(SniiAnnContainerTest, SealingDrainsStagedFilesWhileProducersAreStillAlive) {
    struct Segment {
        std::unique_ptr<IndexFileWriter> file_writer;
        std::unique_ptr<AnnIndexColumnWriter> producer;
    };
    constexpr int kSegments = 3;

    const std::set<std::string> before = staged_ann_files();
    std::vector<Segment> segments;
    for (int seg_id = 0; seg_id < kSegments; ++seg_id) {
        const std::string prefix =
                std::string(kTestDir) + "/held_producer_seg" + std::to_string(seg_id);
        io::FileWriterPtr file_writer;
        assert_ok(io::global_local_filesystem()->create_file(
                InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
        Segment segment;
        segment.file_writer = std::make_unique<IndexFileWriter>(
                io::global_local_filesystem(), prefix, "snii_ann_held_producer_rowset", seg_id,
                InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                /*can_use_ram_dir=*/false, /*tablet_id=*/9910);
        segment.producer =
                std::make_unique<AnnIndexColumnWriter>(segment.file_writer.get(), &_meta);
        assert_ok(segment.producer->init());
        Status finish_status = Status::OK();
        ASSERT_NO_THROW({ finish_status = feed_and_finish(segment.producer.get()); });
        assert_ok(finish_status);
        segments.push_back(std::move(segment));
    }
    // Sanity: their absence below must mean the seal drained them, not that they
    // were never staged.
    ASSERT_EQ(staged_ann_files().size(), before.size() + kSegments);

    for (auto& segment : segments) {
        assert_ok(segment.file_writer->begin_close());
    }
    // Every producer is STILL alive here, exactly as it is in IndexBuilder when
    // it runs its begin_close loop.
    EXPECT_EQ(staged_ann_files(), before)
            << "sealing left the staged files pinned by the producers";

    for (auto& segment : segments) {
        assert_ok(segment.file_writer->finish_close());
    }
}

// The same handover inside ONE container: _indices_dirs holds every staging
// directory for the whole of finish(), so the per-blob release in
// SniiCompoundWriter::finish() can only free a sub-file that the directory has
// already given up. Two ANN indexes, three sub-files (hnsw writes ann.faiss,
// ivf_on_disk writes ann.faiss and ann.ivfdata).
TEST_F(SniiAnnContainerTest, EveryAnnIndexInOneContainerIsDrainedBySealing) {
    const std::set<std::string> before = staged_ann_files();
    const std::string prefix = std::string(kTestDir) + "/multi_ann_seg";
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_multi_ann_rowset",
                           /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                           /*can_use_ram_dir=*/false,
                           /*tablet_id=*/9911);

    std::vector<std::unique_ptr<AnnIndexColumnWriter>> producers;
    for (const TabletIndex* meta : {&_meta, &_ivf_on_disk_meta}) {
        auto producer = std::make_unique<AnnIndexColumnWriter>(&writer, meta);
        assert_ok(producer->init());
        Status finish_status = Status::OK();
        ASSERT_NO_THROW({ finish_status = feed_and_finish(producer.get()); });
        assert_ok(finish_status);
        producers.push_back(std::move(producer));
    }
    ASSERT_EQ(staged_ann_files().size(), before.size() + 3);

    assert_ok(writer.begin_close());
    EXPECT_EQ(staged_ann_files(), before)
            << "one container's seal did not drain every ANN index it sealed";
    assert_ok(writer.finish_close());
}

// begin_close() returns Status. Nothing on its SNII path may throw across that
// boundary -- the non-SNII branch of the same function wraps its directory
// teardown in try/catch precisely because DorisFSDirectory::deleteDirectory()
// throws CLuceneError on an I/O failure. The SNII path must not reach a throwing
// call at all.
//
// WHAT THIS IS NOW. The debug point below no longer sits on any SNII code path,
// so on current code this asserts "begin_close() succeeds with an irrelevant
// switch flipped". It is kept as a REGRESSION GUARD: it goes red the moment
// deleteDirectory() -- or any other throwing CLucene teardown -- is reintroduced
// into the SNII close path. It is not evidence about today's behaviour; the
// verbatim CLuceneError it caught before the staging change is.
TEST_F(SniiAnnContainerTest, ADirectoryDeleteFailureCannotThrowOutOfBeginClose) {
    ScopedDebugPoints debug_points;
    debug_points.enable("DorisFSDirectory::deleteDirectory_throw_is_not_directory");

    const std::string prefix = std::string(kTestDir) + "/no_throw_seg";
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ann_no_throw_rowset",
                           /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                           /*can_use_ram_dir=*/false,
                           /*tablet_id=*/9903);
    feed_ann_index(&writer, &_meta);
    ASSERT_FALSE(testing::Test::HasFatalFailure());

    Status begin_status = Status::InternalError("begin_close did not run");
    ASSERT_NO_THROW({ begin_status = writer.begin_close(); });
    EXPECT_TRUE(begin_status.ok()) << begin_status.to_string();
    Status finish_status = Status::InternalError("finish_close did not run");
    ASSERT_NO_THROW({ finish_status = writer.finish_close(); });
    EXPECT_TRUE(finish_status.ok()) << finish_status.to_string();
}

// The staged bytes must survive the trip through the container unchanged, and
// the extents must be the ones the reader is later pointed at.
//
// ORACLE. Comparing the directory against the container at the directory's own
// offsets proves nothing (see the header note). Two independent checks instead:
// the sub-file must start with the fourcc faiss stamps on every serialized
// index -- four printable ASCII bytes that container padding, a zeroed extent,
// or an off-by-N offset cannot produce -- and the same container written twice
// from the same input must be byte-for-byte equal.
TEST_F(SniiAnnContainerTest, AnnBlobBytesRoundTripThroughTheProductionReader) {
    const std::string prefix = write_container();

    auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                    InvertedIndexStorageFormatPB::SNII);
    assert_ok(reader->init());
    auto opened = reader->open(&_meta, nullptr);
    ASSERT_TRUE(opened.has_value()) << opened.error().to_string();
    auto& dir = opened.value();

    std::vector<std::string> names;
    ASSERT_TRUE(dir->list(&names));
    ASSERT_FALSE(names.empty());

    doris::snii::io::LocalFileReader file;
    assert_ok(file.open(InvertedIndexDescriptor::get_index_file_path_v2(prefix)));
    doris::snii::reader::SniiSegmentReader segment;
    assert_ok(doris::snii::reader::SniiSegmentReader::open(&file, &segment));
    const doris::snii::format::LogicalIndexMetadataRef* entry = nullptr;
    assert_ok(segment.blob_entry(static_cast<uint64_t>(kAnnIndexId), "", &entry));
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->files.size(), names.size());

    for (const auto& blob : entry->files) {
        SCOPED_TRACE(blob.name);
        ASSERT_GE(blob.length, 4U);
        std::vector<uint8_t> from_container;
        assert_ok(file.read_at(blob.offset, blob.length, &from_container));

        // faiss stamps a printable four-character fourcc at the head of every
        // file it serializes; the ivfdata side is raw, so only ann.faiss is
        // checked against it.
        if (blob.name == std::string(faiss_index_fila_name)) {
            for (size_t i = 0; i < 4; ++i) {
                EXPECT_GE(from_container[i], 0x20)
                        << "byte " << i << " of the sealed faiss index is not a fourcc character";
                EXPECT_LT(from_container[i], 0x7F)
                        << "byte " << i << " of the sealed faiss index is not a fourcc character";
            }
        }

        // The production read path must serve exactly those bytes.
        lucene::store::IndexInput* raw = nullptr;
        CLuceneError err;
        ASSERT_TRUE(dir->openInput(blob.name.c_str(), raw, err)) << err.what();
        std::unique_ptr<lucene::store::IndexInput> input(raw);
        ASSERT_EQ(static_cast<uint64_t>(input->length()), blob.length);
        std::vector<uint8_t> from_reader(blob.length);
        input->readBytes(from_reader.data(), static_cast<int32_t>(blob.length));
        EXPECT_EQ(from_reader, from_container);
    }
}

// The load-bearing layout invariant, asserted directly rather than inferred from
// a byte comparison: a blob's sub-files are sealed in ascending name order.
//
// This needs IVF_ON_DISK. A HNSW index emits ONE sub-file, so ordering cannot be
// wrong there; IVF_ON_DISK emits ann.ivfdata BEFORE ann.faiss, i.e. producer
// order is the reverse of name order, and only the staging container's ordering
// puts it right. Swapping that container for an unordered one would leave every
// same-process byte comparison green and break exactly this.
TEST_F(SniiAnnContainerTest, IvfOnDiskSubFilesAreSealedInAscendingNameOrder) {
    const std::string prefix = std::string(kTestDir) + "/ivf_seg";
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ann_ivf_rowset",
                           /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                           /*can_use_ram_dir=*/false,
                           /*tablet_id=*/9904);
    feed_ann_index(&writer, &_ivf_on_disk_meta);
    ASSERT_FALSE(testing::Test::HasFatalFailure());
    assert_ok(writer.begin_close());
    assert_ok(writer.finish_close());

    doris::snii::io::LocalFileReader file;
    assert_ok(file.open(InvertedIndexDescriptor::get_index_file_path_v2(prefix)));
    doris::snii::reader::SniiSegmentReader segment;
    assert_ok(doris::snii::reader::SniiSegmentReader::open(&file, &segment));
    const doris::snii::format::LogicalIndexMetadataRef* entry = nullptr;
    assert_ok(segment.blob_entry(static_cast<uint64_t>(kIvfOnDiskIndexId), "", &entry));
    ASSERT_NE(entry, nullptr);

    std::vector<std::string> sealed;
    for (const auto& blob : entry->files) {
        sealed.push_back(blob.name);
    }
    // Both sub-files must be there, or the ordering assertion below is vacuous.
    ASSERT_EQ(sealed.size(), 2U) << "IVF_ON_DISK must emit ann.faiss and ann.ivfdata";
    EXPECT_TRUE(std::is_sorted(sealed.begin(), sealed.end()))
            << "sub-files were sealed in producer order, not name order: " << sealed[0] << ", "
            << sealed[1];

    // Producer order really is the reverse, so the assertion above has teeth.
    ASSERT_LT(std::string(faiss_index_fila_name), std::string(faiss_ivfdata_file_name));
}

// The Defer on begin_close()'s SNII path must release the staging directories on
// the FAILURE path too, not only after a successful finish().
//
// The failure is forced by registering a blob under the ANN index's own key
// before closing: the compound writer rejects a duplicate key, so the seal fails
// while the staged buffers are still held.
TEST_F(SniiAnnContainerTest, AFailedSealStillReleasesTheStagingDirectories) {
    const std::string prefix = std::string(kTestDir) + "/failed_seal_seg";
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter writer(io::global_local_filesystem(), prefix, "snii_ann_failed_seal_rowset",
                           /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII, std::move(file_writer),
                           /*can_use_ram_dir=*/false,
                           /*tablet_id=*/9905);
    feed_ann_index(&writer, &_meta);
    ASSERT_FALSE(testing::Test::HasFatalFailure());

    std::vector<uint8_t> payload = synthetic_bytes(0x5A, 128);
    std::vector<doris::snii::writer::BlobFileSource> collide;
    collide.push_back(doris::snii::writer::BlobFileSource {
            .name = "collide",
            .length = payload.size(),
            .read_fn = [&payload](uint64_t offset, size_t len, uint8_t* out) -> Status {
                std::memcpy(out, payload.data() + offset, len);
                return Status::OK();
            }});
    assert_ok(writer.add_snii_blob_index(&_meta, doris::snii::format::LogicalIndexKind::kBkd,
                                         std::move(collide), {}));

    // Sanity: the staging directory is registered right now, so its absence
    // afterwards means the release ran and not that it was never there.
    ASSERT_NE(writer.debug_string().find("index id is: "), std::string::npos);

    Status begin_status = Status::OK();
    ASSERT_NO_THROW({ begin_status = writer.begin_close(); });
    ASSERT_FALSE(begin_status.ok()) << "the duplicate blob key should have failed the seal";

    EXPECT_EQ(writer.debug_string().find("index id is: "), std::string::npos)
            << "a failed seal left the staging directories registered: " << writer.debug_string();
}

// Determinism: two builds of the same input in the same process must produce the
// same container.
//
// SCOPE, deliberately narrow. This compares this code against ITSELF, so it can
// never show that the layout still matches an older one -- that comparison was
// made once, by hand, against a container built before the staging change, and
// no in-process test can repeat it. It is also blind to the ordering contract:
// an unordered file container would give both builds the same iteration order
// and stay green. IvfOnDiskSubFilesAreSealedInAscendingNameOrder is what covers
// ordering; this test covers only "nothing here is nondeterministic".
TEST_F(SniiAnnContainerTest, TwoContainersBuiltFromTheSameInputAreDeterministic) {
    auto slurp = [](const std::string& path, std::vector<uint8_t>* out) {
        doris::snii::io::LocalFileReader file;
        assert_ok(file.open(path));
        assert_ok(file.read_at(0, file.size(), out));
    };

    const std::string first = write_container();
    std::vector<uint8_t> first_bytes;
    slurp(InvertedIndexDescriptor::get_index_file_path_v2(first), &first_bytes);
    ASSERT_FALSE(testing::Test::HasFatalFailure());

    // write_container() always uses the same prefix, so the second build has to
    // go somewhere else; move the first result aside instead.
    const std::string kept = std::string(kTestDir) + "/first.idx";
    assert_ok(io::global_local_filesystem()->rename(
            InvertedIndexDescriptor::get_index_file_path_v2(first), kept));

    const std::string second = write_container();
    std::vector<uint8_t> second_bytes;
    slurp(InvertedIndexDescriptor::get_index_file_path_v2(second), &second_bytes);
    ASSERT_FALSE(testing::Test::HasFatalFailure());

    ASSERT_FALSE(first_bytes.empty());
    EXPECT_EQ(first_bytes.size(), second_bytes.size());
    EXPECT_TRUE(first_bytes == second_bytes)
            << "two builds of the same ANN index produced different containers";
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
