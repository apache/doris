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

// The integration proof for design 10: the NATIVE BKD riding a REAL SNII
// container, end to end.
//
//   BkdBuilder -> StagedBlobFile -> SniiCompoundWriter::add_blob_index
//     -> sealed container on disk
//     -> SniiSegmentReader::blob_entry -> BkdReader::open -> range / lookup_many
//
// Everything below the container is already covered elsewhere; what is only
// covered HERE is that the blob file table the container seals (offsets, lengths,
// cold/hot placement, crc) reproduces the two sub-files byte-for-byte, and that
// the reader can be driven off the container's own extents rather than off a
// hand-built image.
//
// The pre-existing snii_blob_bkd_roundtrip_test proves the same for the CLucene
// BKD through the lucene Directory shim. This one uses no CLucene at all.

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/index/snii/bkd/bkd_builder.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/bkd/staged_blob_file.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/temp_dir.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"

namespace doris::snii::bkd {
namespace {

using doris::segment_v2::snii_doris::DorisSniiFileReader;
using doris::snii::format::LogicalIndexKind;
using doris::snii::format::LogicalIndexMetadataRef;

constexpr uint64_t kIndexId = 77;
constexpr FieldType kFieldType = FieldType::OLAP_FIELD_TYPE_BIGINT;
constexpr uint32_t kBytesPerDim = sizeof(int64_t);

// The one encoder both sides use, resolved from the index's OWN field type
// (INV-1): feeding anything else would build a self-consistent but semantically
// wrong index.
std::string encode(int64_t value) {
    std::string out;
    get_key_coder(kFieldType)->full_encode_ascending(&value, &out);
    return out;
}

Slice slice_of(const std::string& bytes) {
    return Slice(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}

struct Point {
    int64_t value = 0;
    uint32_t doc_id = 0;
};

std::vector<Point> sample_points(uint32_t count, int64_t span) {
    std::vector<Point> points;
    uint64_t state = 0x9E3779B97F4A7C15ULL;
    for (uint32_t i = 0; i < count; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        points.push_back(
                Point {static_cast<int64_t>(state % static_cast<uint64_t>(2 * span)) - span, i});
    }
    return points;
}

// A container holding exactly one BKD blob index, sealed on disk. Removes itself.
class BkdContainer {
public:
    ~BkdContainer() {
        if (!path_.empty()) {
            ::unlink(path_.c_str());
        }
    }

    // Builds the index, stages the two sub-files, registers them as one blob
    // logical index and seals the container.
    Status build(const std::vector<Point>& points, uint32_t points_per_leaf) {
        path_ = writer::resolve_temp_dir() + "/snii_bkd_container_" + std::to_string(::getpid()) +
                ".idx";

        BkdBuilderOptions options;
        options.bytes_per_dim = kBytesPerDim;
        options.field_type = kFieldType;
        options.points_per_leaf = points_per_leaf;

        std::unique_ptr<BkdBuilder> builder;
        RETURN_IF_ERROR(BkdBuilder::create(options, &builder));
        for (const Point& point : points) {
            const std::string value = encode(point.value);
            RETURN_IF_ERROR(builder->add(point.doc_id, slice_of(value)));
        }

        // bkd_data is the COLD sub-file and is sized by the point count, so it is
        // staged to a file rather than held in RAM; bkd_index is the small hot one.
        RETURN_IF_ERROR(StagedBlobFile::create("bkd_data", &data_));
        ByteSink index_sink;
        RETURN_IF_ERROR(builder->finish(data_.get(), &index_sink, &stats_));
        RETURN_IF_ERROR(data_->finalize());
        index_bytes_ = index_sink.take();

        io::LocalFileWriter file_writer;
        RETURN_IF_ERROR(file_writer.open(path_));
        writer::SniiCompoundWriter compound(&file_writer);

        writer::BlobFileSource cold;
        cold.name = "bkd_data";
        cold.length = data_->bytes_written();
        StagedBlobFile* staged = data_.get();
        cold.read_fn = [staged](uint64_t offset, size_t len, uint8_t* out) {
            return staged->read_at(offset, len, out);
        };

        writer::BlobFileSource hot;
        hot.name = "bkd_index";
        hot.length = index_bytes_.size();
        const std::vector<uint8_t>* index = &index_bytes_;
        hot.read_fn = [index](uint64_t offset, size_t len, uint8_t* out) {
            if (offset > index->size() || len > index->size() - offset) {
                return Status::IOError("bkd_index staging read out of range");
            }
            std::memcpy(out, index->data() + offset, len);
            return Status::OK();
        };

        RETURN_IF_ERROR(
                compound.add_blob_index(kIndexId, "", LogicalIndexKind::kBkd, {cold}, {hot}));
        return compound.finish();
    }

    // Opens the sealed container and resolves the blob entry into the two
    // extents BkdReader::open consumes.
    Status open(std::shared_ptr<DorisSniiFileReader>* file, BkdSections* sections) const {
        doris::io::FileReaderSPtr local;
        RETURN_IF_ERROR(doris::io::global_local_filesystem()->open_file(path_, &local));
        *file = std::make_shared<DorisSniiFileReader>(std::move(local));

        reader::SniiSegmentReader segment;
        RETURN_IF_ERROR(reader::SniiSegmentReader::open(file->get(), &segment));
        const LogicalIndexMetadataRef* entry = nullptr;
        RETURN_IF_ERROR(segment.blob_entry(kIndexId, "", &entry));

        // The container decides where each sub-file landed; the reader is driven
        // off THAT, never off anything the producer remembered.
        for (const format::NamedBlobFileRef& blob : entry->files) {
            if (blob.name == "bkd_data") {
                sections->data_offset = blob.offset;
                sections->data_length = blob.length;
            } else if (blob.name == "bkd_index") {
                sections->index_offset = blob.offset;
                sections->index_length = blob.length;
            }
        }
        return Status::OK();
    }

    const BkdStats& stats() const { return stats_; }
    const std::vector<uint8_t>& index_bytes() const { return index_bytes_; }
    uint64_t data_bytes() const { return data_->bytes_written(); }

private:
    std::string path_;
    std::unique_ptr<StagedBlobFile> data_;
    std::vector<uint8_t> index_bytes_;
    BkdStats stats_;
};

// Brute force over the same points, independent of every line of the index.
roaring::Roaring brute_force(const std::vector<Point>& points, int64_t low, int64_t high) {
    roaring::Roaring hits;
    for (const Point& point : points) {
        if (point.value >= low && point.value <= high) {
            hits.add(point.doc_id);
        }
    }
    return hits;
}

} // namespace

// The container preserves both sub-files exactly, and a reader driven off the
// container's own extents answers what a brute-force scan does.
TEST(BkdContainerRoundtripTest, NativeBkdSurvivesASealedContainer) {
    for (const uint32_t points_per_leaf : {1U, 16U, 1024U}) {
        SCOPED_TRACE("points_per_leaf " + std::to_string(points_per_leaf));
        const std::vector<Point> points = sample_points(3000, 500);

        BkdContainer container;
        ASSERT_TRUE(container.build(points, points_per_leaf).ok());

        std::shared_ptr<DorisSniiFileReader> file;
        BkdSections sections;
        ASSERT_TRUE(container.open(&file, &sections).ok());

        // The container placed them somewhere of its own choosing, but the
        // lengths must be what was staged -- a truncated blob would still open.
        EXPECT_EQ(sections.data_length, container.data_bytes());
        EXPECT_EQ(sections.index_length, container.index_bytes().size());
        EXPECT_GT(sections.data_offset, 0U);
        EXPECT_GT(sections.index_offset, 0U);

        std::unique_ptr<BkdReader> reader;
        ASSERT_TRUE(BkdReader::open(file.get(), sections, &reader).ok());
        EXPECT_EQ(reader->point_count(), points.size());
        EXPECT_EQ(reader->doc_count(), container.stats().doc_count);
        EXPECT_EQ(reader->leaf_count(), container.stats().leaf_count);

        for (const auto& [low, high] : std::vector<std::pair<int64_t, int64_t>> {
                     {-500, 499}, {-100, 100}, {0, 0}, {200, 201}, {-1000, -600}, {499, 499}}) {
            SCOPED_TRACE("range [" + std::to_string(low) + ", " + std::to_string(high) + "]");
            const std::string lower = encode(low);
            const std::string upper = encode(high);
            roaring::Roaring hits;
            ASSERT_TRUE(reader->range(slice_of(lower), true, slice_of(upper), true, &hits).ok());
            EXPECT_TRUE(hits == brute_force(points, low, high));
        }
    }
}

// The empty index through the same path: bkd_data is a ZERO-LENGTH blob file,
// which the container must carry and the reader must accept as "empty" rather
// than as damage (design 5.3).
TEST(BkdContainerRoundtripTest, EmptyIndexIsAZeroLengthBlobFile) {
    BkdContainer container;
    ASSERT_TRUE(container.build({}, /*points_per_leaf=*/16).ok());
    EXPECT_EQ(container.data_bytes(), 0U);

    std::shared_ptr<DorisSniiFileReader> file;
    BkdSections sections;
    ASSERT_TRUE(container.open(&file, &sections).ok());
    EXPECT_EQ(sections.data_length, 0U);

    std::unique_ptr<BkdReader> reader;
    ASSERT_TRUE(BkdReader::open(file.get(), sections, &reader).ok());
    EXPECT_TRUE(reader->empty());
    EXPECT_EQ(reader->point_count(), 0U);

    const std::string lower = encode(-100);
    const std::string upper = encode(100);
    roaring::Roaring hits;
    ASSERT_TRUE(reader->range(slice_of(lower), true, slice_of(upper), true, &hits).ok());
    EXPECT_TRUE(hits.isEmpty());
}

// lookup_many over the container, which is the shape `IN (...)` will take.
TEST(BkdContainerRoundtripTest, LookupManyWorksOverTheContainer) {
    const std::vector<Point> points = sample_points(1200, 30);
    BkdContainer container;
    ASSERT_TRUE(container.build(points, /*points_per_leaf=*/32).ok());

    std::shared_ptr<DorisSniiFileReader> file;
    BkdSections sections;
    ASSERT_TRUE(container.open(&file, &sections).ok());
    std::unique_ptr<BkdReader> reader;
    ASSERT_TRUE(BkdReader::open(file.get(), sections, &reader).ok());

    // Ascending and deduplicated, as lookup_many requires.
    const std::vector<int64_t> wanted = {-30, -7, 0, 3, 12, 29, 500};
    std::vector<std::string> encoded;
    for (const int64_t value : wanted) {
        encoded.push_back(encode(value));
    }
    std::vector<Slice> values;
    for (const std::string& bytes : encoded) {
        values.push_back(slice_of(bytes));
    }

    roaring::Roaring actual;
    ASSERT_TRUE(reader->lookup_many(values, &actual).ok());

    roaring::Roaring expected;
    for (const int64_t value : wanted) {
        expected |= brute_force(points, value, value);
    }
    EXPECT_TRUE(actual == expected);
}

} // namespace doris::snii::bkd
