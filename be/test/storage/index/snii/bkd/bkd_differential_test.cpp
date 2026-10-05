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

// P1-5, design 12.1: the differential test between the SNII-native BKD and the
// CLucene BKD it replaces. It is the only test that can show the rewrite did not
// change SEMANTICS, because it feeds the SAME points and the SAME query
// intervals to both implementations and demands the same doc ids back.
//
// THREE answers are compared on every query, not two (design 12.2 / R1):
//   brute force   -- a linear scan of the point list, independent of both indexes
//   CLucene BKD   -- the implementation shipping for V1/V2/V3 today
//   native BKD    -- BkdBuilder + BkdReader
// A differential test alone can only prove "same as before"; the brute-force
// oracle is what decides WHO is right when the two disagree, and the old
// implementation has catalogued defects (design 14), so a disagreement is not
// automatically a bug in the new code.
//
// Point values on BOTH sides come from KeyCoder::full_encode_ascending (INV-1):
// unsigned big-endian sortable bytes for the index's own FieldType. CLucene's
// NumericUtils is deliberately NOT used -- Doris production never encodes BKD
// points with it (inverted_index_writer.cpp calls the KeyCoder), so encoding the
// baseline differently would compare something that never ships.

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/bkd/bkd_reader.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/decimal12.h"
#include "core/type_limit.h"
#include "core/uint24.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/snii/bkd/bkd_builder.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/types.h"

namespace doris::snii::bkd {
namespace {

// Byte payloads travel as std::string because that is what
// KeyCoder::full_encode_ascending appends to. Every value of one index has the
// same width, so a memcmp over that width IS the total order the index
// implements (INV-1).
using Bytes = std::string;

struct EncodedPoint {
    uint32_t doc_id = 0;
    Bytes value;
};

// One side of an interval. `present == false` is the unbounded side, which is
// how <, <=, > and >= all reach BkdReader::range as a single call.
struct Bound {
    bool present = false;
    Bytes value;
    bool inclusive = true;
};

struct RangeQuery {
    Bound lower;
    Bound upper;
};

// A point set, the leaf capacity to build it with, and the probe values its
// query intervals are drawn from.
struct Dataset {
    std::string name;
    std::vector<EncodedPoint> points;
    // Encoded probe values, ALL produced by the KeyCoder of the dataset's field
    // type: bounds have to be built exactly the way point values are, or the
    // comparison would run against a different byte order.
    std::vector<Bytes> probes;
    uint32_t points_per_leaf = 16;
};

// ---------------------------------------------------------------------------
// Per-FieldType value generation
// ---------------------------------------------------------------------------

// Produces STORAGE-representation values for a field type. Points and query
// bounds are both generated here and then encoded with the type's KeyCoder, so
// everything downstream is type-agnostic bytes.
template <FieldType FT>
struct ValueFactory {
    using CppType = typename CppTypeTraits<FT>::CppType;

    // A deterministic mapping from a rank to a value, injective wherever the
    // domain allows it. Narrow types (BOOL, TINYINT) wrap, which is a legitimate
    // duplicate-heavy shape rather than a defect of the generator.
    static CppType from_rank(int64_t rank) {
        if constexpr (std::is_same_v<CppType, decimal12_t>) {
            decimal12_t value;
            value.integer = rank;
            // The KeyCoder encodes integer then fraction, both sign-flipped big
            // endian, so a fraction that varies with the rank exercises the
            // second half of the 12-byte value instead of leaving it constant.
            value.fraction = static_cast<int32_t>((rank % 1000) * 1000000);
            return value;
        } else if constexpr (std::is_same_v<CppType, uint24_t>) {
            return uint24_t(static_cast<uint32_t>(rank & 0xFFFFFF));
        } else if constexpr (std::is_same_v<CppType, wide::Int256>) {
            return wide::Int256(rank);
        } else {
            return static_cast<CppType>(rank);
        }
    }

    // The type's extreme values (design 12.1's "type_limit::min/max" row). They
    // encode to the all-0x00 / all-0xFF ends of the sortable byte space, which
    // is where an off-by-one in a bound comparison surfaces.
    static CppType lowest() {
        if constexpr (std::is_same_v<CppType, decimal12_t>) {
            decimal12_t value;
            value.integer = std::numeric_limits<int64_t>::min();
            value.fraction = std::numeric_limits<int32_t>::min();
            return value;
        } else if constexpr (std::is_same_v<CppType, uint24_t>) {
            return uint24_t(uint32_t {0});
        } else {
            return type_limit<CppType>::min();
        }
    }

    static CppType highest() {
        if constexpr (std::is_same_v<CppType, decimal12_t>) {
            decimal12_t value;
            value.integer = std::numeric_limits<int64_t>::max();
            value.fraction = std::numeric_limits<int32_t>::max();
            return value;
        } else if constexpr (std::is_same_v<CppType, uint24_t>) {
            return uint24_t(uint32_t {0xFFFFFF});
        } else {
            return type_limit<CppType>::max();
        }
    }
};

// The single encoder both sides use. The returned width is bytes_per_dim by
// construction, and that is asserted rather than assumed: a KeyCoder whose
// output is not sizeof(CppType) would silently break INV-2 for every case below.
template <FieldType FT>
Bytes encode(const typename CppTypeTraits<FT>::CppType& value) {
    Bytes buf;
    get_key_coder(FT)->full_encode_ascending(&value, &buf);
    EXPECT_EQ(buf.size(), field_type_size(FT));
    return buf;
}

template <FieldType FT>
Bytes encode_rank(int64_t rank) {
    return encode<FT>(ValueFactory<FT>::from_rank(rank));
}

// ---------------------------------------------------------------------------
// Brute force: the oracle
// ---------------------------------------------------------------------------

bool lower_ok(const Bound& bound, const Bytes& value) {
    if (!bound.present) {
        return true;
    }
    const int cmp = std::memcmp(value.data(), bound.value.data(), value.size());
    return bound.inclusive ? cmp >= 0 : cmp > 0;
}

bool upper_ok(const Bound& bound, const Bytes& value) {
    if (!bound.present) {
        return true;
    }
    const int cmp = std::memcmp(value.data(), bound.value.data(), value.size());
    return bound.inclusive ? cmp <= 0 : cmp < 0;
}

bool matches(const RangeQuery& query, const Bytes& value) {
    return lower_ok(query.lower, value) && upper_ok(query.upper, value);
}

roaring::Roaring brute_force(const std::vector<EncodedPoint>& points, const RangeQuery& query) {
    roaring::Roaring hits;
    for (const EncodedPoint& point : points) {
        if (matches(query, point.value)) {
            hits.add(point.doc_id);
        }
    }
    return hits;
}

// ---------------------------------------------------------------------------
// Side A: the SNII-native index
// ---------------------------------------------------------------------------

// Collects the bkd_data bytes the builder appends.
class MemoryFileWriter final : public io::FileWriter {
public:
    Status append(Slice data) override {
        bytes_.insert(bytes_.end(), data.data(), data.data() + data.size());
        return Status::OK();
    }
    Status finalize() override { return Status::OK(); }
    uint64_t bytes_written() const override { return bytes_.size(); }

    const std::vector<uint8_t>& bytes() const { return bytes_; }

private:
    std::vector<uint8_t> bytes_;
};

// A whole container image served by positioned reads.
class MemoryFileReader final : public io::FileReader {
public:
    explicit MemoryFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        out->resize(len);
        return read_into(offset, out->data(), len);
    }

    Status read_into(uint64_t offset, uint8_t* out, size_t out_len) override {
        if (out_len == 0) {
            return Status::OK();
        }
        if (offset > bytes_.size() || out_len > bytes_.size() - offset) {
            return Status::Error<ErrorCode::CORRUPTION, false>("read past EOF");
        }
        std::memcpy(out, bytes_.data() + offset, out_len);
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }

private:
    std::vector<uint8_t> bytes_;
};

// Builds the two sub-files into one image at non-zero offsets -- the shape a
// real SNII container hands the reader -- and keeps the reader alive over it.
class NativeIndex {
public:
    Status build(const Dataset& dataset, FieldType field_type) {
        BkdBuilderOptions options;
        options.bytes_per_dim = static_cast<uint32_t>(field_type_size(field_type));
        options.field_type = field_type;
        options.points_per_leaf = dataset.points_per_leaf;

        std::unique_ptr<BkdBuilder> builder;
        RETURN_IF_ERROR(BkdBuilder::create(options, &builder));
        for (const EncodedPoint& point : dataset.points) {
            RETURN_IF_ERROR(builder->add(point.doc_id, to_slice(point.value)));
        }
        MemoryFileWriter data;
        ByteSink index;
        BkdStats stats;
        RETURN_IF_ERROR(builder->finish(&data, &index, &stats));

        const std::vector<uint8_t> index_bytes = index.take();
        std::vector<uint8_t> image(kLeadingPad, 0xA5);
        BkdSections sections;
        sections.data_offset = image.size();
        sections.data_length = data.bytes().size();
        image.insert(image.end(), data.bytes().begin(), data.bytes().end());
        image.insert(image.end(), kMiddlePad, 0x5A);
        sections.index_offset = image.size();
        sections.index_length = index_bytes.size();
        image.insert(image.end(), index_bytes.begin(), index_bytes.end());
        image.insert(image.end(), kTrailingPad, 0xC3);

        file_ = std::make_unique<MemoryFileReader>(std::move(image));
        return BkdReader::open(file_.get(), sections, &reader_);
    }

    Status range(const RangeQuery& query, roaring::Roaring* hits) const {
        return reader_->range(query.lower.present ? to_slice(query.lower.value) : Slice(),
                              query.lower.inclusive,
                              query.upper.present ? to_slice(query.upper.value) : Slice(),
                              query.upper.inclusive, hits, &scratch_);
    }

    const BkdReader& reader() const { return *reader_; }

private:
    static Slice to_slice(const Bytes& bytes) { return Slice(std::string_view(bytes)); }

    static constexpr size_t kLeadingPad = 13;
    static constexpr size_t kMiddlePad = 5;
    static constexpr size_t kTrailingPad = 3;

    std::unique_ptr<MemoryFileReader> file_;
    std::unique_ptr<BkdReader> reader_;
    mutable BkdQueryScratch scratch_;
};

// ---------------------------------------------------------------------------
// Side B: the CLucene index
// ---------------------------------------------------------------------------

// Answers exactly the interval `matches()` defines, so both sides are asked the
// same question. compare() is the cell/query relation the recursion needs: an
// interval is convex, so a cell is INSIDE precisely when both of its corners
// match, and OUTSIDE precisely when the cell lies entirely below the lower bound
// or entirely above the upper one.
class IntervalVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
public:
    IntervalVisitor(const RangeQuery& query, roaring::Roaring* hits) : query_(query), hits_(hits) {}

    void visit(int docid) override { hits_->add(static_cast<uint32_t>(docid)); }
    void visit(roaring::Roaring& docids) override { *hits_ |= docids; }
    void visit(roaring::Roaring&& docids) override { visit(docids); }
    int visit(int docid, std::vector<uint8_t>& packed) override {
        if (accepts(packed)) {
            hits_->add(static_cast<uint32_t>(docid));
        }
        return 0;
    }
    void visit(std::vector<char>& docids, std::vector<uint8_t>& packed) override {
        if (!accepts(packed)) {
            return;
        }
        auto bitmap = roaring::Roaring::read(docids.data(), false);
        visit(bitmap);
    }
    void visit(roaring::Roaring* docids, std::vector<uint8_t>& packed) override {
        if (accepts(packed)) {
            visit(*docids);
        }
    }
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packed) override {
        if (!accepts(packed)) {
            return;
        }
        int32_t docid = iter->docid_set->nextDoc();
        while (docid != lucene::util::bkd::bkd_docid_set::NO_MORE_DOCS) {
            hits_->add(static_cast<uint32_t>(docid));
            docid = iter->docid_set->nextDoc();
        }
    }

    lucene::util::bkd::relation compare(std::vector<uint8_t>& min_packed,
                                        std::vector<uint8_t>& max_packed) override {
        const Bytes cell_min(reinterpret_cast<const char*>(min_packed.data()), min_packed.size());
        const Bytes cell_max(reinterpret_cast<const char*>(max_packed.data()), max_packed.size());
        if (!lower_ok(query_.lower, cell_max) || !upper_ok(query_.upper, cell_min)) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        }
        if (matches(query_, cell_min) && matches(query_, cell_max)) {
            return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
        }
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    }

    lucene::util::bkd::relation compare_prefix(std::vector<uint8_t>& /*prefix*/) override {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    }

private:
    bool accepts(const std::vector<uint8_t>& packed) const {
        const Bytes value(reinterpret_cast<const char*>(packed.data()), packed.size());
        return matches(query_, value);
    }

    const RangeQuery& query_;
    roaring::Roaring* hits_;
};

using RamDirPtr = std::unique_ptr<lucene::store::RAMDirectory, doris::segment_v2::DirectoryDeleter>;

// Drives the unmodified CLucene writer/reader pair the way
// InvertedIndexColumnWriter does: DIMS == 1, MAX_LEAF_COUNT == 1024,
// total_point_count == INT32_MAX, single_value_per_doc == true,
// max_depth == config::max_depth_in_bkd_tree, and docs_seen_ / max_doc_ pushed
// in from outside right before finish() (design 14 #7 -- reproducing the
// production call sequence is the whole point of a differential baseline).
//
// max_depth is NOT a free parameter here. bkd_writer::finish() overrides
// max_points_in_leaf_node_ with point_count / 2^max_depth whenever that is
// non-zero and then rounds the leaf count up to a power of two, so a smaller
// max_depth can ask for MORE leaves than there are points; the build then walks
// off the end of a point_reader (the `assert(result)` in mark_right_tree fires
// under a debug build, and reads a dangling pointer once asserts are compiled
// out). Production's 32 keeps that override dormant below 2^32 points, and that
// is the configuration this baseline has to reproduce.
class CluceneIndex {
public:
    ~CluceneIndex() {
        reader_.reset();
        if (dir_) {
            dir_->close();
        }
    }

    void build(const Dataset& dataset, uint32_t bytes_per_dim) {
        uint32_t max_doc = 0;
        std::set<uint32_t> distinct_docs;
        for (const EncodedPoint& point : dataset.points) {
            max_doc = std::max(max_doc, point.doc_id + 1);
            distinct_docs.insert(point.doc_id);
        }

        dir_ = RamDirPtr(_CLNEW lucene::store::RAMDirectory());
        auto writer = std::make_shared<lucene::util::bkd::bkd_writer>(
                static_cast<int32_t>(max_doc), /*numDataDims=*/1, /*numIndexDims=*/1,
                static_cast<int32_t>(bytes_per_dim), /*maxPointsInLeafNode=*/1024,
                /*maxMBSortInHeap=*/512.0,
                /*totalPointCount=*/std::numeric_limits<int32_t>::max(),
                /*singleValuePerDoc=*/true, config::max_depth_in_bkd_tree);
        for (const EncodedPoint& point : dataset.points) {
            writer->add(reinterpret_cast<const uint8_t*>(point.value.data()), bytes_per_dim,
                        static_cast<int32_t>(point.doc_id));
        }
        writer->max_doc_ = static_cast<int32_t>(max_doc);
        writer->docs_seen_ = static_cast<uint32_t>(distinct_docs.size());

        std::unique_ptr<lucene::store::IndexOutput> data_out(dir_->createOutput("bkd"));
        std::unique_ptr<lucene::store::IndexOutput> index_out(dir_->createOutput("bkd_index"));
        std::unique_ptr<lucene::store::IndexOutput> meta_out(dir_->createOutput("bkd_meta"));
        const int64_t index_fp = writer->finish(data_out.get(), index_out.get());
        writer->meta_finish(meta_out.get(), index_fp, 0);
        data_out->close();
        index_out->close();
        meta_out->close();

        reader_ = std::make_shared<lucene::util::bkd::bkd_reader>(dir_.get(),
                                                                  /*close_directory=*/false);
        opened_ = reader_->open();
    }

    // false only for the 0-point index: the old reader signals emptiness by
    // refusing to open (design 5.3 replaces that with an index that opens and
    // answers every query with an empty bitmap).
    bool opened() const { return opened_; }

    // Leaves the old writer actually cut. Asserted non-trivial by the test so a
    // future change to the dataset sizes cannot silently reduce the baseline to
    // a single leaf, i.e. to no tree recursion at all.
    int32_t leaf_count() const { return opened_ ? reader_->num_leaves_ : 0; }

    void range(const RangeQuery& query, roaring::Roaring* hits) const {
        *hits = roaring::Roaring();
        if (!opened_) {
            return;
        }
        IntervalVisitor visitor(query, hits);
        reader_->intersect(&visitor);
    }

private:
    RamDirPtr dir_;
    std::shared_ptr<lucene::util::bkd::bkd_reader> reader_;
    bool opened_ = false;
};

// ---------------------------------------------------------------------------
// Datasets and queries
// ---------------------------------------------------------------------------

// A deterministic 64-bit LCG so any failure is reproducible from the seed.
class Rng {
public:
    explicit Rng(uint64_t seed) : state_(seed) {}
    uint64_t next() {
        state_ = state_ * 6364136223846793005ULL + 1442695040888963407ULL;
        return state_ >> 11;
    }
    size_t next_index(size_t bound) { return static_cast<size_t>(next() % bound); }

private:
    uint64_t state_;
};

// The CLucene writer halves its leaf capacity from the point count until it is
// <= 1024, so the baseline only grows a tree above 1024 points: 2049..4096
// points give 4 leaves, 4097..8192 give 8. The "big" datasets sit above that on
// purpose -- below it the baseline is one flat leaf and the comparison would
// never exercise its recursion, its split values or its packed index.
constexpr int64_t kBigPointCount = 5000;

// The covering matrix of design 12.1, minus the type axis (which the typed test
// supplies): empty, single point, all values equal, all values distinct, type
// extremes, an array column repeating one doc id, NULL-dense (only some rows
// carry a point), exactly-full / one-short / one-over leaf multiples, long runs
// straddling leaf boundaries, and unordered values (both single- and
// multi-valued), which is the only shape where a leaf's doc ids are not already
// ascending for free.
template <FieldType FT>
std::vector<Dataset> make_datasets(Rng* rng) {
    std::vector<Dataset> datasets;

    // Probe ranks used by every dataset: below the point set, at both of its
    // ends, at a few interior positions, above it, and the type's extremes.
    const auto probes_for = [](int64_t count) {
        std::vector<Bytes> probes;
        for (const int64_t rank : {int64_t {-7}, int64_t {-1}, int64_t {0}, int64_t {1}, count / 3,
                                   count / 2, count - 2, count - 1, count, count + 5}) {
            probes.push_back(encode_rank<FT>(rank));
        }
        probes.push_back(encode<FT>(ValueFactory<FT>::lowest()));
        probes.push_back(encode<FT>(ValueFactory<FT>::highest()));
        return probes;
    };

    {
        Dataset dataset;
        dataset.name = "empty";
        dataset.probes = probes_for(4);
        dataset.points_per_leaf = 16;
        datasets.push_back(std::move(dataset));
    }
    {
        Dataset dataset;
        dataset.name = "single_point";
        dataset.points.push_back({7, encode_rank<FT>(3)});
        dataset.probes = probes_for(6);
        dataset.points_per_leaf = 16;
        datasets.push_back(std::move(dataset));
    }
    {
        // Every value identical across many leaves: kAllEqual leaves plus a
        // split-value array whose consecutive entries are equal.
        Dataset dataset;
        dataset.name = "all_equal";
        for (int64_t i = 0; i < kBigPointCount; ++i) {
            dataset.points.push_back({static_cast<uint32_t>(i), encode_rank<FT>(42)});
        }
        dataset.probes = probes_for(84);
        dataset.points_per_leaf = 64;
        datasets.push_back(std::move(dataset));
    }
    {
        // Ascending ranks: all distinct for every type wider than one byte, and
        // a wrapped duplicate-heavy set for BOOL / TINYINT, whose whole value
        // domain is 256 wide.
        Dataset dataset;
        dataset.name = "ascending_ranks";
        for (int64_t i = 0; i < kBigPointCount; ++i) {
            dataset.points.push_back({static_cast<uint32_t>(i), encode_rank<FT>(i)});
        }
        dataset.probes = probes_for(kBigPointCount);
        dataset.points_per_leaf = 64;
        datasets.push_back(std::move(dataset));
    }
    {
        // Unordered values with ascending doc ids: inside a leaf the doc ids are
        // NOT monotone, so this is the only shape that really exercises the RAW
        // leaf's doc-id encoding (design 5.2).
        Dataset dataset;
        dataset.name = "unordered_values";
        for (int64_t i = 0; i < kBigPointCount; ++i) {
            dataset.points.push_back(
                    {static_cast<uint32_t>(i),
                     encode_rank<FT>(static_cast<int64_t>(rng->next() % 100000) - 50000)});
        }
        dataset.probes = probes_for(50000);
        dataset.points_per_leaf = 100;
        datasets.push_back(std::move(dataset));
    }
    {
        // Values at the very ends of the sortable byte space, where an
        // inclusive/exclusive mix-up in the global-bounds fast reject surfaces.
        Dataset dataset;
        dataset.name = "type_extremes";
        const Bytes low = encode<FT>(ValueFactory<FT>::lowest());
        const Bytes high = encode<FT>(ValueFactory<FT>::highest());
        uint32_t doc = 0;
        for (uint32_t repeat = 0; repeat < 3; ++repeat) {
            dataset.points.push_back({doc++, low});
        }
        for (const int64_t rank : {int64_t {-5}, int64_t {0}, int64_t {5}}) {
            dataset.points.push_back({doc++, encode_rank<FT>(rank)});
        }
        for (uint32_t repeat = 0; repeat < 3; ++repeat) {
            dataset.points.push_back({doc++, high});
        }
        dataset.probes = probes_for(6);
        dataset.points_per_leaf = 4;
        datasets.push_back(std::move(dataset));
    }
    {
        // Array column: one row contributes several points, consecutively
        // (design 14 #8 -- the old writer claims singleValuePerDoc == true here).
        Dataset dataset;
        dataset.name = "array_column";
        for (uint32_t doc = 0; doc < 1700; ++doc) {
            for (int64_t k = 0; k < 3; ++k) {
                dataset.points.push_back({doc, encode_rank<FT>(static_cast<int64_t>(doc) * 3 + k)});
            }
        }
        dataset.probes = probes_for(5100);
        dataset.points_per_leaf = 128;
        datasets.push_back(std::move(dataset));
    }
    {
        // Array column with unordered values: several points per row AND doc ids
        // that repeat inside a leaf in no value order.
        Dataset dataset;
        dataset.name = "array_column_unordered";
        for (uint32_t doc = 0; doc < 1300; ++doc) {
            const uint32_t arity = 1 + static_cast<uint32_t>(rng->next() % 4);
            for (uint32_t k = 0; k < arity; ++k) {
                dataset.points.push_back(
                        {doc, encode_rank<FT>(static_cast<int64_t>(rng->next() % 4000))});
            }
        }
        dataset.probes = probes_for(4000);
        dataset.points_per_leaf = 100;
        datasets.push_back(std::move(dataset));
    }
    {
        // NULL-dense: NULL rows never reach the builder at all (design D9), so
        // the doc ids that DO carry a point are sparse and non-contiguous.
        Dataset dataset;
        dataset.name = "null_dense";
        for (uint32_t i = 0; i < 2500; ++i) {
            dataset.points.push_back({i * 7 + 3, encode_rank<FT>(i % 37)});
        }
        dataset.probes = probes_for(37);
        dataset.points_per_leaf = 32;
        datasets.push_back(std::move(dataset));
    }
    {
        // NULL-dense inside a HUGE segment: doc ids run past 0xFFFFFF, which is
        // the threshold where the old writer stops packing an unsorted leaf's
        // doc ids into 24 bits and switches to 32 (docids_writer.cpp). Random
        // values are what make the leaves unsorted in the first place, so this
        // is the only dataset that reaches that branch.
        Dataset dataset;
        dataset.name = "sparse_huge_docids";
        for (uint32_t i = 0; i < 5000; ++i) {
            dataset.points.push_back(
                    {i * 4096 + 11, encode_rank<FT>(static_cast<int64_t>(rng->next() % 9000))});
        }
        dataset.probes = probes_for(9000);
        dataset.points_per_leaf = 64;
        datasets.push_back(std::move(dataset));
    }
    {
        // Runs SHORTER than a leaf: every leaf holds many runs, which is the
        // kRle mode of design 5.2 (as opposed to the whole-leaf kAllEqual that
        // runs_across_leaves below produces) and the old writer's
        // low-cardinality leaf encoding.
        Dataset dataset;
        dataset.name = "medium_runs";
        uint32_t doc = 0;
        for (int64_t value = 0; value < 1000; ++value) {
            for (uint32_t repeat = 0; repeat < 5; ++repeat) {
                dataset.points.push_back({doc++, encode_rank<FT>(value)});
            }
        }
        dataset.probes = probes_for(1000);
        dataset.points_per_leaf = 128;
        datasets.push_back(std::move(dataset));
    }
    for (const int64_t delta : {int64_t {0}, int64_t {-1}, int64_t {1}}) {
        // Exactly N full leaves, one point short of that, and one point over it:
        // the boundary between "the last leaf is full" and "the last leaf holds
        // a single point". 4 * 512 also straddles the old writer's own leaf
        // rounding, so both sides are cutting leaves here.
        Dataset dataset;
        dataset.points_per_leaf = 512;
        const int64_t count = 4 * dataset.points_per_leaf + delta;
        dataset.name = delta == 0  ? "exact_leaf_multiple"
                       : delta < 0 ? "one_short_of_leaf_multiple"
                                   : "one_over_leaf_multiple";
        for (int64_t i = 0; i < count; ++i) {
            dataset.points.push_back({static_cast<uint32_t>(i), encode_rank<FT>(i)});
        }
        dataset.probes = probes_for(count);
        datasets.push_back(std::move(dataset));
    }
    {
        // Long runs of one value crossing leaf boundaries: kRle leaves, and the
        // case where the split value equals the value on both sides of the cut.
        Dataset dataset;
        dataset.name = "runs_across_leaves";
        uint32_t doc = 0;
        for (int64_t value = 0; value < 9; ++value) {
            for (uint32_t repeat = 0; repeat < 600; ++repeat) {
                dataset.points.push_back({doc++, encode_rank<FT>(value)});
            }
        }
        dataset.probes = probes_for(9);
        dataset.points_per_leaf = 128;
        datasets.push_back(std::move(dataset));
    }
    return datasets;
}

// Every combination worth asking of a probe set: both sides unbounded, each side
// alone (inclusive and exclusive), the degenerate single-value interval, closed
// / half-open / open intervals between adjacent probes, and random probe pairs
// -- including reversed ones, which are the legal-but-empty intervals.
std::vector<RangeQuery> make_queries(const std::vector<Bytes>& probes, Rng* rng) {
    std::vector<RangeQuery> queries;
    queries.push_back(RangeQuery {});

    for (const Bytes& probe : probes) {
        for (const bool inclusive : {true, false}) {
            RangeQuery lower_only;
            lower_only.lower = Bound {true, probe, inclusive};
            queries.push_back(lower_only);

            RangeQuery upper_only;
            upper_only.upper = Bound {true, probe, inclusive};
            queries.push_back(upper_only);

            // [v, v] must return exactly the points equal to v, (v, v) none.
            RangeQuery point_query;
            point_query.lower = Bound {true, probe, inclusive};
            point_query.upper = Bound {true, probe, inclusive};
            queries.push_back(point_query);
        }
    }

    for (size_t i = 0; i + 1 < probes.size(); ++i) {
        for (const bool lower_inclusive : {true, false}) {
            for (const bool upper_inclusive : {true, false}) {
                RangeQuery query;
                query.lower = Bound {true, probes[i], lower_inclusive};
                query.upper = Bound {true, probes[i + 1], upper_inclusive};
                queries.push_back(query);
            }
        }
    }

    for (int i = 0; i < 16; ++i) {
        RangeQuery query;
        query.lower = Bound {true, probes[rng->next_index(probes.size())], (i % 2) == 0};
        query.upper = Bound {true, probes[rng->next_index(probes.size())], (i % 3) == 0};
        queries.push_back(query);
    }
    return queries;
}

std::string to_hex(const Bytes& bytes) {
    static const char* kDigits = "0123456789abcdef";
    std::string out;
    for (const char raw : bytes) {
        const auto byte = static_cast<uint8_t>(raw);
        out.push_back(kDigits[byte >> 4]);
        out.push_back(kDigits[byte & 0x0F]);
    }
    return out;
}

std::string describe(const RangeQuery& query) {
    std::string text = query.lower.present
                               ? (query.lower.inclusive ? "[" : "(") + to_hex(query.lower.value)
                               : std::string("(-inf");
    text += ", ";
    text += query.upper.present ? to_hex(query.upper.value) + (query.upper.inclusive ? "]" : ")")
                                : std::string("+inf)");
    return text;
}

// Bitmap equality with a diff a human can act on: the cardinalities plus the
// first few doc ids each side has and the other does not.
::testing::AssertionResult same_hits(const roaring::Roaring& expected,
                                     const roaring::Roaring& actual) {
    if (expected == actual) {
        return ::testing::AssertionSuccess();
    }
    roaring::Roaring missing = expected - actual;
    roaring::Roaring extra = actual - expected;
    std::string text = "expected " + std::to_string(expected.cardinality()) + " docs, got " +
                       std::to_string(actual.cardinality()) + "; missing " +
                       std::to_string(missing.cardinality()) + " extra " +
                       std::to_string(extra.cardinality()) + "; first missing:";
    int printed = 0;
    for (const uint32_t doc : missing) {
        text += " " + std::to_string(doc);
        if (++printed == 8) {
            break;
        }
    }
    text += "; first extra:";
    printed = 0;
    for (const uint32_t doc : extra) {
        text += " " + std::to_string(doc);
        if (++printed == 8) {
            break;
        }
    }
    return ::testing::AssertionFailure() << text;
}

uint32_t distinct_doc_count(const std::vector<EncodedPoint>& points) {
    std::set<uint32_t> docs;
    for (const EncodedPoint& point : points) {
        docs.insert(point.doc_id);
    }
    return static_cast<uint32_t>(docs.size());
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

template <FieldType FT>
struct FieldTag {
    static constexpr FieldType kType = FT;
};

template <typename Tag>
class BkdDifferentialTest : public ::testing::Test {};

// Every field type a native BKD index can be built for -- exactly the list
// bkd_index_block.cpp accepts, i.e. every non-string instantiation of
// InvertedIndexColumnWriter.
using AllFieldTypes = ::testing::Types<
        FieldTag<FieldType::OLAP_FIELD_TYPE_BOOL>, FieldTag<FieldType::OLAP_FIELD_TYPE_TINYINT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_SMALLINT>, FieldTag<FieldType::OLAP_FIELD_TYPE_INT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_BIGINT>, FieldTag<FieldType::OLAP_FIELD_TYPE_LARGEINT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_FLOAT>, FieldTag<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL32>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL64>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL256>, FieldTag<FieldType::OLAP_FIELD_TYPE_DATE>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DATETIME>, FieldTag<FieldType::OLAP_FIELD_TYPE_DATEV2>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>, FieldTag<FieldType::OLAP_FIELD_TYPE_IPV4>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_IPV6>>;

TYPED_TEST_SUITE(BkdDifferentialTest, AllFieldTypes);

TYPED_TEST(BkdDifferentialTest, SameAnswersAsCluceneAndAsBruteForce) {
    constexpr FieldType kFieldType = TypeParam::kType;
    const auto bytes_per_dim = static_cast<uint32_t>(field_type_size(kFieldType));

    Rng rng(0x5EED0000ULL + static_cast<uint64_t>(kFieldType));
    for (const Dataset& dataset : make_datasets<kFieldType>(&rng)) {
        SCOPED_TRACE("field_type=" + std::to_string(static_cast<int>(kFieldType)) +
                     " bytes_per_dim=" + std::to_string(bytes_per_dim) + " dataset=" +
                     dataset.name + " points=" + std::to_string(dataset.points.size()));

        NativeIndex native;
        ASSERT_TRUE(native.build(dataset, kFieldType).ok());

        CluceneIndex clucene;
        clucene.build(dataset, bytes_per_dim);

        // Documented API difference (design 5.3): the old reader signals "empty"
        // by refusing to open; the new one opens and answers with empty bitmaps.
        EXPECT_EQ(clucene.opened(), !dataset.points.empty());
        EXPECT_EQ(native.reader().empty(), dataset.points.empty());
        EXPECT_EQ(native.reader().point_count(), dataset.points.size());
        // doc_count is counted inside the builder now (design 6.1 / 14 #7)
        // instead of being pushed in from outside, and an array column repeating
        // one doc id must not inflate it.
        EXPECT_EQ(native.reader().doc_count(), distinct_doc_count(dataset.points));
        // Above 1024 points the baseline must really be a tree; if a future edit
        // shrinks these datasets the comparison would quietly degrade to a
        // single flat leaf on the CLucene side.
        if (dataset.points.size() > 1024) {
            EXPECT_GT(clucene.leaf_count(), 1);
        }

        roaring::Roaring native_hits;
        roaring::Roaring clucene_hits;
        for (const RangeQuery& query : make_queries(dataset.probes, &rng)) {
            ASSERT_TRUE(native.range(query, &native_hits).ok()) << describe(query);
            clucene.range(query, &clucene_hits);
            const roaring::Roaring truth = brute_force(dataset.points, query);

            EXPECT_TRUE(same_hits(truth, native_hits))
                    << "native vs brute force on " << describe(query);
            EXPECT_TRUE(same_hits(truth, clucene_hits))
                    << "CLucene vs brute force on " << describe(query);
            EXPECT_TRUE(same_hits(clucene_hits, native_hits))
                    << "native vs CLucene on " << describe(query);
            if (::testing::Test::HasFailure()) {
                return;
            }
        }
    }
}

} // namespace
} // namespace doris::snii::bkd
