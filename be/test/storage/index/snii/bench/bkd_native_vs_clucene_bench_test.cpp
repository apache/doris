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

// The SNII-native BKD against the CLucene BKD it replaces (design 12 / task P4-1).
//
// WHY THIS EXISTS: design 11's comparison table is a STRUCTURAL argument, not a
// measurement, and the design says so -- no performance claim from it belongs in
// a PR description until this runs. One entry in that table is explicitly a
// REVERSIBLE decision awaiting evidence: the leaf layout puts values first and
// records a docid_block_offset, betting that one extra offset parse on a
// whole-leaf hit costs less than the old layout's skip-read on a boundary leaf.
// If the bet is wrong the layout should be flipped back. The two range cases
// below are shaped to answer exactly that:
//
//   range_wide   - spans many leaves, so almost every leaf is a WHOLE-leaf hit
//                  and the offset-parse cost dominates.
//   range_narrow - touches one or two leaves, both BOUNDARY leaves, so the
//                  value-scan-without-skip path dominates.
//
// A native win on both vindicates the layout. A native loss on range_wide with a
// win on range_narrow is the signal to flip it.
//
// Why CPU time is the headline: this machine is shared and hybrid-core, so wall
// clock moves with whatever else runs. Process CPU time barely does. Wall is
// still reported -- a large wall/CPU gap means the run was descheduled and
// should be repeated.
//
// Pin to a performance core; on a hybrid CPU an E-core sample is not comparable
// to a P-core one and mixing them silently widens every percentile:
//
//   taskset -c 4 env SNII_BKD_BENCH_POINTS=2000000 SNII_BKD_BENCH_ITERATIONS=30 \
//     ./run-be-ut.sh --run --filter='*BkdNativeVsClucene*' -j 28
//
// Both indexes are built from the SAME encoded points in the SAME process, so
// the comparison isolates the implementation and not the data or the machine.
//
// DISABLED_ so CI never runs it; the filter above opts in.

#include <CLucene.h>
#include <CLucene/util/bkd/bkd_reader.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <gtest/gtest.h>
#include <time.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "common/check.h"
#include "common/config.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/snii/bkd/bkd_builder.h"
#include "storage/index/snii/bkd/bkd_index_block.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/bkd/leaf_codec.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "util/time.h"

namespace doris::snii::bkd {
namespace {

constexpr FieldType kFieldType = FieldType::OLAP_FIELD_TYPE_BIGINT;
constexpr uint32_t kBytesPerDim = sizeof(int64_t);
constexpr uint32_t kDefaultPoints = 2000000;
constexpr int kDefaultIterations = 30;

struct Measurement {
    double wall_s = 0;
    double cpu_s = 0;
};

Measurement measure(const std::function<void()>& body) {
    timespec cpu_start {};
    timespec cpu_end {};
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
    const int64_t wall_start = MonotonicNanos();
    body();
    const int64_t wall_end = MonotonicNanos();
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_end);
    Measurement m;
    m.wall_s = static_cast<double>(wall_end - wall_start) / 1e9;
    m.cpu_s = static_cast<double>(cpu_end.tv_sec - cpu_start.tv_sec) +
              static_cast<double>(cpu_end.tv_nsec - cpu_start.tv_nsec) / 1e9;
    return m;
}

// Nearest-rank, matching the SNII/V3 benchmark so the two report the same statistic.
double nearest_rank_percentile(const std::vector<double>& sorted, size_t percentile) {
    DORIS_CHECK(!sorted.empty());
    DORIS_CHECK(percentile > 0 && percentile <= 100);
    const size_t whole_hundreds = sorted.size() / 100;
    const size_t remainder = sorted.size() % 100;
    const size_t rank = whole_hundreds * percentile + (remainder * percentile + 99) / 100;
    return sorted[rank - 1];
}

double mean_of(const std::vector<double>& xs) {
    return std::accumulate(xs.begin(), xs.end(), 0.0) / static_cast<double>(xs.size());
}

double stddev_of(const std::vector<double>& xs) {
    if (xs.size() < 2) {
        return 0.0;
    }
    const double m = mean_of(xs);
    double acc = 0;
    for (const double x : xs) {
        acc += (x - m) * (x - m);
    }
    return std::sqrt(acc / static_cast<double>(xs.size() - 1));
}

int env_int(const char* name, int fallback) {
    const char* const raw = std::getenv(name);
    if (raw == nullptr) {
        return fallback;
    }
    const int value = std::atoi(raw);
    return value > 0 ? value : fallback;
}

std::string encode(int64_t value) {
    std::string out;
    get_key_coder(kFieldType)->full_encode_ascending(&value, &out);
    return out;
}

Slice slice_of(const std::string& bytes) {
    return Slice(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}

struct EncodedPoint {
    std::string value;
    int64_t raw = 0;
    uint32_t doc_id = 0;
};

// A skewed-but-not-degenerate key distribution: a wide numeric domain with
// duplicates, which is what a real id / timestamp / metric column looks like.
// A uniform permutation would make every leaf equally selective and hide the
// boundary-leaf behaviour the layout question is about.
std::vector<EncodedPoint> make_points(uint32_t count, int64_t span) {
    std::vector<EncodedPoint> points;
    points.reserve(count);
    uint64_t state = 0x9E3779B97F4A7C15ULL;
    for (uint32_t i = 0; i < count; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const int64_t raw = static_cast<int64_t>(state % static_cast<uint64_t>(2 * span)) - span;
        points.push_back(EncodedPoint {encode(raw), raw, i});
    }
    return points;
}

// Collects bkd_data in memory so the measurement is CPU, not page cache.
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

// ---------------------------------------------------------------------------
// The CLucene baseline, driven exactly as InvertedIndexColumnWriter drives it
// (DIMS 1, MAX_LEAF_COUNT 1024, total_point_count INT32_MAX,
// single_value_per_doc true, docs_seen_/max_doc_ pushed in before finish).
// Reproducing the production call sequence is the whole point of a baseline.
// ---------------------------------------------------------------------------
using RamDirPtr = std::unique_ptr<lucene::store::RAMDirectory, doris::segment_v2::DirectoryDeleter>;

class CluceneBkd {
public:
    ~CluceneBkd() {
        reader_.reset();
        if (dir_) {
            dir_->close();
        }
    }

    void build(const std::vector<EncodedPoint>& points) {
        uint32_t max_doc = 0;
        std::set<uint32_t> distinct;
        for (const EncodedPoint& p : points) {
            max_doc = std::max(max_doc, p.doc_id + 1);
            distinct.insert(p.doc_id);
        }
        dir_ = RamDirPtr(_CLNEW lucene::store::RAMDirectory());
        auto writer = std::make_shared<lucene::util::bkd::bkd_writer>(
                static_cast<int32_t>(max_doc), 1, 1, static_cast<int32_t>(kBytesPerDim),
                /*maxPointsInLeafNode=*/1024, /*maxMBSortInHeap=*/512.0,
                /*totalPointCount=*/std::numeric_limits<int32_t>::max(),
                /*singleValuePerDoc=*/true, config::max_depth_in_bkd_tree);
        for (const EncodedPoint& p : points) {
            writer->add(reinterpret_cast<const uint8_t*>(p.value.data()), kBytesPerDim,
                        static_cast<int32_t>(p.doc_id));
        }
        writer->max_doc_ = static_cast<int32_t>(max_doc);
        writer->docs_seen_ = static_cast<uint32_t>(distinct.size());

        std::unique_ptr<lucene::store::IndexOutput> data_out(dir_->createOutput("bkd"));
        std::unique_ptr<lucene::store::IndexOutput> index_out(dir_->createOutput("bkd_index"));
        std::unique_ptr<lucene::store::IndexOutput> meta_out(dir_->createOutput("bkd_meta"));
        const int64_t index_fp = writer->finish(data_out.get(), index_out.get());
        writer->meta_finish(meta_out.get(), index_fp, 0);
        bytes_ = data_out->getFilePointer() + index_out->getFilePointer() +
                 meta_out->getFilePointer();
        data_out->close();
        index_out->close();
        meta_out->close();

        reader_ = std::make_shared<lucene::util::bkd::bkd_reader>(dir_.get(), false);
        DORIS_CHECK(reader_->open());
    }

    uint64_t bytes() const { return static_cast<uint64_t>(bytes_); }
    lucene::util::bkd::bkd_reader* reader() const { return reader_.get(); }

private:
    RamDirPtr dir_;
    std::shared_ptr<lucene::util::bkd::bkd_reader> reader_;
    int64_t bytes_ = 0;
};

// The visitor shape InvertedIndexVisitor uses: a closed [min, max] box with the
// strictness folded into the bounds, which is the only interval the old reader
// can express.
class RangeVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
public:
    RangeVisitor(std::string low, std::string high, roaring::Roaring* hits)
            : low_(std::move(low)), high_(std::move(high)), hits_(hits) {}

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
        if (cmp(max_packed, low_) < 0 || cmp(min_packed, high_) > 0) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        }
        if (cmp(min_packed, low_) >= 0 && cmp(max_packed, high_) <= 0) {
            return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
        }
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    }

    lucene::util::bkd::relation compare_prefix(std::vector<uint8_t>&) override {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    }

private:
    static int cmp(const std::vector<uint8_t>& packed, const std::string& bound) {
        return std::memcmp(packed.data(), bound.data(), kBytesPerDim);
    }
    bool accepts(const std::vector<uint8_t>& packed) const {
        return cmp(packed, low_) >= 0 && cmp(packed, high_) <= 0;
    }
    std::string low_;
    std::string high_;
    roaring::Roaring* hits_;
};

struct QueryCase {
    const char* label;
    int64_t low;
    int64_t high;
};

// Reports one metric's spread. stddev is printed alongside the percentiles
// because a p50 with a stddev of its own magnitude is not a measurement.
void report(const char* format, const char* label, std::vector<double> cpu,
            std::vector<double> wall) {
    std::sort(cpu.begin(), cpu.end());
    std::sort(wall.begin(), wall.end());
    printf("  %-8s %-14s cpu p50=%9.3f ms  p99=%9.3f ms  mean=%9.3f ms  sd=%8.3f ms | "
           "wall p50=%9.3f ms\n",
           format, label, nearest_rank_percentile(cpu, 50) * 1e3,
           nearest_rank_percentile(cpu, 99) * 1e3, mean_of(cpu) * 1e3, stddev_of(cpu) * 1e3,
           nearest_rank_percentile(wall, 50) * 1e3);
}

// Serves the two sub-files from one contiguous buffer, exactly as the container
// lays them out, so query cost is CPU and not storage.
class ConcatReader final : public io::FileReader {
public:
    explicit ConcatReader(const std::vector<uint8_t>* bytes) : bytes_(bytes) {}
    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        out->resize(len);
        return read_into(offset, out->data(), len);
    }
    Status read_into(uint64_t offset, uint8_t* out, size_t len) override {
        DORIS_CHECK(offset + len <= bytes_->size());
        std::memcpy(out, bytes_->data() + offset, len);
        return Status::OK();
    }
    uint64_t size() const override { return bytes_->size(); }

private:
    const std::vector<uint8_t>* bytes_;
};

class BkdNativeVsCluceneBench : public ::testing::Test {};

// ---------------------------------------------------------------------------

TEST_F(BkdNativeVsCluceneBench, DISABLED_BuildAndQuery) {
    const uint32_t point_count =
            static_cast<uint32_t>(env_int("SNII_BKD_BENCH_POINTS", kDefaultPoints));
    const int iterations = env_int("SNII_BKD_BENCH_ITERATIONS", kDefaultIterations);
    const int64_t span = 1 << 20;

    printf("\n=== SNII-native BKD vs CLucene BKD ===\n");
    printf("points=%u  span=+/-%ld  iterations=%d  points_per_leaf=%u\n", point_count, span,
           iterations, kDefaultPointsPerLeaf);

    const std::vector<EncodedPoint> points = make_points(point_count, span);

    // ---- build ----
    std::vector<uint8_t> native_index_bytes;
    MemoryFileWriter native_data;
    BkdStats stats;
    const Measurement native_build = measure([&] {
        BkdBuilderOptions options;
        options.bytes_per_dim = kBytesPerDim;
        options.field_type = kFieldType;
        std::unique_ptr<BkdBuilder> builder;
        DORIS_CHECK(BkdBuilder::create(options, &builder).ok());
        for (const EncodedPoint& p : points) {
            DORIS_CHECK(builder->add(p.doc_id, slice_of(p.value)).ok());
        }
        ByteSink index;
        DORIS_CHECK(builder->finish(&native_data, &index, &stats).ok());
        native_index_bytes = index.take();
    });

    CluceneBkd clucene;
    const Measurement clucene_build = measure([&] { clucene.build(points); });

    const uint64_t native_bytes = native_index_bytes.size() + native_data.bytes().size();
    printf("\nbuild   native  cpu=%8.3f s  bytes=%10lu  leaves=%u\n", native_build.cpu_s,
           native_bytes, stats.leaf_count);
    printf("build   clucene cpu=%8.3f s  bytes=%10lu\n", clucene_build.cpu_s, clucene.bytes());
    printf("build   ratio   cpu=%8.3fx  bytes=%8.3fx  (>1 means native is worse)\n",
           native_build.cpu_s / clucene_build.cpu_s,
           static_cast<double>(native_bytes) / static_cast<double>(clucene.bytes()));

    // ---- query ----
    BkdSections sections;
    sections.index_offset = 0;
    sections.index_length = native_index_bytes.size();
    sections.data_offset = 0;
    sections.data_length = native_data.bytes().size();

    // The native reader addresses index and data by absolute offset in ONE
    // stream, so the two sub-files are concatenated exactly as the container
    // lays them out.
    std::vector<uint8_t> concatenated = native_index_bytes;
    const uint64_t data_offset = concatenated.size();
    concatenated.insert(concatenated.end(), native_data.bytes().begin(), native_data.bytes().end());
    sections.data_offset = data_offset;

    ConcatReader reader_source(&concatenated);
    std::unique_ptr<BkdReader> native;
    DORIS_CHECK(BkdReader::open(&reader_source, sections, &native).ok());

    // range_wide spans many leaves (whole-leaf hits dominate); range_narrow
    // touches one or two (boundary leaves dominate). eq is the degenerate
    // boundary case.
    // eq must probe a value the dataset actually CONTAINS. Probing an absent
    // value times the early-exit path -- the reader proves emptiness from the
    // leaf directory and never reads a leaf -- which is a different question
    // from what a point lookup costs.
    const int64_t present = points[points.size() / 2].raw;
    const std::vector<QueryCase> cases = {
            {"eq", present, present},
            {"range_narrow", -16, 16},
            {"range_mid", -span / 64, span / 64},
            {"range_wide", -span / 2, span / 2},
            // One-sided unbounded: the native reader leaves the open side genuinely
            // unbounded while the baseline must encode a type-limit sentinel and
            // compare against it at every leaf. This is the largest win end to end
            // and had no unit-level counterpart until now.
            {"lt", std::numeric_limits<int64_t>::min(), -1},
    };

    printf("\n");
    for (const QueryCase& c : cases) {
        const std::string low = encode(c.low);
        const std::string high = encode(c.high);

        std::vector<double> native_cpu;
        std::vector<double> native_wall;
        std::vector<double> clucene_cpu;
        std::vector<double> clucene_wall;
        uint64_t native_hits = 0;
        uint64_t clucene_hits = 0;

        for (int i = 0; i < iterations; ++i) {
            roaring::Roaring hits;
            const Measurement m = measure([&] {
                DORIS_CHECK(native->range(slice_of(low), true, slice_of(high), true, &hits).ok());
            });
            native_cpu.push_back(m.cpu_s);
            native_wall.push_back(m.wall_s);
            native_hits = hits.cardinality();
        }
        for (int i = 0; i < iterations; ++i) {
            roaring::Roaring hits;
            RangeVisitor visitor(low, high, &hits);
            const Measurement m = measure([&] { clucene.reader()->intersect(&visitor); });
            clucene_cpu.push_back(m.cpu_s);
            clucene_wall.push_back(m.wall_s);
            clucene_hits = hits.cardinality();
        }

        // A performance comparison between two implementations that disagree on
        // the ANSWER is meaningless; assert equality before reporting.
        ASSERT_EQ(native_hits, clucene_hits) << "case " << c.label << " disagrees on the result";

        printf("%s (hits=%lu)\n", c.label, native_hits);
        report("native", c.label, native_cpu, native_wall);
        report("clucene", c.label, clucene_cpu, clucene_wall);
        std::sort(native_cpu.begin(), native_cpu.end());
        std::sort(clucene_cpu.begin(), clucene_cpu.end());
        printf("  ratio    %-14s cpu p50=%8.3fx  (>1 means native is slower)\n\n", c.label,
               nearest_rank_percentile(native_cpu, 50) / nearest_rank_percentile(clucene_cpu, 50));
    }
}

// IN (...) as it will actually arrive: one lookup_many pass over N ascending,
// deduplicated values against N independent traversals on the baseline side.
// N is large on purpose -- the 5-value case used end to end lands in noise and
// cannot show the ">= min(N, leaves) leaf reads" claim at all.
TEST_F(BkdNativeVsCluceneBench, DISABLED_InListManyValues) {
    const uint32_t point_count =
            static_cast<uint32_t>(env_int("SNII_BKD_BENCH_POINTS", kDefaultPoints));
    const int iterations = env_int("SNII_BKD_BENCH_ITERATIONS", kDefaultIterations);
    const int n_values = env_int("SNII_BKD_BENCH_IN_VALUES", 256);
    const int64_t span = 1 << 20;
    const std::vector<EncodedPoint> points = make_points(point_count, span);

    printf("\n=== IN (%d values), points=%u, iterations=%d ===\n", n_values, point_count,
           iterations);

    BkdBuilderOptions options;
    options.bytes_per_dim = kBytesPerDim;
    options.field_type = kFieldType;
    MemoryFileWriter data;
    std::vector<uint8_t> index_bytes;
    BkdStats stats;
    {
        std::unique_ptr<BkdBuilder> builder;
        DORIS_CHECK(BkdBuilder::create(options, &builder).ok());
        for (const EncodedPoint& p : points) {
            DORIS_CHECK(builder->add(p.doc_id, slice_of(p.value)).ok());
        }
        ByteSink index;
        DORIS_CHECK(builder->finish(&data, &index, &stats).ok());
        index_bytes = index.take();
    }
    std::vector<uint8_t> concatenated = index_bytes;
    BkdSections sections;
    sections.index_offset = 0;
    sections.index_length = index_bytes.size();
    sections.data_offset = concatenated.size();
    sections.data_length = data.bytes().size();
    concatenated.insert(concatenated.end(), data.bytes().begin(), data.bytes().end());
    ConcatReader source(&concatenated);
    std::unique_ptr<BkdReader> reader;
    DORIS_CHECK(BkdReader::open(&source, sections, &reader).ok());

    CluceneBkd clucene;
    clucene.build(points);

    // Ascending and deduplicated, as lookup_many requires. Drawn from values the
    // dataset holds so the probes are real hits.
    std::set<int64_t> wanted;
    for (int i = 0; i < n_values; ++i) {
        wanted.insert(points[(static_cast<size_t>(i) * 7919) % points.size()].raw);
    }
    std::vector<std::string> encoded;
    for (const int64_t v : wanted) {
        encoded.push_back(encode(v));
    }
    std::vector<Slice> probes;
    for (const std::string& e : encoded) {
        probes.push_back(slice_of(e));
    }

    std::vector<double> native_cpu;
    std::vector<double> native_wall;
    uint64_t native_hits = 0;
    for (int i = 0; i < iterations; ++i) {
        roaring::Roaring hits;
        const Measurement m =
                measure([&] { DORIS_CHECK(reader->lookup_many(probes, &hits).ok()); });
        native_cpu.push_back(m.cpu_s);
        native_wall.push_back(m.wall_s);
        native_hits = hits.cardinality();
    }

    std::vector<double> clucene_cpu;
    std::vector<double> clucene_wall;
    uint64_t clucene_hits = 0;
    for (int i = 0; i < iterations; ++i) {
        roaring::Roaring hits;
        const Measurement m = measure([&] {
            // The baseline shape: one full traversal per value, unioned.
            for (const std::string& e : encoded) {
                RangeVisitor visitor(e, e, &hits);
                clucene.reader()->intersect(&visitor);
            }
        });
        clucene_cpu.push_back(m.cpu_s);
        clucene_wall.push_back(m.wall_s);
        clucene_hits = hits.cardinality();
    }

    ASSERT_EQ(native_hits, clucene_hits) << "in_list disagrees on the result";
    printf("in_list (values=%zu, hits=%lu)\n", encoded.size(), native_hits);
    report("native", "in_list", native_cpu, native_wall);
    report("clucene", "in_list", clucene_cpu, clucene_wall);
    std::sort(native_cpu.begin(), native_cpu.end());
    std::sort(clucene_cpu.begin(), clucene_cpu.end());
    printf("  ratio    %-14s cpu p50=%8.3fx  (>1 means native is slower)\n\n", "in_list",
           nearest_rank_percentile(native_cpu, 50) / nearest_rank_percentile(clucene_cpu, 50));
}

// How much of a large-result query is IRREDUCIBLE?
//
// Both implementations must materialize the same doc ids into the same roaring
// bitmap. That construction is a floor neither can optimize away, so it bounds
// how fast either can possibly get. This measures the floor directly instead of
// inferring it: the bitmap is rebuilt from an already-decoded doc id array, with
// no index work at all in the timed region.
//
// If the floor is a large fraction of the measured query time, then a target
// expressed as "N% faster than the baseline" is arithmetically out of reach for
// this operator regardless of how good the index is, and the honest response is
// to say so rather than keep optimizing.
TEST_F(BkdNativeVsCluceneBench, DISABLED_ResultMaterializationFloor) {
    const uint32_t point_count =
            static_cast<uint32_t>(env_int("SNII_BKD_BENCH_POINTS", kDefaultPoints));
    const int iterations = env_int("SNII_BKD_BENCH_ITERATIONS", kDefaultIterations);
    const int64_t span = 1 << 20;
    const std::vector<EncodedPoint> points = make_points(point_count, span);

    BkdBuilderOptions options;
    options.bytes_per_dim = kBytesPerDim;
    options.field_type = kFieldType;
    MemoryFileWriter data;
    std::vector<uint8_t> index_bytes;
    BkdStats stats;
    {
        std::unique_ptr<BkdBuilder> builder;
        DORIS_CHECK(BkdBuilder::create(options, &builder).ok());
        for (const EncodedPoint& p : points) {
            DORIS_CHECK(builder->add(p.doc_id, slice_of(p.value)).ok());
        }
        ByteSink index;
        DORIS_CHECK(builder->finish(&data, &index, &stats).ok());
        index_bytes = index.take();
    }
    std::vector<uint8_t> concatenated = index_bytes;
    BkdSections sections;
    sections.index_offset = 0;
    sections.index_length = index_bytes.size();
    sections.data_offset = concatenated.size();
    sections.data_length = data.bytes().size();
    concatenated.insert(concatenated.end(), data.bytes().begin(), data.bytes().end());
    ConcatReader source(&concatenated);
    std::unique_ptr<BkdReader> reader;
    DORIS_CHECK(BkdReader::open(&source, sections, &reader).ok());

    const std::string low = encode(-span / 2);
    const std::string high = encode(span / 2);

    // The full query, for reference.
    std::vector<double> full;
    roaring::Roaring answer;
    for (int i = 0; i < iterations; ++i) {
        roaring::Roaring hits;
        full.push_back(
                measure([&] {
                    DORIS_CHECK(
                            reader->range(slice_of(low), true, slice_of(high), true, &hits).ok());
                }).cpu_s);
        answer = hits;
    }

    // The same doc ids, already decoded, inserted into a fresh bitmap. No index,
    // no decode -- only the materialization every implementation has to pay.
    std::vector<uint32_t> docids;
    docids.reserve(answer.cardinality());
    for (const uint32_t d : answer) {
        docids.push_back(d);
    }
    std::vector<double> floor_only;
    for (int i = 0; i < iterations; ++i) {
        roaring::Roaring rebuilt;
        floor_only.push_back(measure([&] { rebuilt.addMany(docids.size(), docids.data()); }).cpu_s);
        DORIS_CHECK(rebuilt.cardinality() == answer.cardinality());
    }

    // Component isolation: the same leaves, read and decoded, with NOTHING
    // inserted into a bitmap. Subtracting this from the full query separates
    // "getting the doc ids out of the format" from "putting them in the answer".
    // Guessing at this split produced three failed experiments; measuring it
    // takes one run. Done entirely through public API -- the benchmark opens its
    // own index block for the leaf extents rather than adding a bench-only
    // accessor to the reader.
    std::vector<double> decode_only;
    {
        BkdIndexBlockReader block;
        DORIS_CHECK(
                BkdIndexBlockReader::open(Slice(index_bytes), sections.data_length, &block).ok());
        const uint32_t leaf_count = block.leaf_count();
        const uint8_t* data_base = concatenated.data() + sections.data_offset;
        for (int i = 0; i < iterations; ++i) {
            uint64_t sink = 0;
            std::vector<uint32_t> ids;
            decode_only.push_back(
                    measure([&] {
                        for (uint32_t leaf = 0; leaf < leaf_count; ++leaf) {
                            const uint64_t off = block.leaf(leaf).offset;
                            const uint64_t end_off = (leaf + 1 < leaf_count)
                                                             ? block.leaf(leaf + 1).offset
                                                             : sections.data_length;
                            const Slice blk(data_base + off, static_cast<size_t>(end_off - off));
                            if (decode_leaf_doc_ids(blk, kBytesPerDim, block.leaf(leaf).count, &ids)
                                        .ok()) {
                                sink += ids.size();
                            }
                        }
                    }).cpu_s);
            DORIS_CHECK(sink > 0);
        }
        std::sort(decode_only.begin(), decode_only.end());
    }

    std::sort(full.begin(), full.end());
    std::sort(floor_only.begin(), floor_only.end());
    const double q = nearest_rank_percentile(full, 50) * 1e3;
    const double f = nearest_rank_percentile(floor_only, 50) * 1e3;
    printf("\n=== result materialization floor (range_wide, %lu hits) ===\n", answer.cardinality());
    printf("  full query          p50 = %8.3f ms\n", q);
    printf("  bitmap build only   p50 = %8.3f ms  (%.1f%% of the query)\n", f, 100.0 * f / q);
    printf("  index work          p50 = %8.3f ms\n", q - f);
    printf("  => even a FREE index could not go below %.3f ms on this operator.\n", f);
    printf("  read+decode ALL %u leaves p50 = %8.3f ms (no bitmap at all)\n", reader->leaf_count(),
           nearest_rank_percentile(decode_only, 50) * 1e3);
}

// points_per_leaf calibration. The default is 1024 because that is what the
// CLucene writer used, not because anything measured it here. A larger leaf
// reads more bytes for a boundary hit but cuts the split array and the number of
// leaf reads a wide range performs; the crossover is what this sweep locates.
TEST_F(BkdNativeVsCluceneBench, DISABLED_PointsPerLeafSweep) {
    const uint32_t point_count =
            static_cast<uint32_t>(env_int("SNII_BKD_BENCH_POINTS", kDefaultPoints));
    const int iterations = env_int("SNII_BKD_BENCH_ITERATIONS", kDefaultIterations);
    const int64_t span = 1 << 20;
    const std::vector<EncodedPoint> points = make_points(point_count, span);

    printf("\n=== points_per_leaf sweep (points=%u, iterations=%d) ===\n", point_count, iterations);
    printf("%8s %10s %10s %12s %12s %12s\n", "ppl", "leaves", "bytes", "build_cpu_s",
           "narrow_p50ms", "wide_p50ms");

    for (const uint32_t ppl : {128U, 256U, 512U, 1024U, 2048U, 4096U}) {
        BkdBuilderOptions options;
        options.bytes_per_dim = kBytesPerDim;
        options.field_type = kFieldType;
        options.points_per_leaf = ppl;

        MemoryFileWriter data;
        std::vector<uint8_t> index_bytes;
        BkdStats stats;
        const Measurement build = measure([&] {
            std::unique_ptr<BkdBuilder> builder;
            DORIS_CHECK(BkdBuilder::create(options, &builder).ok());
            for (const EncodedPoint& p : points) {
                DORIS_CHECK(builder->add(p.doc_id, slice_of(p.value)).ok());
            }
            ByteSink index;
            DORIS_CHECK(builder->finish(&data, &index, &stats).ok());
            index_bytes = index.take();
        });

        std::vector<uint8_t> concatenated = index_bytes;
        BkdSections sections;
        sections.index_offset = 0;
        sections.index_length = index_bytes.size();
        sections.data_offset = concatenated.size();
        sections.data_length = data.bytes().size();
        concatenated.insert(concatenated.end(), data.bytes().begin(), data.bytes().end());

        ConcatReader source(&concatenated);
        std::unique_ptr<BkdReader> reader;
        DORIS_CHECK(BkdReader::open(&source, sections, &reader).ok());

        const auto time_case = [&](int64_t low, int64_t high) {
            const std::string l = encode(low);
            const std::string h = encode(high);
            std::vector<double> cpu;
            for (int i = 0; i < iterations; ++i) {
                roaring::Roaring hits;
                cpu.push_back(measure([&] {
                                  DORIS_CHECK(
                                          reader->range(slice_of(l), true, slice_of(h), true, &hits)
                                                  .ok());
                              }).cpu_s);
            }
            std::sort(cpu.begin(), cpu.end());
            return nearest_rank_percentile(cpu, 50) * 1e3;
        };

        printf("%8u %10u %10lu %12.3f %12.3f %12.3f\n", ppl, stats.leaf_count,
               index_bytes.size() + data.bytes().size(), build.cpu_s, time_case(-16, 16),
               time_case(-span / 2, span / 2));
    }
}

} // namespace
} // namespace doris::snii::bkd
