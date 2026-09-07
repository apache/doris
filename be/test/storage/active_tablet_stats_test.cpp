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

#include "storage/active_tablet_stats.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

constexpr int64_t kNowMs = 1'700'000'000'000;
constexpr int64_t kReportIntervalMs = 60'000;

// Minimal concrete BaseTablet, same shape as the FakeTablet stubs in the scanner and
// file-cache tests. The collector only reads the counters and the atomics added for
// active-tablet stats, so none of these overrides is ever reached.
class StatsTestTablet final : public BaseTablet {
public:
    explicit StatsTestTablet(int64_t tablet_id) : BaseTablet(create_meta(tablet_id)) {}

    std::string tablet_path() const override { return ""; }

    bool exceed_version_limit(int32_t /*limit*/) override { return false; }

    Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& /*context*/,
                                                               bool /*vertical*/) override {
        return ResultError(Status::NotSupported("stats test tablet"));
    }

    Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& /*rowset*/, std::shared_ptr<PartialUpdateInfo> /*partial_update_info*/,
            int64_t /*txn_expiration*/ = 0) override {
        return ResultError(Status::NotSupported("stats test tablet"));
    }

    Status capture_rs_readers(const Version& /*spec_version*/,
                              std::vector<RowSetSplits>* /*rs_splits*/,
                              const CaptureRowsetOps& /*opts*/) override {
        return Status::NotSupported("stats test tablet");
    }

    Status save_delete_bitmap(const TabletTxnInfo* /*txn_info*/, int64_t /*txn_id*/,
                              DeleteBitmapPtr /*delete_bitmap*/, RowsetWriter* /*rowset_writer*/,
                              const RowsetIdUnorderedSet& /*cur_rowset_ids*/,
                              int64_t /*lock_id*/ = -1,
                              int64_t /*next_visible_version*/ = -1) override {
        return Status::NotSupported("stats test tablet");
    }

    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() override { return nullptr; }

    void clear_cache() override {}

    Versions calc_missed_versions(int64_t /*spec_version*/,
                                  Versions /*existing_versions*/) const override {
        return {};
    }

    size_t tablet_footprint() override { return 0; }

private:
    static TabletMetaSharedPtr create_meta(int64_t tablet_id) {
        auto meta = std::make_shared<TabletMeta>(std::make_shared<TabletSchema>());
        meta->_tablet_id = tablet_id;
        return meta;
    }
};

// A tablet that already has a committed baseline, i.e. one the collector will emit
// candidates for rather than skip as first-seen.
std::shared_ptr<StatsTestTablet> make_baselined_tablet(int64_t tablet_id, int64_t baseline_ms) {
    auto tablet = std::make_shared<StatsTestTablet>(tablet_id);
    tablet->last_reported_time_ms.store(baseline_ms);
    tablet->last_query_scan_time_ms.store(baseline_ms);
    tablet->last_load_flush_time_ms.store(baseline_ms);
    return tablet;
}

// Drives one report round at a fixed wall clock instead of UnixMillis(), so the
// delta windows the assertions check are exact.
void collect_at(ActiveTabletCollector& collector, const std::shared_ptr<BaseTablet>& tablet,
                int64_t now_ms) {
    collector._now_ms = now_ms;
    collector.collect(tablet);
}

} // namespace

// The reason BE ranks by delta/window and not by raw delta: tablet 1 has 5x the raw
// scan delta of tablet 2 but accumulated it over 10x the window, so it is the colder
// one. Also pins that the two dimensions are truncated independently -- a load-only
// tablet must not be crowded out by query traffic.
TEST(ActiveTabletStatsTest, TakesIndependentTopListsByRate) {
    const auto old_cap = config::report_active_tablet_max_num;
    config::report_active_tablet_max_num = 1;

    ActiveTabletCollector collector;
    collector._query_cands = {{1, 100, 10000, 0}, {2, 20, 1000, 0}};
    collector._load_cands = {{3, 30, 1000, 0}, {4, 200, 10000, 0}};

    collector.take_top_n();
    config::report_active_tablet_max_num = old_cap;

    ASSERT_EQ(collector.query_candidates().size(), 1);
    EXPECT_EQ(collector.query_candidates().front().tablet_id, 2);
    ASSERT_EQ(collector.load_candidates().size(), 1);
    EXPECT_EQ(collector.load_candidates().front().tablet_id, 3);
    EXPECT_TRUE(collector.truncated());
}

// A tablet seen for the first time (no baseline yet) must not be reported: without a
// previous timestamp there is no window to normalise its delta by.
TEST(ActiveTabletStatsTest, FirstRoundOnlyEstablishesBaseline) {
    auto tablet = std::make_shared<StatsTestTablet>(101);
    tablet->query_scan_count->increment(42);
    tablet->last_query_scan_time_ms.store(kNowMs);

    ActiveTabletCollector collector;
    collect_at(collector, tablet, kNowMs);

    EXPECT_TRUE(collector.query_candidates().empty());
    EXPECT_TRUE(collector.load_candidates().empty());

    collector.commit();
    EXPECT_EQ(tablet->last_reported_scan_count.load(), 42);
    EXPECT_EQ(tablet->last_reported_time_ms.load(), kNowMs);
}

// Collection must be read-only on the baselines. The report path retries the walk up
// to 5 times on a report-version conflict, so a destructive read (exchange) would make
// every retry after the first see a zero delta.
TEST(ActiveTabletStatsTest, CollectDoesNotAdvanceBaseline) {
    auto tablet = make_baselined_tablet(102, kNowMs - kReportIntervalMs);
    tablet->query_scan_count->increment(10);

    ActiveTabletCollector collector;
    for (int retry = 0; retry < 3; ++retry) {
        collector.clear();
        collect_at(collector, tablet, kNowMs);

        ASSERT_EQ(collector.query_candidates().size(), 1) << "retry " << retry;
        EXPECT_EQ(collector.query_candidates().front().delta, 10);
        EXPECT_EQ(collector.query_candidates().front().window_ms, kReportIntervalMs);
        EXPECT_EQ(tablet->last_reported_scan_count.load(), 0);
        EXPECT_EQ(tablet->last_reported_time_ms.load(), kNowMs - kReportIntervalMs);
    }
}

// Once the report is delivered, commit() advances the baseline so the next round
// reports only what happened since.
TEST(ActiveTabletStatsTest, CommitAdvancesBaselineSoNextDeltaIsIncremental) {
    auto tablet = make_baselined_tablet(103, kNowMs - kReportIntervalMs);
    tablet->query_scan_count->increment(10);
    tablet->flush_finish_count->increment(4);

    ActiveTabletCollector collector;
    collect_at(collector, tablet, kNowMs);
    collector.commit();
    EXPECT_EQ(tablet->last_reported_scan_count.load(), 10);
    EXPECT_EQ(tablet->last_reported_flush_count.load(), 4);
    EXPECT_EQ(tablet->last_reported_time_ms.load(), kNowMs);

    const int64_t next_ms = kNowMs + kReportIntervalMs;
    tablet->query_scan_count->increment(3);
    tablet->flush_finish_count->increment(1);
    tablet->last_query_scan_time_ms.store(next_ms);
    tablet->last_load_flush_time_ms.store(next_ms);

    collector.clear();
    collect_at(collector, tablet, next_ms);

    ASSERT_EQ(collector.query_candidates().size(), 1);
    EXPECT_EQ(collector.query_candidates().front().delta, 3);
    EXPECT_EQ(collector.query_candidates().front().window_ms, kReportIntervalMs);
    ASSERT_EQ(collector.load_candidates().size(), 1);
    EXPECT_EQ(collector.load_candidates().front().delta, 1);
}

// When a report is dropped (5 failed retries, or handle_report() returning false) the
// baseline is not committed, so the next round's delta must span both intervals and
// window_ms must widen with it -- that is what keeps the rate comparable.
TEST(ActiveTabletStatsTest, DroppedReportCarriesDeltaIntoAWiderWindow) {
    auto tablet = make_baselined_tablet(104, kNowMs - kReportIntervalMs);
    tablet->query_scan_count->increment(10);

    ActiveTabletCollector collector;
    collect_at(collector, tablet, kNowMs);
    // report failed: no commit()

    const int64_t next_ms = kNowMs + kReportIntervalMs;
    tablet->query_scan_count->increment(10);
    tablet->last_query_scan_time_ms.store(next_ms);

    collector.clear();
    collect_at(collector, tablet, next_ms);

    ASSERT_EQ(collector.query_candidates().size(), 1);
    const auto& candidate = collector.query_candidates().front();
    EXPECT_EQ(candidate.delta, 20);
    EXPECT_EQ(candidate.window_ms, 2 * kReportIntervalMs);
    // Same rate as a single round of 10 -- the point of normalising by the window.
    EXPECT_DOUBLE_EQ(candidate.rate(), 10.0 * 1000.0 / kReportIntervalMs);
}

// A tablet whose delta is stale -- it kept being cut by the topN cap, so its baseline
// was never committed -- must age out of the report once it stops being touched.
// This is the case report_active_tablet_window_second exists for; without it the
// stale delta would keep the tablet in the active set forever.
TEST(ActiveTabletStatsTest, StaleDeltaOutsideActiveWindowIsDropped) {
    const int64_t window_ms = int64_t {config::report_active_tablet_window_second} * 1000;
    auto tablet = make_baselined_tablet(105, kNowMs - window_ms - kReportIntervalMs);
    tablet->query_scan_count->increment(10);
    // Last actually queried just before the active window opened.
    tablet->last_query_scan_time_ms.store(kNowMs - window_ms - 1);

    ActiveTabletCollector collector;
    collect_at(collector, tablet, kNowMs);

    EXPECT_TRUE(collector.query_candidates().empty());
    // Still pending, so the baseline moves on and the stale delta is retired.
    collector.commit();
    EXPECT_EQ(tablet->last_reported_scan_count.load(), 10);
}

// A tablet dropped between collection and delivery must not crash commit().
TEST(ActiveTabletStatsTest, CommitSkipsTabletsDroppedAfterCollection) {
    auto tablet = make_baselined_tablet(106, kNowMs - kReportIntervalMs);
    tablet->query_scan_count->increment(10);

    ActiveTabletCollector collector;
    collect_at(collector, tablet, kNowMs);

    tablet.reset();
    collector.commit();
}

} // namespace doris
