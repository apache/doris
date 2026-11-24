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

// Compare deltas because the build memory tracker is shared across the process.

#include "storage/index/inverted/snii/writer/snii_build_memory_tracker.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "common/status.h"
#include "runtime/memory/mem_tracker.h"
#include "storage/index/inverted/snii/writer/memory_reporter.h"

namespace doris::snii::writer {
namespace {

// Changes of at least one step reach the shared counters at once, so tests that are
// about what those counters mean use whole steps.
constexpr int64_t kStep = kBuildBytesPublishStep;

TEST(SniiBuildMemoryTracker, IsALabelledProcessSingleton) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    ASSERT_NE(tracker, nullptr);
    EXPECT_EQ(tracker, snii_build_mem_tracker()) << "the tracker must be a stable singleton";
    EXPECT_EQ(tracker->label(), "SniiIndexBuild")
            << "the label is what makes index-build memory identifiable in the memory picture";
}

TEST(SniiBuildMemoryTracker, ReservationsMoveTheTrackerAndReturnToBaseline) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    {
        MemoryReporter reporter(snii_build_consume_release(BuildMemoryPopulation::kRegistered),
                                /*cap_bytes=*/0, MemoryReporter::CapPolicy::kSpillThreshold);
        MemoryReporter::Reservation reservation = reporter.make_reservation();
        ASSERT_TRUE(reservation.set_bytes(kStep).ok());
        EXPECT_EQ(reporter.current_bytes(), kStep);
        EXPECT_EQ(tracker->consumption() - baseline, reporter.current_bytes());

        ASSERT_TRUE(reservation.set_bytes(4 * kStep).ok());
        EXPECT_EQ(tracker->consumption() - baseline, reporter.current_bytes())
                << "the tracker must track the reporter's LIVE bytes, not its peak";

        // Shrinking is the half a leak would silently skip.
        ASSERT_TRUE(reservation.set_bytes(2 * kStep).ok());
        EXPECT_EQ(tracker->consumption() - baseline, reporter.current_bytes());
    }
    EXPECT_EQ(tracker->consumption(), baseline)
            << "a drained writer must leave nothing behind in the memory picture";
}

TEST(SniiBuildMemoryTracker, LegacyReportPathAlsoMovesTheTracker) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    MemoryReporter reporter(snii_build_consume_release(BuildMemoryPopulation::kRegistered));
    reporter.report(3 * kStep);
    EXPECT_EQ(tracker->consumption() - baseline, 3 * kStep);
    reporter.report(kStep);
    EXPECT_EQ(tracker->consumption() - baseline, 4 * kStep);
    reporter.report(-4 * kStep);
    EXPECT_EQ(tracker->consumption(), baseline);
}

// Independent reporters (ingestion writers plus the compaction merge reporter)
// share one tracker: the limiter's decision is over their SUM, which is the
// whole point of having a single labelled line.
TEST(SniiBuildMemoryTracker, SeparateReportersAccumulateIntoOneLine) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    auto ingestion = std::make_unique<MemoryReporter>(
            snii_build_consume_release(BuildMemoryPopulation::kRegistered));
    auto merge = std::make_unique<MemoryReporter>(
            snii_build_consume_release(BuildMemoryPopulation::kRegistered));
    ingestion->report(7 * kStep);
    merge->report(3 * kStep);
    EXPECT_EQ(tracker->consumption() - baseline, 10 * kStep);

    ingestion->report(-7 * kStep);
    EXPECT_EQ(tracker->consumption() - baseline, 3 * kStep);
    merge->report(-3 * kStep);
    EXPECT_EQ(tracker->consumption(), baseline);
}

// C2: the DECISION layer must not be charged for memory it has no lever over.
// Index-merge compaction feeds the same observation line but registers no
// spillable writer, so it is absent from the registered counter the decision
// reads -- without the decision ever subtracting one population from another.
TEST(SniiBuildMemoryTracker, RegisteredPopulationIsTrackedSeparatelyFromTheObservationLine) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    const int64_t registered_baseline = snii_registered_build_bytes();
    {
        MemoryReporter ingestion(snii_build_consume_release(BuildMemoryPopulation::kRegistered));
        MemoryReporter merge(snii_build_consume_release(BuildMemoryPopulation::kUnregistered));
        ingestion.report(7 * kStep);
        merge.report(3 * kStep);
        // ONE observation line covers both: that is Part 1's goal.
        EXPECT_EQ(tracker->consumption() - baseline, 10 * kStep);
        // What the decision layer judges, read as a single value: the
        // reclaimable population alone, with the merge's bytes never included.
        EXPECT_EQ(snii_registered_build_bytes() - registered_baseline, 7 * kStep);
        ingestion.report(-7 * kStep);
        merge.report(-3 * kStep);
    }
    EXPECT_EQ(tracker->consumption(), baseline);
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

// I1: MemoryReporter used to be able to die with unbalanced legacy report()
// bytes harmlessly, because consume_release_ was null in production. Now the
// residue would sit in a PROCESS-WIDE counter that also drives the build-RAM
// decision, so a few MiB per segment would become permanent phantom pressure.
// The destructor drains whatever is left.
TEST(SniiBuildMemoryTracker, ReporterDestructorDrainsUnbalancedBytes) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    {
        MemoryReporter leaky(snii_build_consume_release(BuildMemoryPopulation::kRegistered));
        leaky.report(4 * kStep); // no matching report(-4 * kStep): the bug being contained
        EXPECT_EQ(tracker->consumption() - baseline, 4 * kStep);
    }
    EXPECT_EQ(tracker->consumption(), baseline)
            << "an unbalanced reporter must not leave residue in a process-wide counter";
}

// The drain must cover the registered counter too, since that is what the
// decision reads: residue there would be permanent phantom reclaimable memory.
TEST(SniiBuildMemoryTracker, ReporterDestructorDrainsTheRegisteredSubsetToo) {
    const int64_t registered_baseline = snii_registered_build_bytes();
    {
        MemoryReporter leaky(snii_build_consume_release(BuildMemoryPopulation::kRegistered));
        leaky.report(4 * kStep);
        EXPECT_EQ(snii_registered_build_bytes() - registered_baseline, 4 * kStep);
    }
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

// ...and an unregistered reporter's residue must never reach it.
TEST(SniiBuildMemoryTracker, UnregisteredResidueNeverReachesTheDecisionInput) {
    const int64_t registered_baseline = snii_registered_build_bytes();
    {
        MemoryReporter leaky(snii_build_consume_release(BuildMemoryPopulation::kUnregistered));
        leaky.report(4 * kStep);
        EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
    }
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

// This is what keeps flush threads off each other's cache lines: a term encoder takes
// scratch and gives it back for every term, and that must never reach the shared
// counters. The reporter stays exact, because the hard budget of compaction reads it.
// Both populations: BKD writers and compaction merges run the same per-term reservations
// on the same flush threads as the text writers.
TEST(SniiBuildMemoryTracker, PerTermSwingsStayOffTheSharedCounters) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    const int64_t registered_baseline = snii_registered_build_bytes();
    constexpr int64_t kTermScratch = 32 * 1024;
    for (BuildMemoryPopulation population :
         {BuildMemoryPopulation::kRegistered, BuildMemoryPopulation::kUnregistered}) {
        MemoryReporter reporter(snii_build_consume_release(population));
        for (int term = 0; term < 1000; ++term) {
            MemoryReporter::Reservation scratch = reporter.make_postings_reservation();
            ASSERT_TRUE(scratch.set_bytes(kTermScratch).ok());
            ASSERT_EQ(reporter.current_bytes(), kTermScratch);
            ASSERT_EQ(tracker->consumption(), baseline);
            ASSERT_EQ(snii_registered_build_bytes(), registered_baseline);
        }
        EXPECT_EQ(reporter.current_bytes(), 0);
    }
    EXPECT_EQ(tracker->consumption(), baseline);
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

TEST(SniiBuildMemoryTracker, HeldBackBytesArePublishedTogetherAtTheStep) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    const int64_t registered_baseline = snii_registered_build_bytes();
    MemoryReporter reporter(snii_build_consume_release(BuildMemoryPopulation::kRegistered));

    reporter.report(kStep - 1);
    EXPECT_EQ(tracker->consumption(), baseline);
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);

    reporter.report(1);
    EXPECT_EQ(tracker->consumption() - baseline, kStep);
    EXPECT_EQ(snii_registered_build_bytes() - registered_baseline, kStep);

    // Publishing restarts the count, so one byte back must not bounce off the step.
    reporter.report(-1);
    EXPECT_EQ(tracker->consumption() - baseline, kStep);

    reporter.report(-(kStep - 1));
    EXPECT_EQ(tracker->consumption(), baseline);
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

// +1.5 steps and -1.25 steps both publish, and the last -0.25 steps never reaches a step.
// The reporter must not wait for its destructor to give that back: IndexFileWriter keeps
// a finished text writer's reporter until the rowset writer closes, so every segment of a
// load would leave its remainder in the spill decision's input. An empty reporter
// publishes what it holds back.
TEST(SniiBuildMemoryTracker, AnEmptyReporterHoldsNothingBack) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    const int64_t registered_baseline = snii_registered_build_bytes();
    auto retained = std::make_shared<MemoryReporter>(
            snii_build_consume_release(BuildMemoryPopulation::kRegistered));
    retained->report(kStep + kStep / 2);
    retained->report(-(kStep + kStep / 4));
    EXPECT_EQ(tracker->consumption() - baseline, kStep / 4);
    retained->report(-kStep / 4);
    EXPECT_EQ(retained->current_bytes(), 0);
    EXPECT_EQ(tracker->consumption(), baseline)
            << "a finished writer whose reporter is still retained left bytes behind";
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);

    // The same holds when the reporter goes back to zero through a reservation: both steps
    // publish, and only the zero rule can return the last quarter step.
    {
        MemoryReporter::Reservation reservation = retained->make_reservation();
        ASSERT_TRUE(reservation.set_bytes(kStep + kStep / 2).ok());
        ASSERT_TRUE(reservation.set_bytes(kStep / 4).ok());
        EXPECT_EQ(tracker->consumption() - baseline, kStep / 4);
    }
    EXPECT_EQ(retained->current_bytes(), 0);
    EXPECT_EQ(tracker->consumption(), baseline);
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

// Unbalanced bytes still go back when the reporter is destroyed.
TEST(SniiBuildMemoryTracker, DestroyingAReporterPublishesWhatItHeldBack) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    {
        MemoryReporter reporter(snii_build_consume_release(BuildMemoryPopulation::kRegistered));
        reporter.report(kStep + kStep / 2);
        reporter.report(-(kStep + kStep / 4));
        reporter.report(kStep / 8);
        EXPECT_EQ(tracker->consumption() - baseline, kStep / 4);
    }
    EXPECT_EQ(tracker->consumption(), baseline);
}

TEST(SniiBuildMemoryTracker, ConcurrentChangesOnOneReporterLoseNothing) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    const int64_t registered_baseline = snii_registered_build_bytes();
    constexpr int kThreads = 8;
    constexpr int64_t kChange = kStep / 3 + 7;
    {
        MemoryReporter reporter(snii_build_consume_release(BuildMemoryPopulation::kRegistered));
        std::vector<std::thread> threads;
        for (int i = 0; i < kThreads; ++i) {
            // Each thread leaves its own net on the reporter, so a lost change would
            // show as a wrong total and not just cancel out.
            threads.emplace_back([&reporter] {
                for (int n = 0; n < 2000; ++n) {
                    reporter.report(kChange);
                    reporter.report(n == 0 ? 0 : -kChange);
                }
            });
        }
        for (std::thread& thread : threads) {
            thread.join();
        }
        EXPECT_EQ(reporter.current_bytes(), kThreads * kChange);
        const int64_t published = tracker->consumption() - baseline;
        EXPECT_LT(reporter.current_bytes() - published, kStep);
        EXPECT_GT(reporter.current_bytes() - published, -kStep);
    }
    EXPECT_EQ(tracker->consumption(), baseline);
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

} // namespace
} // namespace doris::snii::writer
