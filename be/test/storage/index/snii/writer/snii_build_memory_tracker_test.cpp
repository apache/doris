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

// The SNII index-build OBSERVATION tracker: a labelled MemTracker fed by every
// production MemoryReporter so index-build RAM is a visible category in Doris's
// memory picture (the jemalloc allocation hook only knows which THREAD
// allocated, so the bytes otherwise dissolve into whichever task tracker
// happened to be attached).
//
// These tests pin the contract the limiter's decision layer depends on:
//   (1) the tracker is a stable, labelled process singleton;
//   (2) a reporter wired with snii_build_consume_release(BuildMemoryPopulation::kRegistered) moves the tracker in
//       lockstep with its own live bytes, through BOTH the Reservation path and
//       the legacy report() path;
//   (3) the tracker returns to its baseline when writers drain -- a missed
//       negative would leave permanently overstated memory in the picture and,
//       through the limiter, permanent phantom back-pressure.
//
// The tracker is process-wide and shared with anything else in this binary that
// builds a production reporter, so every assertion is a DELTA from a baseline
// taken at the start of the test, never an absolute value.

#include "storage/index/snii/writer/snii_build_memory_tracker.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

#include "common/status.h"
#include "runtime/memory/mem_tracker.h"
#include "storage/index/snii/writer/memory_reporter.h"

namespace doris::snii::writer {
namespace {

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
        ASSERT_TRUE(reservation.set_bytes(4096).ok());
        EXPECT_EQ(reporter.current_bytes(), 4096);
        EXPECT_EQ(tracker->consumption() - baseline, reporter.current_bytes());

        ASSERT_TRUE(reservation.set_bytes(64 * 1024).ok());
        EXPECT_EQ(tracker->consumption() - baseline, reporter.current_bytes())
                << "the tracker must track the reporter's LIVE bytes, not its peak";

        // Shrinking is the half a leak would silently skip.
        ASSERT_TRUE(reservation.set_bytes(1024).ok());
        EXPECT_EQ(tracker->consumption() - baseline, reporter.current_bytes());
    }
    EXPECT_EQ(tracker->consumption(), baseline)
            << "a drained writer must leave nothing behind in the memory picture";
}

TEST(SniiBuildMemoryTracker, LegacyReportPathAlsoMovesTheTracker) {
    doris::MemTracker* tracker = snii_build_mem_tracker();
    const int64_t baseline = tracker->consumption();
    MemoryReporter reporter(snii_build_consume_release(BuildMemoryPopulation::kRegistered));
    reporter.report(1500);
    EXPECT_EQ(tracker->consumption() - baseline, 1500);
    reporter.report(500);
    EXPECT_EQ(tracker->consumption() - baseline, 2000);
    reporter.report(-2000);
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
    ingestion->report(700);
    merge->report(300);
    EXPECT_EQ(tracker->consumption() - baseline, 1000);

    ingestion->report(-700);
    EXPECT_EQ(tracker->consumption() - baseline, 300);
    merge->report(-300);
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
        ingestion.report(700);
        merge.report(300);
        // ONE observation line covers both: that is Part 1's goal.
        EXPECT_EQ(tracker->consumption() - baseline, 1000);
        // What the decision layer judges, read as a single value: the
        // reclaimable population alone, with the merge's 300 never included.
        EXPECT_EQ(snii_registered_build_bytes() - registered_baseline, 700);
        ingestion.report(-700);
        merge.report(-300);
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
        leaky.report(4096); // no matching report(-4096): the bug being contained
        EXPECT_EQ(tracker->consumption() - baseline, 4096);
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
        leaky.report(4096);
        EXPECT_EQ(snii_registered_build_bytes() - registered_baseline, 4096);
    }
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

// ...and an unregistered reporter's residue must never reach it.
TEST(SniiBuildMemoryTracker, UnregisteredResidueNeverReachesTheDecisionInput) {
    const int64_t registered_baseline = snii_registered_build_bytes();
    {
        MemoryReporter leaky(snii_build_consume_release(BuildMemoryPopulation::kUnregistered));
        leaky.report(4096);
        EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
    }
    EXPECT_EQ(snii_registered_build_bytes(), registered_baseline);
}

} // namespace
} // namespace doris::snii::writer
