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

#include "util/monotime.h"

#include <gtest/gtest.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstdint>
#include <ctime>
#include <ostream>
#include <string>

#include "common/logging.h"

namespace doris {

TEST(TestMonoTime, TestMonotonicity) {
    alarm(360);
    MonoTime prev(MonoTime::Now());
    MonoTime next;

    do {
        next = MonoTime::Now();
        //LOG(INFO) << " next = " << next.ToString();
    } while (!prev.ComesBefore(next));
    ASSERT_FALSE(next.ComesBefore(prev));
    alarm(0);
}

TEST(TestMonoTime, TestComparison) {
    MonoTime now(MonoTime::Now());
    MonoTime future(now);
    future.AddDelta(MonoDelta::FromNanoseconds(1L));

    ASSERT_GT((future - now).ToNanoseconds(), 0);
    ASSERT_LT((now - future).ToNanoseconds(), 0);
    ASSERT_EQ((now - now).ToNanoseconds(), 0);

    MonoDelta nano(MonoDelta::FromNanoseconds(1L));
    MonoDelta mil(MonoDelta::FromMilliseconds(1L));
    MonoDelta sec(MonoDelta::FromSeconds(1.0));

    ASSERT_TRUE(nano.LessThan(mil));
    ASSERT_TRUE(mil.LessThan(sec));
    ASSERT_TRUE(mil.MoreThan(nano));
    ASSERT_TRUE(sec.MoreThan(mil));
}

TEST(TestMonoTime, TestTimeVal) {
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;

    // Normal conversion case.
    MonoDelta one_sec_one_micro(MonoDelta::FromNanoseconds(1000001000L));
    one_sec_one_micro.ToTimeVal(&tv);
    ASSERT_EQ(1, tv.tv_sec);
    ASSERT_EQ(1, tv.tv_usec);

    // Case where we are still positive but sub-micro.
    // Round up to nearest microsecond. This is to avoid infinite timeouts
    // in APIs that take a struct timeval.
    MonoDelta zero_sec_one_nano(MonoDelta::FromNanoseconds(1L));
    zero_sec_one_nano.ToTimeVal(&tv);
    ASSERT_EQ(0, tv.tv_sec);
    ASSERT_EQ(1, tv.tv_usec); // Special case: 1ns rounds up to

    // Negative conversion case. Ensure the timeval is normalized.
    // That means sec is negative and usec is positive.
    MonoDelta neg_micro(MonoDelta::FromMicroseconds(-1L));
    ASSERT_EQ(-1000, neg_micro.ToNanoseconds());
    neg_micro.ToTimeVal(&tv);
    ASSERT_EQ(-1, tv.tv_sec);
    ASSERT_EQ(999999, tv.tv_usec);

    // Case where we are still negative but sub-micro.
    // Round up to nearest microsecond. This is to avoid infinite timeouts
    // in APIs that take a struct timeval and for consistency.
    MonoDelta zero_sec_neg_one_nano(MonoDelta::FromNanoseconds(-1L));
    zero_sec_neg_one_nano.ToTimeVal(&tv);
    ASSERT_EQ(-1, tv.tv_sec);
    ASSERT_EQ(999999, tv.tv_usec);
}

TEST(TestMonoTime, TestTimeSpec) {
    MonoTime one_sec_one_nano_expected(1000000001L);
    struct timespec ts;
    ts.tv_sec = 1;
    ts.tv_nsec = 1;
    MonoTime one_sec_one_nano_actual(ts);
    ASSERT_EQ(0, one_sec_one_nano_expected.GetDeltaSince(one_sec_one_nano_actual).ToNanoseconds());

    MonoDelta zero_sec_two_nanos(MonoDelta::FromNanoseconds(2L));
    zero_sec_two_nanos.ToTimeSpec(&ts);
    ASSERT_EQ(0, ts.tv_sec);
    ASSERT_EQ(2, ts.tv_nsec);

    // Negative conversion case. Ensure the timespec is normalized.
    // That means sec is negative and nsec is positive.
    MonoDelta neg_nano(MonoDelta::FromNanoseconds(-1L));
    ASSERT_EQ(-1, neg_nano.ToNanoseconds());
    neg_nano.ToTimeSpec(&ts);
    ASSERT_EQ(-1, ts.tv_sec);
    ASSERT_EQ(999999999, ts.tv_nsec);
}

TEST(TestMonoTime, TestDeltas) {
    alarm(360);
    const MonoDelta max_delta(MonoDelta::FromSeconds(0.1));
    MonoTime prev(MonoTime::Now());
    MonoTime next;
    MonoDelta cur_delta;
    do {
        next = MonoTime::Now();
        cur_delta = next.GetDeltaSince(prev);
    } while (cur_delta.LessThan(max_delta));
    alarm(0);
}

TEST(TestMonoTime, TestDeltaConversions) {
    // TODO: Reliably test MonoDelta::FromSeconds() considering floating-point rounding errors

    MonoDelta mil(MonoDelta::FromMilliseconds(500));
    ASSERT_EQ(500 * MonoTime::kNanosecondsPerMillisecond, mil.nano_delta_);

    MonoDelta micro(MonoDelta::FromMicroseconds(500));
    ASSERT_EQ(500 * MonoTime::kNanosecondsPerMicrosecond, micro.nano_delta_);

    MonoDelta nano(MonoDelta::FromNanoseconds(500));
    ASSERT_EQ(500, nano.nano_delta_);
}

static void DoTestMonoTimePerf() {
    const MonoDelta max_delta(MonoDelta::FromMilliseconds(500));
    uint64_t num_calls = 0;
    MonoTime prev(MonoTime::Now());
    MonoTime next;
    MonoDelta cur_delta;
    do {
        next = MonoTime::Now();
        cur_delta = next.GetDeltaSince(prev);
        num_calls++;
    } while (cur_delta.LessThan(max_delta));
    LOG(INFO) << "DoTestMonoTimePerf():" << num_calls << " in " << max_delta.ToString()
              << " seconds.";
}

TEST(TestMonoTime, TestSleepFor) {
    MonoTime start = MonoTime::Now();
    MonoDelta sleep = MonoDelta::FromMilliseconds(100);
    SleepFor(sleep);
    MonoTime end = MonoTime::Now();
    MonoDelta actualSleep = end.GetDeltaSince(start);
    ASSERT_GE(actualSleep.ToNanoseconds(), sleep.ToNanoseconds());
}

// Test functionality of the handy operators for MonoTime/MonoDelta objects.
// The test assumes that the core functionality provided by the
// MonoTime/MonoDelta objects are in place, and it tests that the operators
// have the expected behavior expressed in terms of already existing,
// semantically equivalent methods.
TEST(TestMonoTime, TestOperators) {
    // MonoTime& MonoTime::operator+=(const MonoDelta& delta);
    {
        MonoTime tmp = MonoTime::Now();
        MonoTime start = tmp;
        MonoDelta delta = MonoDelta::FromMilliseconds(100);
        MonoTime o_end = start;
        o_end += delta;
        tmp.AddDelta(delta);
        MonoTime m_end = tmp;
        EXPECT_TRUE(m_end.Equals(o_end));
    }

    // MonoTime& MonoTime::operator-=(const MonoDelta& delta);
    {
        MonoTime tmp = MonoTime::Now();
        MonoTime start = tmp;
        MonoDelta delta = MonoDelta::FromMilliseconds(100);
        MonoTime o_end = start;
        o_end -= delta;
        tmp.AddDelta(MonoDelta::FromNanoseconds(-delta.ToNanoseconds()));
        MonoTime m_end = tmp;
        EXPECT_TRUE(m_end.Equals(o_end));
    }

    // bool operator==(const MonoDelta& lhs, const MonoDelta& rhs);
    {
        MonoDelta dn = MonoDelta::FromNanoseconds(0);
        MonoDelta dm = MonoDelta::FromMicroseconds(0);
        ASSERT_TRUE(dn.Equals(dm));
        EXPECT_TRUE(dn == dm);
        EXPECT_TRUE(dm == dn);
    }

    // bool operator!=(const MonoDelta& lhs, const MonoDelta& rhs);
    {
        MonoDelta dn = MonoDelta::FromNanoseconds(1);
        MonoDelta dm = MonoDelta::FromMicroseconds(1);
        ASSERT_FALSE(dn.Equals(dm));
        EXPECT_TRUE(dn != dm);
        EXPECT_TRUE(dm != dn);
    }

    // bool operator<(const MonoDelta& lhs, const MonoDelta& rhs);
    {
        MonoDelta d0 = MonoDelta::FromNanoseconds(0);
        MonoDelta d1 = MonoDelta::FromNanoseconds(1);
        ASSERT_TRUE(d0.LessThan(d1));
        EXPECT_TRUE(d0 < d1);
    }

    // bool operator<=(const MonoDelta& lhs, const MonoDelta& rhs);
    {
        MonoDelta d0 = MonoDelta::FromNanoseconds(0);
        MonoDelta d1 = MonoDelta::FromNanoseconds(1);
        ASSERT_TRUE(d0.LessThan(d1));
        EXPECT_TRUE(d0 <= d1);

        MonoDelta d20 = MonoDelta::FromNanoseconds(2);
        MonoDelta d21 = MonoDelta::FromNanoseconds(2);
        ASSERT_TRUE(d20.Equals(d21));
        EXPECT_TRUE(d20 <= d21);
    }

    // bool operator>(const MonoDelta& lhs, const MonoDelta& rhs);
    {
        MonoDelta d0 = MonoDelta::FromNanoseconds(0);
        MonoDelta d1 = MonoDelta::FromNanoseconds(1);
        ASSERT_TRUE(d1.MoreThan(d0));
        EXPECT_TRUE(d1 > d0);
    }

    // bool operator>=(const MonoDelta& lhs, const MonoDelta& rhs);
    {
        MonoDelta d0 = MonoDelta::FromNanoseconds(0);
        MonoDelta d1 = MonoDelta::FromNanoseconds(1);
        ASSERT_TRUE(d1.MoreThan(d0));
        EXPECT_TRUE(d1 >= d1);

        MonoDelta d20 = MonoDelta::FromNanoseconds(2);
        MonoDelta d21 = MonoDelta::FromNanoseconds(2);
        ASSERT_TRUE(d20.Equals(d21));
        EXPECT_TRUE(d21 >= d20);
    }

    // bool operator==(const MonoTime& lhs, const MonoTime& rhs);
    {
        MonoTime t0 = MonoTime::Now();
        MonoTime t1(t0);
        ASSERT_TRUE(t0.Equals(t1));
        ASSERT_TRUE(t1.Equals(t0));
        EXPECT_TRUE(t0 == t1);
        EXPECT_TRUE(t1 == t0);
    }

    // bool operator!=(const MonoTime& lhs, const MonoTime& rhs);
    {
        MonoTime t0 = MonoTime::Now();
        MonoTime t1(t0 + MonoDelta::FromMilliseconds(100));
        ASSERT_TRUE(!t0.Equals(t1));
        ASSERT_TRUE(!t1.Equals(t0));
        EXPECT_TRUE(t0 != t1);
        EXPECT_TRUE(t1 != t0);
    }

    // bool operator<(const MonoTime& lhs, const MonoTime& rhs);
    {
        MonoTime t0 = MonoTime::Now();
        MonoTime t1(t0 + MonoDelta::FromMilliseconds(100));
        ASSERT_TRUE(t0.ComesBefore(t1));
        ASSERT_FALSE(t1.ComesBefore(t0));
        EXPECT_TRUE(t0 < t1);
        EXPECT_FALSE(t1 < t0);
    }

    // bool operator<=(const MonoTime& lhs, const MonoTime& rhs);
    {
        MonoTime t00 = MonoTime::Now();
        MonoTime t01(t00);
        ASSERT_TRUE(t00.Equals(t00));
        ASSERT_TRUE(t00.Equals(t01));
        ASSERT_TRUE(t01.Equals(t00));
        ASSERT_TRUE(t01.Equals(t01));
        EXPECT_TRUE(t00 <= t00);
        EXPECT_TRUE(t00 <= t01);
        EXPECT_TRUE(t01 <= t00);
        EXPECT_TRUE(t01 <= t01);

        MonoTime t1(t00 + MonoDelta::FromMilliseconds(100));
        ASSERT_TRUE(t00.ComesBefore(t1));
        ASSERT_TRUE(t01.ComesBefore(t1));
        ASSERT_FALSE(t1.ComesBefore(t00));
        ASSERT_FALSE(t1.ComesBefore(t01));
        EXPECT_TRUE(t00 <= t1);
        EXPECT_TRUE(t01 <= t1);
        EXPECT_FALSE(t1 <= t00);
        EXPECT_FALSE(t1 <= t01);
    }

    // bool operator>(const MonoTime& lhs, const MonoTime& rhs);
    {
        MonoTime t0 = MonoTime::Now();
        MonoTime t1(t0 + MonoDelta::FromMilliseconds(100));
        ASSERT_TRUE(t0.ComesBefore(t1));
        ASSERT_FALSE(t1.ComesBefore(t0));
        EXPECT_TRUE(t0 < t1);
        EXPECT_FALSE(t1 < t0);
    }

    // bool operator>=(const MonoTime& lhs, const MonoTime& rhs);
    {
        MonoTime t00 = MonoTime::Now();
        MonoTime t01(t00);
        ASSERT_TRUE(t00.Equals(t00));
        ASSERT_TRUE(t00.Equals(t01));
        ASSERT_TRUE(t01.Equals(t00));
        ASSERT_TRUE(t01.Equals(t01));
        EXPECT_TRUE(t00 >= t00);
        EXPECT_TRUE(t00 >= t01);
        EXPECT_TRUE(t01 >= t00);
        EXPECT_TRUE(t01 >= t01);

        MonoTime t1(t00 + MonoDelta::FromMilliseconds(100));
        ASSERT_TRUE(t00.ComesBefore(t1));
        ASSERT_TRUE(t01.ComesBefore(t1));
        ASSERT_FALSE(t1.ComesBefore(t00));
        ASSERT_FALSE(t1.ComesBefore(t01));
        EXPECT_FALSE(t00 >= t1);
        EXPECT_FALSE(t01 >= t1);
        EXPECT_TRUE(t1 >= t00);
        EXPECT_TRUE(t1 >= t01);
    }

    // MonoDelta operator-(const MonoTime& t0, const MonoTime& t1);
    {
        const int64_t deltas[] = {100, -100};

        MonoTime tmp = MonoTime::Now();
        for (auto d : deltas) {
            MonoDelta delta = MonoDelta::FromMilliseconds(d);

            MonoTime start = tmp;
            tmp.AddDelta(delta);
            MonoTime end = tmp;
            MonoDelta delta_o = end - start;
            EXPECT_TRUE(delta.Equals(delta_o));
        }
    }

    // MonoTime operator+(const MonoTime& t, const MonoDelta& delta);
    {
        MonoTime start = MonoTime::Now();

        MonoDelta delta_0 = MonoDelta::FromMilliseconds(0);
        MonoTime end_0 = start + delta_0;
        EXPECT_TRUE(end_0.Equals(start));

        MonoDelta delta_1 = MonoDelta::FromMilliseconds(1);
        MonoTime end_1 = start + delta_1;
        EXPECT_TRUE(end_1 > end_0);
        end_0.AddDelta(delta_1);
        EXPECT_TRUE(end_0.Equals(end_1));
    }

    // MonoTime operator-(const MonoTime& t, const MonoDelta& delta);
    {
        MonoTime start = MonoTime::Now();

        MonoDelta delta_0 = MonoDelta::FromMilliseconds(0);
        MonoTime end_0 = start - delta_0;
        EXPECT_TRUE(end_0.Equals(start));

        MonoDelta delta_1 = MonoDelta::FromMilliseconds(1);
        MonoTime end_1 = start - delta_1;
        EXPECT_TRUE(end_1 < end_0);
        end_1.AddDelta(delta_1);
        EXPECT_TRUE(end_1.Equals(end_0));
    }
}

TEST(TestMonoTimePerf, TestMonoTimePerf) {
    alarm(360);
    DoTestMonoTimePerf();
    alarm(0);
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
