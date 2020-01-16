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

#include "runtime/mem_tracker.h"

#include <gtest/gtest.h>

#include "util/metrics.h"
#include "util/logging.h"

namespace doris {

TEST(MemTestTest, SingleTrackerNoLimit) {
    MemTracker t(-1);
    EXPECT_FALSE(t.has_limit());
    t.consume(10);
    EXPECT_EQ(t.consumption(), 10);
    t.consume(10);
    EXPECT_EQ(t.consumption(), 20);
    t.release(15);
    EXPECT_EQ(t.consumption(), 5);
    EXPECT_FALSE(t.limit_exceeded());
}

TEST(MemTestTest, SingleTrackerWithLimit) {
    MemTracker t(11);
    EXPECT_TRUE(t.has_limit());
    t.consume(10);
    EXPECT_EQ(t.consumption(), 10);
    EXPECT_FALSE(t.limit_exceeded());
    t.consume(10);
    EXPECT_EQ(t.consumption(), 20);
    EXPECT_TRUE(t.limit_exceeded());
    t.release(15);
    EXPECT_EQ(t.consumption(), 5);
    EXPECT_FALSE(t.limit_exceeded());
}

#if 0
TEST(MemTestTest, ConsumptionMetric) {
    TMetricDef md;
    md.__set_key("test");
    md.__set_units(TUnit::BYTES);
    md.__set_kind(TMetricKind::GAUGE);
    UIntGauge metric(md, 0);
    EXPECT_EQ(metric.value(), 0);

    MemTracker t(&metric, 100, -1, "");
    EXPECT_TRUE(t.has_limit());
    EXPECT_EQ(t.consumption(), 0);

    // consume()/release() arguments have no effect
    t.consume(150);
    EXPECT_EQ(t.consumption(), 0);
    EXPECT_EQ(t.peak_consumption(), 0);
    EXPECT_FALSE(t.limit_exceeded());
    t.release(5);
    EXPECT_EQ(t.consumption(), 0);
    EXPECT_EQ(t.peak_consumption(), 0);
    EXPECT_FALSE(t.limit_exceeded());

    metric.Increment(10);
    // _consumption is only updated with _consumption_metric after calls to
    // consume()/release() with a non-zero value
    t.consume(1);
    EXPECT_EQ(t.consumption(), 10);
    EXPECT_EQ(t.peak_consumption(), 10);
    metric.Increment(-5);
    t.consume(-1);
    EXPECT_EQ(t.consumption(), 5);
    EXPECT_EQ(t.peak_consumption(), 10);
    EXPECT_FALSE(t.limit_exceeded());
    metric.Increment(150);
    t.consume(1);
    EXPECT_EQ(t.consumption(), 155);
    EXPECT_EQ(t.peak_consumption(), 155);
    EXPECT_TRUE(t.limit_exceeded());
    metric.Increment(-150);
    t.consume(-1);
    EXPECT_EQ(t.consumption(), 5);
    EXPECT_EQ(t.peak_consumption(), 155);
    EXPECT_FALSE(t.limit_exceeded());
    // _consumption is not updated when consume()/release() is called with a zero value
    metric.Increment(10);
    t.consume(0);
    EXPECT_EQ(t.consumption(), 5);
    EXPECT_EQ(t.peak_consumption(), 155);
    EXPECT_FALSE(t.limit_exceeded());
}
#endif // #end #if 0

TEST(MemTestTest, TrackerHierarchy) {
    MemTracker p(100);
    MemTracker c1(80, "", &p);
    MemTracker c2(50, "", &p);

    // everything below limits
    c1.consume(60);
    EXPECT_EQ(c1.consumption(), 60);
    EXPECT_FALSE(c1.limit_exceeded());
    EXPECT_FALSE(c1.any_limit_exceeded());
    EXPECT_EQ(c2.consumption(), 0);
    EXPECT_FALSE(c2.limit_exceeded());
    EXPECT_FALSE(c2.any_limit_exceeded());
    EXPECT_EQ(p.consumption(), 60);
    EXPECT_FALSE(p.limit_exceeded());
    EXPECT_FALSE(p.any_limit_exceeded());

    // p goes over limit
    c2.consume(50);
    EXPECT_EQ(c1.consumption(), 60);
    EXPECT_FALSE(c1.limit_exceeded());
    EXPECT_TRUE(c1.any_limit_exceeded());
    EXPECT_EQ(c2.consumption(), 50);
    EXPECT_FALSE(c2.limit_exceeded());
    EXPECT_TRUE(c2.any_limit_exceeded());
    EXPECT_EQ(p.consumption(), 110);
    EXPECT_TRUE(p.limit_exceeded());

    // c2 goes over limit, p drops below limit
    c1.release(20);
    c2.consume(10);
    EXPECT_EQ(c1.consumption(), 40);
    EXPECT_FALSE(c1.limit_exceeded());
    EXPECT_FALSE(c1.any_limit_exceeded());
    EXPECT_EQ(c2.consumption(), 60);
    EXPECT_TRUE(c2.limit_exceeded());
    EXPECT_TRUE(c2.any_limit_exceeded());
    EXPECT_EQ(p.consumption(), 100);
    EXPECT_FALSE(p.limit_exceeded());
}

TEST(MemTestTest, TrackerHierarchyTryConsume) {
    MemTracker p(100);
    MemTracker c1(80, "", &p);
    MemTracker c2(50, "", &p);

    // everything below limits
    bool consumption = c1.try_consume(60);
    EXPECT_EQ(consumption, true);
    EXPECT_EQ(c1.consumption(), 60);
    EXPECT_FALSE(c1.limit_exceeded());
    EXPECT_FALSE(c1.any_limit_exceeded());
    EXPECT_EQ(c2.consumption(), 0);
    EXPECT_FALSE(c2.limit_exceeded());
    EXPECT_FALSE(c2.any_limit_exceeded());
    EXPECT_EQ(p.consumption(), 60);
    EXPECT_FALSE(p.limit_exceeded());
    EXPECT_FALSE(p.any_limit_exceeded());

    // p goes over limit
    consumption = c2.try_consume(50);
    EXPECT_EQ(consumption, true);
    EXPECT_EQ(c1.consumption(), 60);
    EXPECT_FALSE(c1.limit_exceeded());
    EXPECT_FALSE(c1.any_limit_exceeded());
    EXPECT_EQ(c2.consumption(), 0);
    EXPECT_FALSE(c2.limit_exceeded());
    EXPECT_FALSE(c2.any_limit_exceeded());
    EXPECT_EQ(p.consumption(), 60);
    EXPECT_FALSE(p.limit_exceeded());
    EXPECT_FALSE(p.any_limit_exceeded());

    // c2 goes over limit, p drops below limit
    c1.release(20);
    c2.consume(10);
    EXPECT_EQ(c1.consumption(), 40);
    EXPECT_FALSE(c1.limit_exceeded());
    EXPECT_FALSE(c1.any_limit_exceeded());
    EXPECT_EQ(c2.consumption(), 10);
    EXPECT_FALSE(c2.limit_exceeded());
    EXPECT_FALSE(c2.any_limit_exceeded());
    EXPECT_EQ(p.consumption(), 50);
    EXPECT_FALSE(p.limit_exceeded());
}

#if 0
class GcFunctionHelper {
    public:
        static const int NUM_RELEASE_BYTES = 1;

        GcFunctionHelper(MemTracker* tracker) : _tracker(tracker) { }

        ~GcFunctionHelper() {}

        void gc_func() { _tracker->release(NUM_RELEASE_BYTES); }

    private:
        MemTracker* _tracker;
};

TEST(MemTestTest, GcFunctions) {
    MemTracker t(10);
    ASSERT_TRUE(t.has_limit());

    t.consume(9);
    EXPECT_FALSE(t.limit_exceeded());

    // Test TryConsume()
    EXPECT_FALSE(t.TryConsume(2));
    EXPECT_EQ(t.consumption(), 9);
    EXPECT_FALSE(t.limit_exceeded());

    // Attach GcFunction that releases 1 byte
    GcFunctionHelper gc_func_helper(&t);
    t.AddGcFunction(boost::bind(&GcFunctionHelper::gc_func, &gc_func_helper));
    EXPECT_TRUE(t.TryConsume(2));
    EXPECT_EQ(t.consumption(), 10);
    EXPECT_FALSE(t.limit_exceeded());

    // GcFunction will be called even though TryConsume() fails
    EXPECT_FALSE(t.TryConsume(2));
    EXPECT_EQ(t.consumption(), 9);
    EXPECT_FALSE(t.limit_exceeded());

    // GcFunction won't be called
    EXPECT_TRUE(t.TryConsume(1));
    EXPECT_EQ(t.consumption(), 10);
    EXPECT_FALSE(t.limit_exceeded());

    // Test limit_exceeded()
    t.consume(1);
    EXPECT_EQ(t.consumption(), 11);
    EXPECT_FALSE(t.limit_exceeded());
    EXPECT_EQ(t.consumption(), 10);

    // Add more GcFunctions, test that we only call them until the limit is no longer
    // exceeded
    GcFunctionHelper gc_func_helper2(&t);
    t.AddGcFunction(boost::bind(&GcFunctionHelper::gc_func, &gc_func_helper2));
    GcFunctionHelper gc_func_helper3(&t);
    t.AddGcFunction(boost::bind(&GcFunctionHelper::gc_func, &gc_func_helper3));
    t.consume(1);
    EXPECT_EQ(t.consumption(), 11);
    EXPECT_FALSE(t.limit_exceeded());
    EXPECT_EQ(t.consumption(), 10);
}
#endif // enf #if 0

} // end namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
