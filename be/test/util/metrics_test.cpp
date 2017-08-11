// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "util/metrics.h"
#include "util/non-primitive-metrics.h"
#include <gtest/gtest.h>
#include <boost/scoped_ptr.hpp>

namespace palo {

class MetricsTest : public testing::Test {
public:
    Metrics* metrics() {
        return _metrics.get();
    }
    ~MetricsTest() {
    }
    MetricsTest() : _metrics(new Metrics()) {
        _bool_metric = _metrics->create_and_register_primitive_metric("bool", false);
        _int_metric = _metrics->create_and_register_primitive_metric("int", 0L);
        _double_metric = _metrics->create_and_register_primitive_metric("double",
                         1.23);
        _string_metric = _metrics->create_and_register_primitive_metric("string",
                         string("hello world"));

        vector<int> items;
        items.push_back(1);
        items.push_back(2);
        items.push_back(3);
        _list_metric = _metrics->register_metric(new ListMetric<int>("list", items));
        set<int> item_set;
        item_set.insert(4);
        item_set.insert(5);
        item_set.insert(6);
        _set_metric = _metrics->register_metric(new SetMetric<int>("set", item_set));

        set<string> string_set;
        string_set.insert("one");
        string_set.insert("two");
        _string_set_metric = _metrics->register_metric(new SetMetric<string>("string_set",
                             string_set));
    }
private:
    Metrics::BooleanMetric* _bool_metric;
    Metrics::IntMetric* _int_metric;
    Metrics::DoubleMetric* _double_metric;
    Metrics::StringMetric* _string_metric;

    ListMetric<int>* _list_metric;
    SetMetric<int>* _set_metric;
    SetMetric<string>* _string_set_metric; // For quote testing

    boost::scoped_ptr<Metrics> _metrics;
};

TEST_F(MetricsTest, IntMetrics) {
    EXPECT_NE(metrics()->DebugString().find("int:0"), string::npos);
    _int_metric->update(3);
    EXPECT_NE(metrics()->DebugString().find("int:3"), string::npos);
}

TEST_F(MetricsTest, DoubleMetrics) {
    EXPECT_NE(metrics()->DebugString().find("double:1.23"), string::npos);
    _double_metric->update(2.34);
    EXPECT_NE(metrics()->DebugString().find("double:2.34"), string::npos);
}

TEST_F(MetricsTest, StringMetrics) {
    EXPECT_NE(metrics()->DebugString().find("string:hello world"), string::npos);
    _string_metric->update("foo bar");
    EXPECT_NE(metrics()->DebugString().find("string:foo bar"), string::npos);
}

TEST_F(MetricsTest, BooleanMetrics) {
    EXPECT_NE(metrics()->DebugString().find("bool:0"), string::npos);
    _bool_metric->update(true);
    EXPECT_NE(metrics()->DebugString().find("bool:1"), string::npos);
}

TEST_F(MetricsTest, ListMetrics) {
    EXPECT_NE(metrics()->DebugString().find("list:[1, 2, 3]"), string::npos);
    _list_metric->update(vector<int>());
    EXPECT_NE(metrics()->DebugString().find("list:[]"), string::npos);
}

TEST_F(MetricsTest, SetMetrics) {
    EXPECT_NE(metrics()->DebugString().find("set:[4, 5, 6]"), string::npos);
    _set_metric->Add(7);
    _set_metric->Add(7);
    _set_metric->Remove(4);
    _set_metric->Remove(4);
    EXPECT_NE(metrics()->DebugString().find("set:[5, 6, 7]"), string::npos);
}

TEST_F(MetricsTest, TestAndSet) {
    _int_metric->update(1);
    // Expect update to fail
    EXPECT_EQ(_int_metric->TestAndSet(5, 0), 1);
    EXPECT_EQ(_int_metric->value(), 1);

    // Successful update
    EXPECT_EQ(_int_metric->TestAndSet(5, 1), 1);
    EXPECT_EQ(_int_metric->value(), 5);
}

TEST_F(MetricsTest, increment) {
    _int_metric->update(1);
    EXPECT_EQ(_int_metric->increment(10), 11);
    EXPECT_EQ(_int_metric->value(), 11);
}

TEST_F(MetricsTest, JsonQuoting) {
    // Strings should be quoted in Json output
    EXPECT_NE(metrics()->debug_string_json().find("\"string\": \"hello world\""),
              string::npos);

    // Other types should not be quoted
    EXPECT_NE(metrics()->debug_string_json().find("\"bool\": 0"), string::npos);

    // Strings in sets should be quoted
    EXPECT_NE(metrics()->debug_string_json().find("\"string_set\": [\"one\", \"two\"]"),
              string::npos);

    // Other types in sets should not be quoted
    EXPECT_NE(metrics()->debug_string_json().find("\"set\": [4, 5, 6]"), string::npos);
}
}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("PALO_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
