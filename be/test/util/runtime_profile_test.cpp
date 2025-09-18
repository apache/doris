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

#include "util/runtime_profile.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <sstream>

#include "common/exception.h"
#include "common/object_pool.h"

using namespace std;

namespace doris {

class RuntimeProfileTest : public testing::Test {};

TEST(RuntimeProfileTest, Basic) {
    RuntimeProfile profile_a("ProfileA");
    RuntimeProfile profile_a1("ProfileA1");
    RuntimeProfile profile_a2("ProfileA2");

    TRuntimeProfileTree thrift_profile;

    profile_a.add_child(&profile_a1, true);
    profile_a.add_child(&profile_a2, true);

    // Test Empty
    profile_a.to_thrift(&thrift_profile.nodes);
    EXPECT_EQ(thrift_profile.nodes.size(), 3);
    thrift_profile.nodes.clear();

    RuntimeProfile::Counter* counter_a;
    RuntimeProfile::Counter* counter_b;
    RuntimeProfile::Counter* counter_merged;

    // Updating/setting counter
    counter_a = profile_a.add_counter("A", TUnit::UNIT);
    EXPECT_TRUE(counter_a != nullptr);
    counter_a->update(10);
    counter_a->update(-5);
    EXPECT_EQ(counter_a->value(), 5);
    counter_a->set(1L);
    EXPECT_EQ(counter_a->value(), 1);

    counter_b = profile_a2.add_counter("B", TUnit::BYTES);
    EXPECT_TRUE(counter_b != nullptr);

    std::stringstream ss;
    // Serialize/deserialize
    profile_a.to_thrift(&thrift_profile.nodes, 2);
    profile_a.pretty_print(&ss, " ", 4);
    std::cout << "Profile A:\n" << ss.str() << std::endl;
    ss.clear();
    ASSERT_EQ(thrift_profile.nodes.size(), 3);
    // Reset stream.
    ss = std::stringstream();
    std::unique_ptr<RuntimeProfile> from_thrift = RuntimeProfile::from_thrift(thrift_profile);
    from_thrift->pretty_print(&ss, "", 4);
    std::cout << "From thrift profile:\n" << ss.str() << std::endl;
    ss.clear();

    counter_merged = from_thrift->get_counter("A");
    ASSERT_NE(counter_merged, nullptr);
    EXPECT_EQ(counter_merged->value(), 1);
    EXPECT_TRUE(from_thrift->get_counter("Not there") == nullptr);

    // merge
    RuntimeProfile merged_profile("Merged");
    merged_profile.merge(from_thrift.get());
    counter_merged = merged_profile.get_counter("A");
    EXPECT_EQ(counter_merged->value(), 1);

    // merge 2 more times, counters should get aggregated
    merged_profile.merge(from_thrift.get());
    merged_profile.merge(from_thrift.get());
    EXPECT_EQ(counter_merged->value(), 3);

    // update
    RuntimeProfile updated_profile("updated");
    updated_profile.update(thrift_profile);
    RuntimeProfile::Counter* counter_updated = updated_profile.get_counter("A");
    EXPECT_EQ(counter_updated->value(), 1);

    // update 2 more times, counters should stay the same
    updated_profile.update(thrift_profile);
    updated_profile.update(thrift_profile);
    EXPECT_EQ(counter_updated->value(), 1);
}

TEST(RuntimeProfileTest, ProtoBasic) {
    RuntimeProfile profile_a("ProfileA");
    RuntimeProfile profile_a1("ProfileA1");
    RuntimeProfile profile_a2("ProfileA2");

    profile_a.add_child(&profile_a1, true);
    profile_a.add_child(&profile_a2, true);

    PRuntimeProfileTree proto_profile;

    // Test Empty serialization
    profile_a.to_proto(&proto_profile);
    EXPECT_EQ(proto_profile.nodes_size(), 3);
    proto_profile.clear_nodes();

    RuntimeProfile::Counter* counter_a;
    RuntimeProfile::Counter* counter_b;
    RuntimeProfile::Counter* counter_merged;

    // Updating/setting counter
    counter_a = profile_a.add_counter("A", TUnit::UNIT);
    EXPECT_TRUE(counter_a != nullptr);
    counter_a->update(10);
    counter_a->update(-5);
    EXPECT_EQ(counter_a->value(), 5);
    counter_a->set(1L);
    EXPECT_EQ(counter_a->value(), 1);

    counter_b = profile_a2.add_counter("B", TUnit::BYTES);
    EXPECT_TRUE(counter_b != nullptr);

    std::stringstream ss;
    // Serialize to proto
    profile_a.to_proto(&proto_profile, 2);
    profile_a.pretty_print(&ss, " ", 4);
    std::cout << "Profile A:\n" << ss.str() << std::endl;
    ss.str("");
    ss.clear();

    ASSERT_EQ(proto_profile.nodes_size(), 3);

    // Deserialize from proto
    std::unique_ptr<RuntimeProfile> from_proto = RuntimeProfile::from_proto(proto_profile);
    from_proto->pretty_print(&ss, "", 4);
    std::cout << "From proto profile:\n" << ss.str() << std::endl;
    ss.str("");
    ss.clear();

    counter_merged = from_proto->get_counter("A");
    ASSERT_NE(counter_merged, nullptr);
    EXPECT_EQ(counter_merged->value(), 1);
    EXPECT_TRUE(from_proto->get_counter("Not there") == nullptr);

    // merge
    RuntimeProfile merged_profile("Merged");
    merged_profile.merge(from_proto.get());
    counter_merged = merged_profile.get_counter("A");
    EXPECT_EQ(counter_merged->value(), 1);

    // merge 2 more times, counters should get aggregated
    merged_profile.merge(from_proto.get());
    merged_profile.merge(from_proto.get());
    EXPECT_EQ(counter_merged->value(), 3);

    // update
    RuntimeProfile updated_profile("updated");
    updated_profile.update(proto_profile);
    RuntimeProfile::Counter* counter_updated = updated_profile.get_counter("A");
    EXPECT_EQ(counter_updated->value(), 1);

    // update 2 more times, counters should stay the same
    updated_profile.update(proto_profile);
    updated_profile.update(proto_profile);
    EXPECT_EQ(counter_updated->value(), 1);
}

void ValidateCounter(RuntimeProfile* profile, const string& name, int64_t value) {
    RuntimeProfile::Counter* counter = profile->get_counter(name);
    ASSERT_TRUE(counter != nullptr);
    EXPECT_EQ(counter->value(), value);
}

TEST(RuntimeProfileTest, MergeAndupdate) {
    // Create two trees.  Each tree has two children, one of which has the
    // same name in both trees.  Merging the two trees should result in 3
    // children, with the counters from the shared child aggregated.

    ObjectPool pool;
    RuntimeProfile profile1("Parent1");
    RuntimeProfile p1_child1("Child1");
    RuntimeProfile p1_child2("Child2");
    profile1.add_child(&p1_child1, true);
    profile1.add_child(&p1_child2, true);

    RuntimeProfile profile2("Parent2");
    RuntimeProfile p2_child1("Child1");
    RuntimeProfile p2_child3("Child3");
    profile2.add_child(&p2_child1, true);
    profile2.add_child(&p2_child3, true);

    // Create parent level counters
    RuntimeProfile::Counter* parent1_shared = profile1.add_counter("Parent Shared", TUnit::UNIT);
    RuntimeProfile::Counter* parent2_shared = profile2.add_counter("Parent Shared", TUnit::UNIT);
    RuntimeProfile::Counter* parent1_only = profile1.add_counter("Parent 1 Only", TUnit::UNIT);
    RuntimeProfile::Counter* parent2_only = profile2.add_counter("Parent 2 Only", TUnit::UNIT);
    parent1_shared->update(1);
    parent2_shared->update(3);
    parent1_only->update(2);
    parent2_only->update(5);

    // Create child level counters
    RuntimeProfile::Counter* p1_c1_shared = p1_child1.add_counter("Child1 Shared", TUnit::UNIT);
    RuntimeProfile::Counter* p1_c1_only =
            p1_child1.add_counter("Child1 Parent 1 Only", TUnit::UNIT);
    RuntimeProfile::Counter* p1_c2 = p1_child2.add_counter("Child2", TUnit::UNIT);
    RuntimeProfile::Counter* p2_c1_shared = p2_child1.add_counter("Child1 Shared", TUnit::UNIT);
    RuntimeProfile::Counter* p2_c1_only =
            p1_child1.add_counter("Child1 Parent 2 Only", TUnit::UNIT);
    RuntimeProfile::Counter* p2_c3 = p2_child3.add_counter("Child3", TUnit::UNIT);
    p1_c1_shared->update(10);
    p1_c1_only->update(50);
    p2_c1_shared->update(20);
    p2_c1_only->update(100);
    p2_c3->update(30);
    p1_c2->update(40);

    // merge the two and validate
    TRuntimeProfileTree tprofile1;
    profile1.to_thrift(&tprofile1, 2);
    std::unique_ptr<RuntimeProfile> merged_profile = RuntimeProfile::from_thrift(tprofile1);
    merged_profile->merge(&profile2);
    std::stringstream ss;
    merged_profile->pretty_print(&ss);
    std::cout << "Merged profile:\n" << ss.str() << std::endl;
    EXPECT_EQ(4, merged_profile->num_counters());
    ValidateCounter(merged_profile.get(), "Parent Shared", 4);
    ValidateCounter(merged_profile.get(), "Parent 1 Only", 2);
    ValidateCounter(merged_profile.get(), "Parent 2 Only", 5);

    std::vector<RuntimeProfile*> children;
    merged_profile->get_children(&children);
    EXPECT_EQ(children.size(), 3);

    for (int i = 0; i < 3; ++i) {
        RuntimeProfile* profile = children[i];

        if (profile->name() == "Child1") {
            EXPECT_EQ(4, profile->num_counters());
            ValidateCounter(profile, "Child1 Shared", 30);
            ValidateCounter(profile, "Child1 Parent 1 Only", 50);
            ValidateCounter(profile, "Child1 Parent 2 Only", 100);
        } else if (profile->name() == "Child2") {
            EXPECT_EQ(2, profile->num_counters());
            ValidateCounter(profile, "Child2", 40);
        } else if (profile->name() == "Child3") {
            EXPECT_EQ(2, profile->num_counters());
            ValidateCounter(profile, "Child3", 30);
        } else {
            EXPECT_TRUE(false);
        }
    }

    // make sure we can print
    std::stringstream dummy;
    merged_profile->pretty_print(&dummy);

    // update profile2 w/ profile1 and validate
    profile2.update(tprofile1);
    EXPECT_EQ(4, profile2.num_counters());
    ValidateCounter(&profile2, "Parent Shared", 1);
    ValidateCounter(&profile2, "Parent 1 Only", 2);
    ValidateCounter(&profile2, "Parent 2 Only", 5);

    profile2.get_children(&children);
    EXPECT_EQ(children.size(), 3);

    for (int i = 0; i < 3; ++i) {
        RuntimeProfile* profile = children[i];

        if (profile->name() == "Child1") {
            EXPECT_EQ(4, profile->num_counters());
            ValidateCounter(profile, "Child1 Shared", 10);
            ValidateCounter(profile, "Child1 Parent 1 Only", 50);
            ValidateCounter(profile, "Child1 Parent 2 Only", 100);
        } else if (profile->name() == "Child2") {
            EXPECT_EQ(2, profile->num_counters());
            ValidateCounter(profile, "Child2", 40);
        } else if (profile->name() == "Child3") {
            EXPECT_EQ(2, profile->num_counters());
            ValidateCounter(profile, "Child3", 30);
        } else {
            EXPECT_TRUE(false);
        }
    }

    // make sure we can print
    profile2.pretty_print(&dummy);
}

TEST(RuntimeProfileTest, ProtoMergeAndUpdate) {
    ObjectPool pool;
    RuntimeProfile profile1("Parent1");
    RuntimeProfile p1_child1("Child1");
    RuntimeProfile p1_child2("Child2");
    profile1.add_child(&p1_child1, true);
    profile1.add_child(&p1_child2, true);

    RuntimeProfile profile2("Parent2");
    RuntimeProfile p2_child1("Child1");
    RuntimeProfile p2_child3("Child3");
    profile2.add_child(&p2_child1, true);
    profile2.add_child(&p2_child3, true);

    // Create parent-level counters
    RuntimeProfile::Counter* parent1_shared = profile1.add_counter("Parent Shared", TUnit::UNIT);
    RuntimeProfile::Counter* parent2_shared = profile2.add_counter("Parent Shared", TUnit::UNIT);
    RuntimeProfile::Counter* parent1_only = profile1.add_counter("Parent 1 Only", TUnit::UNIT);
    RuntimeProfile::Counter* parent2_only = profile2.add_counter("Parent 2 Only", TUnit::UNIT);
    parent1_shared->update(1);
    parent2_shared->update(3);
    parent1_only->update(2);
    parent2_only->update(5);

    // Create child-level counters
    RuntimeProfile::Counter* p1_c1_shared = p1_child1.add_counter("Child1 Shared", TUnit::UNIT);
    RuntimeProfile::Counter* p1_c1_only =
            p1_child1.add_counter("Child1 Parent 1 Only", TUnit::UNIT);
    RuntimeProfile::Counter* p1_c2 = p1_child2.add_counter("Child2", TUnit::UNIT);

    RuntimeProfile::Counter* p2_c1_shared = p2_child1.add_counter("Child1 Shared", TUnit::UNIT);
    RuntimeProfile::Counter* p2_c1_only =
            p1_child1.add_counter("Child1 Parent 2 Only", TUnit::UNIT);
    RuntimeProfile::Counter* p2_c3 = p2_child3.add_counter("Child3", TUnit::UNIT);

    p1_c1_shared->update(10);
    p1_c1_only->update(50);
    p2_c1_shared->update(20);
    p2_c1_only->update(100);
    p2_c3->update(30);
    p1_c2->update(40);

    // Serialize profile1 to proto
    PRuntimeProfileTree proto_profile1;
    profile1.to_proto(&proto_profile1, 2);

    // Deserialize from proto and merge with profile2
    std::unique_ptr<RuntimeProfile> merged_profile = RuntimeProfile::from_proto(proto_profile1);
    merged_profile->merge(&profile2);

    std::stringstream ss;
    merged_profile->pretty_print(&ss);
    std::cout << "Merged profile:\n" << ss.str() << std::endl;

    EXPECT_EQ(4, merged_profile->num_counters());
    ValidateCounter(merged_profile.get(), "Parent Shared", 4);
    ValidateCounter(merged_profile.get(), "Parent 1 Only", 2);
    ValidateCounter(merged_profile.get(), "Parent 2 Only", 5);

    std::vector<RuntimeProfile*> children;
    merged_profile->get_children(&children);
    EXPECT_EQ(children.size(), 3);

    for (RuntimeProfile* profile : children) {
        if (profile->name() == "Child1") {
            EXPECT_EQ(4, profile->num_counters());
            ValidateCounter(profile, "Child1 Shared", 30);
            ValidateCounter(profile, "Child1 Parent 1 Only", 50);
            ValidateCounter(profile, "Child1 Parent 2 Only", 100);
        } else if (profile->name() == "Child2") {
            EXPECT_EQ(2, profile->num_counters());
            ValidateCounter(profile, "Child2", 40);
        } else if (profile->name() == "Child3") {
            EXPECT_EQ(2, profile->num_counters());
            ValidateCounter(profile, "Child3", 30);
        } else {
            FAIL() << "Unexpected child profile name: " << profile->name();
        }
    }

    // Test update: update profile2 with profile1's proto tree
    profile2.update(proto_profile1);
    EXPECT_EQ(4, profile2.num_counters());
    ValidateCounter(&profile2, "Parent Shared", 1);
    ValidateCounter(&profile2, "Parent 1 Only", 2);
    ValidateCounter(&profile2, "Parent 2 Only", 5);

    profile2.get_children(&children);
    EXPECT_EQ(children.size(), 3);

    for (RuntimeProfile* profile : children) {
        if (profile->name() == "Child1") {
            EXPECT_EQ(4, profile->num_counters());
            ValidateCounter(profile, "Child1 Shared", 10);
            ValidateCounter(profile, "Child1 Parent 1 Only", 50);
            ValidateCounter(profile, "Child1 Parent 2 Only", 100);
        } else if (profile->name() == "Child2") {
            EXPECT_EQ(2, profile->num_counters());
            ValidateCounter(profile, "Child2", 40);
        } else if (profile->name() == "Child3") {
            EXPECT_EQ(2, profile->num_counters());
            ValidateCounter(profile, "Child3", 30);
        } else {
            FAIL() << "Unexpected child profile name: " << profile->name();
        }
    }

    // Ensure pretty_print doesn't crash
    std::stringstream dummy;
    profile2.pretty_print(&dummy);
}

TEST(RuntimeProfileTest, DerivedCounters) {
    ObjectPool pool;
    RuntimeProfile profile("Profile");
    RuntimeProfile::Counter* bytes_counter = profile.add_counter("bytes", TUnit::BYTES);
    RuntimeProfile::Counter* ticks_counter = profile.add_counter("ticks", TUnit::TIME_NS);
    // set to 1 sec
    ticks_counter->set(1000L * 1000L * 1000L);

    RuntimeProfile::DerivedCounter* throughput_counter = profile.add_derived_counter(
            "throughput", TUnit::BYTES,
            [bytes_counter, ticks_counter] {
                return RuntimeProfile::units_per_second(bytes_counter, ticks_counter);
            },
            RuntimeProfile::ROOT_COUNTER);

    bytes_counter->set(10L);
    EXPECT_EQ(throughput_counter->value(), 10);
    bytes_counter->set(20L);
    EXPECT_EQ(throughput_counter->value(), 20);
    ticks_counter->set(ticks_counter->value() / 2);
    EXPECT_EQ(throughput_counter->value(), 40);
}

TEST(RuntimeProfileTest, InfoStringTest) {
    ObjectPool pool;
    RuntimeProfile profile("Profile");
    EXPECT_TRUE(profile.get_info_string("Key") == nullptr);

    profile.add_info_string("Key", "Value");
    const string* value = profile.get_info_string("Key");
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(*value, "Value");

    // Convert it to thrift
    TRuntimeProfileTree tprofile;
    profile.to_thrift(&tprofile);

    // Convert it back
    std::unique_ptr<RuntimeProfile> from_thrift = RuntimeProfile::from_thrift(tprofile);
    value = from_thrift->get_info_string("Key");
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(*value, "Value");

    // Test update.
    RuntimeProfile update_dst_profile("Profile2");
    update_dst_profile.update(tprofile);
    value = update_dst_profile.get_info_string("Key");
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(*value, "Value");

    // update the original profile, convert it to thrift and update from the dst
    // profile
    profile.add_info_string("Key", "NewValue");
    profile.add_info_string("Foo", "Bar");
    EXPECT_EQ(*profile.get_info_string("Key"), "NewValue");
    EXPECT_EQ(*profile.get_info_string("Foo"), "Bar");
    profile.to_thrift(&tprofile);

    update_dst_profile.update(tprofile);
    EXPECT_EQ(*update_dst_profile.get_info_string("Key"), "NewValue");
    EXPECT_EQ(*update_dst_profile.get_info_string("Foo"), "Bar");
}

TEST(RuntimeProfileTest, ProtoInfoStringTest) {
    ObjectPool pool;
    RuntimeProfile profile("Profile");

    EXPECT_TRUE(profile.get_info_string("Key") == nullptr);

    profile.add_info_string("Key", "Value");
    const std::string* value = profile.get_info_string("Key");
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(*value, "Value");

    // Convert it to proto
    PRuntimeProfileTree pprofile;
    profile.to_proto(&pprofile);

    // Convert it back from proto
    std::unique_ptr<RuntimeProfile> from_proto = RuntimeProfile::from_proto(pprofile);
    value = from_proto->get_info_string("Key");
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(*value, "Value");

    // Test update
    RuntimeProfile update_dst_profile("Profile2");
    update_dst_profile.update(pprofile);
    value = update_dst_profile.get_info_string("Key");
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(*value, "Value");

    // Modify original profile, convert again, and update dst
    profile.add_info_string("Key", "NewValue");
    profile.add_info_string("Foo", "Bar");
    EXPECT_EQ(*profile.get_info_string("Key"), "NewValue");
    EXPECT_EQ(*profile.get_info_string("Foo"), "Bar");

    profile.to_proto(&pprofile);
    update_dst_profile.update(pprofile);

    EXPECT_EQ(*update_dst_profile.get_info_string("Key"), "NewValue");
    EXPECT_EQ(*update_dst_profile.get_info_string("Foo"), "Bar");
}

// This case could be added back when we fixed the issue.
// TEST(RuntimeProfileTest, AddSameCounter) {
//     RuntimeProfile profile("Profile");
//     RuntimeProfile::Counter* counter = profile.add_counter("counter", TUnit::UNIT);
//     counter->set(10L);
//     EXPECT_EQ(counter->value(), 10L);

//     // Adding the same counter again should fail.
//     EXPECT_THROW(profile.add_counter("counter", TUnit::UNIT), doris::Exception);
// }

TEST(RuntimeProfileTest, TestGetChild) {
    // Create root profile
    RuntimeProfile root("Root");

    // Test get non-existing child
    ASSERT_EQ(nullptr, root.get_child("NonExistingChild"));

    // Create child1 and verify get_child
    RuntimeProfile* child1 = root.create_child("Child1");
    ASSERT_NE(nullptr, child1);
    ASSERT_EQ(child1, root.get_child("Child1"));

    // Create child2 and verify get_child
    RuntimeProfile* child2 = root.create_child("Child2");
    ASSERT_NE(nullptr, child2);
    ASSERT_EQ(child2, root.get_child("Child2"));

    // Create nested child and verify get_child
    RuntimeProfile* grandchild = child1->create_child("GrandChild");
    ASSERT_NE(nullptr, grandchild);
    ASSERT_EQ(grandchild, child1->get_child("GrandChild"));
    ASSERT_EQ(nullptr, root.get_child("GrandChild")); // Cannot get grandchild directly from root

    // Verify original children still accessible
    ASSERT_EQ(child1, root.get_child("Child1"));
    ASSERT_EQ(child2, root.get_child("Child2"));
}

} // namespace doris
