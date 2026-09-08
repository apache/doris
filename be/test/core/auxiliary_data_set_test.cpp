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

#include "core/auxiliary_data_set.h"

#include <gtest/gtest.h>

#include <memory>

namespace doris {

namespace {
struct Payload {
    explicit Payload(int* destroy_count_, int value_)
            : destroy_count(destroy_count_), value(value_) {}

    ~Payload() { ++*destroy_count; }

    int* destroy_count;
    int value;
};
} // namespace

TEST(AuxiliaryDataSetTest, AddOwnerReturnsTypedPointerAndKeepsOwnerAlive) {
    AuxiliaryDataSet auxiliary_data;
    int destroy_count = 0;
    auto owner = std::make_shared<Payload>(&destroy_count, 42);
    std::weak_ptr<Payload> weak_owner = owner;

    auto* payload = auxiliary_data.add_owner(owner);
    ASSERT_EQ(payload, owner.get());
    EXPECT_EQ(payload->value, 42);

    owner.reset();
    ASSERT_FALSE(weak_owner.expired());
    EXPECT_EQ(payload->value, 42);

    auxiliary_data.clear();
    EXPECT_TRUE(weak_owner.expired());
    EXPECT_EQ(destroy_count, 1);
}

TEST(AuxiliaryDataSetTest, AddOwnerDeduplicatesBySharedOwnership) {
    AuxiliaryDataSet auxiliary_data;
    int destroy_count = 0;
    auto owner = std::make_shared<Payload>(&destroy_count, 7);
    std::shared_ptr<int> alias(owner, &owner->value);

    auxiliary_data.add_owner(owner);
    EXPECT_EQ(owner.use_count(), 3);

    auxiliary_data.add_owner(alias);
    EXPECT_EQ(owner.use_count(), 3);

    auxiliary_data.clear();
    EXPECT_EQ(owner.use_count(), 2);
    EXPECT_EQ(destroy_count, 0);

    alias.reset();
    owner.reset();
    EXPECT_EQ(destroy_count, 1);
}

TEST(AuxiliaryDataSetTest, AddOwnersFromKeepsSourceOwnersAndDeduplicates) {
    AuxiliaryDataSet source;
    AuxiliaryDataSet target;
    int destroy_count = 0;
    auto owner = std::make_shared<Payload>(&destroy_count, 99);
    std::weak_ptr<Payload> weak_owner = owner;

    source.add_owner(owner);
    EXPECT_EQ(owner.use_count(), 2);

    target.add_owners_from(source);
    EXPECT_EQ(owner.use_count(), 3);

    target.add_owners_from(source);
    EXPECT_EQ(owner.use_count(), 3);

    source.clear();
    owner.reset();
    ASSERT_FALSE(weak_owner.expired());

    target.clear();
    EXPECT_TRUE(weak_owner.expired());
    EXPECT_EQ(destroy_count, 1);
}

} // namespace doris
