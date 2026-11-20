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

#include "common/object_pool.h"

#include <gtest/gtest.h>

#include <new>
#include <vector>

namespace doris {

namespace {

void fail_registration() {
    throw std::bad_alloc();
}

struct TrackedObject {
    static inline std::vector<int> destruction_order;
    explicit TrackedObject(int id) : id(id) {}
    ~TrackedObject() { destruction_order.push_back(id); }
    int id;
};

struct ReentrantObject {
    static inline ObjectPool* pool = nullptr;
    static inline int destroyed = 0;
    static inline bool unlocked = true;
    ~ReentrantObject() {
        ++destroyed;
        unlocked &= pool->size() == 0;
    }
};

} // namespace

TEST(ObjectPoolLifecycleTest, RegisteredObjectsAreDestroyedInReverseOrder) {
    TrackedObject::destruction_order.clear();
    ObjectPool pool;
    EXPECT_NE(pool.add(new TrackedObject(1)), nullptr);
    EXPECT_NE(pool.add(new TrackedObject(2)), nullptr);
    EXPECT_EQ(pool.size(), 2);
    pool.clear();
    EXPECT_EQ(TrackedObject::destruction_order, (std::vector<int> {2, 1}));
    pool.clear();
    EXPECT_EQ(TrackedObject::destruction_order.size(), 2);
}

TEST(ObjectPoolLifecycleTest, FailedScalarRegistrationReleasesAfterUnlock) {
    ObjectPool pool;
    ReentrantObject::pool = &pool;
    ReentrantObject::destroyed = 0;
    ReentrantObject::unlocked = true;
    ObjectPool::_before_register = fail_registration;
    EXPECT_THROW(pool.add(new ReentrantObject), std::bad_alloc);
    ObjectPool::_before_register = nullptr;
    EXPECT_EQ(pool.size(), 0);
    EXPECT_EQ(ReentrantObject::destroyed, 1);
    EXPECT_TRUE(ReentrantObject::unlocked);
}

TEST(ObjectPoolLifecycleTest, FailedArrayRegistrationReleasesEveryElementAfterUnlock) {
    ObjectPool pool;
    ReentrantObject::pool = &pool;
    ReentrantObject::destroyed = 0;
    ReentrantObject::unlocked = true;
    ObjectPool::_before_register = fail_registration;
    EXPECT_THROW(pool.add_array(new ReentrantObject[2]), std::bad_alloc);
    ObjectPool::_before_register = nullptr;
    EXPECT_EQ(pool.size(), 0);
    EXPECT_EQ(ReentrantObject::destroyed, 2);
    EXPECT_TRUE(ReentrantObject::unlocked);
}

} // namespace doris
