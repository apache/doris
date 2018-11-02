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

#include <vector>

#include <gtest/gtest.h>

#include "olap/lru_cache.h"
#include "util/logging.h"

using namespace doris;
using namespace std;

namespace doris {

void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    memcpy(buf, &value, sizeof(value));
    dst->append(buf, sizeof(buf));
}

uint32_t DecodeFixed32(const char* ptr) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}

// Conversions between numeric keys/values and the types expected by Cache.
const CacheKey EncodeKey(std::string* result, int k) {
    PutFixed32(result, k);
    return CacheKey(result->c_str(), result->size());
}

static int DecodeKey(const CacheKey& k) {
    assert(k.size() == 4);
    return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) {
    return reinterpret_cast<void*>(v);
}
static int DecodeValue(void* v) {
    return reinterpret_cast<uintptr_t>(v);
}

class CacheTest : public testing::Test {
public:
    static CacheTest* _s_current;

    static void Deleter(const CacheKey& key, void* v) {
        _s_current->_deleted_keys.push_back(DecodeKey(key));
        _s_current->_deleted_values.push_back(DecodeValue(v));
    }

    static const int kCacheSize = 1000;
    std::vector<int> _deleted_keys;
    std::vector<int> _deleted_values;
    Cache* _cache;

    CacheTest() : _cache(new_lru_cache(kCacheSize)) {
        _s_current = this;
    }

    ~CacheTest() {
        delete _cache;
    }

    int Lookup(int key) {
        std::string result;
        Cache::Handle* handle = _cache->lookup(EncodeKey(&result, key));
        const int r = (handle == NULL) ? -1 : DecodeValue(_cache->value(handle));

        if (handle != NULL) {
            _cache->release(handle);
        }

        return r;
    }

    void Insert(int key, int value, int charge) {
        std::string result;
        _cache->release(_cache->insert(EncodeKey(&result, key), EncodeValue(value), charge,
                                       &CacheTest::Deleter));
    }

    void Erase(int key) {
        std::string result;
        _cache->erase(EncodeKey(&result, key));
    }

    void SetUp() {

    }

    void TearDown() {

    }

};
CacheTest* CacheTest::_s_current;

TEST_F(CacheTest, HitAndMiss) {
    ASSERT_EQ(-1, Lookup(100));

    Insert(100, 101, 1);
    ASSERT_EQ(101, Lookup(100));
    ASSERT_EQ(-1,  Lookup(200));
    ASSERT_EQ(-1,  Lookup(300));

    Insert(200, 201, 1);
    ASSERT_EQ(101, Lookup(100));
    ASSERT_EQ(201, Lookup(200));
    ASSERT_EQ(-1,  Lookup(300));

    Insert(100, 102, 1);
    ASSERT_EQ(102, Lookup(100));
    ASSERT_EQ(201, Lookup(200));
    ASSERT_EQ(-1,  Lookup(300));

    ASSERT_EQ(1, _deleted_keys.size());
    ASSERT_EQ(100, _deleted_keys[0]);
    ASSERT_EQ(101, _deleted_values[0]);
}

TEST_F(CacheTest, Erase) {
    Erase(200);
    ASSERT_EQ(0, _deleted_keys.size());

    Insert(100, 101, 1);
    Insert(200, 201, 1);
    Erase(100);
    ASSERT_EQ(-1,  Lookup(100));
    ASSERT_EQ(201, Lookup(200));
    ASSERT_EQ(1, _deleted_keys.size());
    ASSERT_EQ(100, _deleted_keys[0]);
    ASSERT_EQ(101, _deleted_values[0]);

    Erase(100);
    ASSERT_EQ(-1,  Lookup(100));
    ASSERT_EQ(201, Lookup(200));
    ASSERT_EQ(1, _deleted_keys.size());
}

TEST_F(CacheTest, EntriesArePinned) {
    Insert(100, 101, 1);
    std::string result1;
    Cache::Handle* h1 = _cache->lookup(EncodeKey(&result1, 100));
    ASSERT_EQ(101, DecodeValue(_cache->value(h1)));

    Insert(100, 102, 1);
    std::string result2;
    Cache::Handle* h2 = _cache->lookup(EncodeKey(&result2, 100));
    ASSERT_EQ(102, DecodeValue(_cache->value(h2)));
    ASSERT_EQ(0, _deleted_keys.size());

    _cache->release(h1);
    ASSERT_EQ(1, _deleted_keys.size());
    ASSERT_EQ(100, _deleted_keys[0]);
    ASSERT_EQ(101, _deleted_values[0]);

    Erase(100);
    ASSERT_EQ(-1, Lookup(100));
    ASSERT_EQ(1, _deleted_keys.size());

    _cache->release(h2);
    ASSERT_EQ(2, _deleted_keys.size());
    ASSERT_EQ(100, _deleted_keys[1]);
    ASSERT_EQ(102, _deleted_values[1]);
}

TEST_F(CacheTest, EvictionPolicy) {
    Insert(100, 101, 1);
    Insert(200, 201, 1);

    // Frequently used entry must be kept around
    for (int i = 0; i < kCacheSize + 100; i++) {
        Insert(1000 + i, 2000 + i, 1);
        ASSERT_EQ(2000 + i, Lookup(1000 + i));
        ASSERT_EQ(101, Lookup(100));
    }

    ASSERT_EQ(101, Lookup(100));
    ASSERT_EQ(-1, Lookup(200));
}

TEST_F(CacheTest, HeavyEntries) {
    // Add a bunch of light and heavy entries and then count the combined
    // size of items still in the cache, which must be approximately the
    // same as the total capacity.
    const int kLight = 1;
    const int kHeavy = 10;
    int added = 0;
    int index = 0;

    while (added < 2 * kCacheSize) {
        const int weight = (index & 1) ? kLight : kHeavy;
        Insert(index, 1000 + index, weight);
        added += weight;
        index++;
    }

    int cached_weight = 0;

    for (int i = 0; i < index; i++) {
        const int weight = (i & 1 ? kLight : kHeavy);
        int r = Lookup(i);

        if (r >= 0) {
            cached_weight += weight;
            ASSERT_EQ(1000 + i, r);
        }
    }

    ASSERT_LE(cached_weight, kCacheSize + kCacheSize / 10);
}

TEST_F(CacheTest, NewId) {
    uint64_t a = _cache->new_id();
    uint64_t b = _cache->new_id();
    ASSERT_NE(a, b);
}

}  // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
