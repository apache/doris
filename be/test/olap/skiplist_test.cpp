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

#include "olap/skiplist.h"

#include <gtest/gtest.h>

#include <set>
#include <thread>

#include "olap/schema.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "testutil/test_util.h"
#include "util/hash_util.hpp"
#include "util/priority_thread_pool.hpp"
#include "util/random.h"

namespace doris {

typedef uint64_t Key;
const int random_seed = 301;

struct TestComparator {
    int operator()(const Key& a, const Key& b) const {
        if (a < b) {
            return -1;
        } else if (a > b) {
            return +1;
        } else {
            return 0;
        }
    }
};

class SkipTest : public testing::Test {};

TEST_F(SkipTest, Empty) {
    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));

    TestComparator* cmp = new TestComparator();
    SkipList<Key, TestComparator> list(cmp, mem_pool.get(), false);
    EXPECT_TRUE(!list.Contains(10));

    SkipList<Key, TestComparator>::Iterator iter(&list);
    EXPECT_TRUE(!iter.Valid());
    iter.SeekToFirst();
    EXPECT_TRUE(!iter.Valid());
    iter.Seek(100);
    EXPECT_TRUE(!iter.Valid());
    iter.SeekToLast();
    EXPECT_TRUE(!iter.Valid());
    delete cmp;
}

TEST_F(SkipTest, InsertAndLookup) {
    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));

    const int N = 2000;
    const int R = 5000;
    Random rnd(1000);
    std::set<Key> keys;
    TestComparator* cmp = new TestComparator();
    SkipList<Key, TestComparator> list(cmp, mem_pool.get(), false);
    for (int i = 0; i < N; i++) {
        Key key = rnd.Next() % R;
        if (keys.insert(key).second) {
            bool overwritten = false;
            list.Insert(key, &overwritten);
        }
    }

    for (int i = 0; i < R; i++) {
        if (list.Contains(i)) {
            EXPECT_EQ(keys.count(i), 1);
        } else {
            EXPECT_EQ(keys.count(i), 0);
        }
    }

    // Simple iterator tests
    {
        SkipList<Key, TestComparator>::Iterator iter(&list);
        EXPECT_TRUE(!iter.Valid());

        iter.Seek(0);
        EXPECT_TRUE(iter.Valid());
        EXPECT_EQ(*(keys.begin()), iter.key());

        iter.SeekToFirst();
        EXPECT_TRUE(iter.Valid());
        EXPECT_EQ(*(keys.begin()), iter.key());

        iter.SeekToLast();
        EXPECT_TRUE(iter.Valid());
        EXPECT_EQ(*(keys.rbegin()), iter.key());
    }

    // Forward iteration test
    for (int i = 0; i < R; i++) {
        SkipList<Key, TestComparator>::Iterator iter(&list);
        iter.Seek(i);

        // Compare against model iterator
        std::set<Key>::iterator model_iter = keys.lower_bound(i);
        for (int j = 0; j < 3; j++) {
            if (model_iter == keys.end()) {
                EXPECT_TRUE(!iter.Valid());
                break;
            } else {
                EXPECT_TRUE(iter.Valid());
                EXPECT_EQ(*model_iter, iter.key());
                ++model_iter;
                iter.Next();
            }
        }
    }

    // Backward iteration test
    {
        SkipList<Key, TestComparator>::Iterator iter(&list);
        iter.SeekToLast();

        // Compare against model iterator
        for (std::set<Key>::reverse_iterator model_iter = keys.rbegin(); model_iter != keys.rend();
             ++model_iter) {
            EXPECT_TRUE(iter.Valid());
            EXPECT_EQ(*model_iter, iter.key());
            iter.Prev();
        }
        EXPECT_TRUE(!iter.Valid());
    }
    delete cmp;
}

// Only non-DUP model will use Find() and InsertWithHint().
TEST_F(SkipTest, InsertWithHintNoneDupModel) {
    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));

    const int N = 2000;
    const int R = 5000;
    Random rnd(1000);
    std::set<Key> keys;
    TestComparator* cmp = new TestComparator();
    SkipList<Key, TestComparator> list(cmp, mem_pool.get(), false);
    SkipList<Key, TestComparator>::Hint hint;
    for (int i = 0; i < N; i++) {
        Key key = rnd.Next() % R;
        bool is_exist = list.Find(key, &hint);
        if (keys.insert(key).second) {
            EXPECT_FALSE(is_exist);
            list.InsertWithHint(key, is_exist, &hint);
        } else {
            EXPECT_TRUE(is_exist);
        }
    }

    for (int i = 0; i < R; i++) {
        if (list.Contains(i)) {
            EXPECT_EQ(keys.count(i), 1);
        } else {
            EXPECT_EQ(keys.count(i), 0);
        }
    }
    delete cmp;
}

// We want to make sure that with a single writer and multiple
// concurrent readers (with no synchronization other than when a
// reader's iterator is created), the reader always observes all the
// data that was present in the skip list when the iterator was
// constructor.  Because insertions are happening concurrently, we may
// also observe new values that were inserted since the iterator was
// constructed, but we should never miss any values that were present
// at iterator construction time.
//
// We generate multi-part keys:
//     <key,gen,hash>
// where:
//     key is in range [0..K-1]
//     gen is a generation number for key
//     hash is hash(key,gen)
//
// The insertion code picks a random key, sets gen to be 1 + the last
// generation number inserted for that key, and sets hash to Hash(key,gen).
//
// At the beginning of a read, we snapshot the last inserted
// generation number for each key.  We then iterate, including random
// calls to Next() and Seek().  For every key we encounter, we
// check that it is either expected given the initial snapshot or has
// been concurrently added since the iterator started.
class ConcurrentTest {
private:
    static const uint32_t K = 4;

    static uint64_t key(Key key) { return (key >> 40); }
    static uint64_t gen(Key key) { return (key >> 8) & 0xffffffffu; }
    static uint64_t hash(Key key) { return key & 0xff; }

    static uint64_t hash_numbers(uint64_t k, uint64_t g) {
        uint64_t data[2] = {k, g};
        return HashUtil::hash(reinterpret_cast<char*>(data), sizeof(data), 0);
    }

    static Key make_key(uint64_t k, uint64_t g) {
        EXPECT_EQ(sizeof(Key), sizeof(uint64_t));
        EXPECT_LE(k, K); // We sometimes pass K to seek to the end of the skiplist
        EXPECT_LE(g, 0xffffffffu);
        return ((k << 40) | (g << 8) | (hash_numbers(k, g) & 0xff));
    }

    static bool is_valid_key(Key k) { return hash(k) == (hash_numbers(key(k), gen(k)) & 0xff); }

    static Key random_target(Random* rnd) {
        switch (rnd->Next() % 10) {
        case 0:
            // Seek to beginning
            return make_key(0, 0);
        case 1:
            // Seek to end
            return make_key(K, 0);
        default:
            // Seek to middle
            return make_key(rnd->Next() % K, 0);
        }
    }

    // Per-key generation
    struct State {
        std::atomic<int> generation[K];
        void set(int k, int v) { generation[k].store(v, std::memory_order_release); }
        int get(int k) { return generation[k].load(std::memory_order_acquire); }

        State() {
            for (int k = 0; k < K; k++) {
                set(k, 0);
            }
        }
    };

    // Current state of the test
    State _current;

    std::shared_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::shared_ptr<TestComparator> _comparator;
    // SkipList is not protected by _mu.  We just use a single writer
    // thread to modify it.
    SkipList<Key, TestComparator> _list;

public:
    ConcurrentTest()
            : _mem_tracker(new MemTracker(-1)),
              _mem_pool(new MemPool(_mem_tracker.get())),
              _comparator(new TestComparator()),
              _list(_comparator.get(), _mem_pool.get(), false) {}

    // REQUIRES: External synchronization
    void write_step(Random* rnd) {
        const uint32_t k = rnd->Next() % K;
        const int g = _current.get(k) + 1;
        const Key new_key = make_key(k, g);
        bool overwritten = false;
        _list.Insert(new_key, &overwritten);
        _current.set(k, g);
    }

    void read_step(Random* rnd) {
        // Remember the initial committed state of the skiplist.
        State initial_state;
        for (int k = 0; k < K; k++) {
            initial_state.set(k, _current.get(k));
        }

        Key pos = random_target(rnd);
        SkipList<Key, TestComparator>::Iterator iter(&_list);
        iter.Seek(pos);
        while (true) {
            Key current;
            if (!iter.Valid()) {
                current = make_key(K, 0);
            } else {
                current = iter.key();
                EXPECT_TRUE(is_valid_key(current)) << current;
            }
            EXPECT_LE(pos, current) << "should not go backwards";

            // Verify that everything in [pos,current) was not present in
            // initial_state.
            while (pos < current) {
                EXPECT_LT(key(pos), K) << pos;

                // Note that generation 0 is never inserted, so it is ok if
                // <*,0,*> is missing.
                EXPECT_TRUE((gen(pos) == 0) ||
                            (gen(pos) > static_cast<Key>(initial_state.get(key(pos)))))
                        << "key: " << key(pos) << "; gen: " << gen(pos)
                        << "; initgen: " << initial_state.get(key(pos));

                // Advance to next key in the valid key space
                if (key(pos) < key(current)) {
                    pos = make_key(key(pos) + 1, 0);
                } else {
                    pos = make_key(key(pos), gen(pos) + 1);
                }
            }

            if (!iter.Valid()) {
                break;
            }

            if (rnd->Next() % 2) {
                iter.Next();
                pos = make_key(key(pos), gen(pos) + 1);
            } else {
                Key new_target = random_target(rnd);
                if (new_target > pos) {
                    pos = new_target;
                    iter.Seek(new_target);
                }
            }
        }
    }
};
const uint32_t ConcurrentTest::K;

// Simple test that does single-threaded testing of the ConcurrentTest
// scaffolding.
TEST_F(SkipTest, ConcurrentWithoutThreads) {
    ConcurrentTest test;
    Random rnd(random_seed);
    for (int i = 0; i < 10000; i++) {
        test.read_step(&rnd);
        test.write_step(&rnd);
    }
}

class TestState {
public:
    ConcurrentTest _t;
    int _seed;
    std::atomic<bool> _quit_flag;

    enum ReaderState { STARTING, RUNNING, DONE };

    explicit TestState(int s) : _seed(s), _quit_flag(false), _state(STARTING) {}

    void wait(ReaderState s) {
        std::unique_lock l(_mu);
        while (_state != s) {
            _cv_state.wait(l);
        }
    }

    void change(ReaderState s) {
        std::lock_guard l(_mu);
        _state = s;
        _cv_state.notify_one();
    }

private:
    std::mutex _mu;
    ReaderState _state;
    std::condition_variable _cv_state;
};

static void concurrent_reader(void* arg) {
    TestState* state = reinterpret_cast<TestState*>(arg);
    Random rnd(state->_seed);
    int64_t reads = 0;
    state->change(TestState::RUNNING);
    while (!state->_quit_flag.load(std::memory_order_acquire)) {
        state->_t.read_step(&rnd);
        ++reads;
    }
    state->change(TestState::DONE);
}

static void run_concurrent(int run) {
    const int seed = random_seed + (run * 100);
    Random rnd(seed);
    const int N = LOOP_LESS_OR_MORE(10, 1000);
    const int kSize = 1000;
    PriorityThreadPool thread_pool(10, 100);
    for (int i = 0; i < N; i++) {
        if ((i % 100) == 0) {
            fprintf(stderr, "Run %d of %d\n", i, N);
        }
        TestState state(seed + 1);
        thread_pool.offer(std::bind<void>(concurrent_reader, &state));
        state.wait(TestState::RUNNING);
        for (int i = 0; i < kSize; i++) {
            state._t.write_step(&rnd);
        }
        state._quit_flag.store(true, std::memory_order_release); // Any non-nullptr arg will do
        state.wait(TestState::DONE);
    }
}

TEST_F(SkipTest, Concurrent) {
    for (int i = 1; i < LOOP_LESS_OR_MORE(2, 6); ++i) {
        run_concurrent(i);
    }
}

} // namespace doris
