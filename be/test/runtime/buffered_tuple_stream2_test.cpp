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

#include <gtest/gtest.h>

#include <filesystem>
#include <functional>
#include <limits> // for std::numeric_limits<int>::max()
#include <string>

#include "gen_cpp/Types_types.h"
#include "runtime/buffered_tuple_stream2.inline.h"
#include "runtime/row_batch.h"
#include "runtime/string_value.hpp"
#include "runtime/test_env.h"
#include "runtime/tmp_file_mgr.h"
#include "runtime/types.h"
#include "testutil/desc_tbl_builder.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/logging.h"

using std::vector;

using std::unique_ptr;

static const int BATCH_SIZE = 250;
static const uint32_t PRIME = 479001599;

namespace doris {

static const StringValue STRINGS[] = {
        StringValue("ABC"),
        StringValue("HELLO"),
        StringValue("123456789"),
        StringValue("FOOBAR"),
        StringValue("ONE"),
        StringValue("THREE"),
        StringValue("abcdefghijklmno"),
        StringValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        StringValue("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
};

static const int NUM_STRINGS = sizeof(STRINGS) / sizeof(StringValue);

class SimpleTupleStreamTest : public testing::Test {
public:
    SimpleTupleStreamTest() : _tracker(new MemTracker(-1)) {}
    // A null dtor to pass codestyle check
    ~SimpleTupleStreamTest() {}

protected:
    virtual void SetUp() {
        _test_env.reset(new TestEnv());
        create_descriptors();
        _mem_pool.reset(new MemPool(_tracker.get()));
    }

    virtual void create_descriptors() {
        std::vector<bool> nullable_tuples(1, false);
        std::vector<TTupleId> tuple_ids(1, static_cast<TTupleId>(0));

        DescriptorTblBuilder int_builder(&_pool);
        int_builder.declare_tuple() << TYPE_INT;
        _int_desc = _pool.add(new RowDescriptor(*int_builder.build(), tuple_ids, nullable_tuples));

        DescriptorTblBuilder string_builder(&_pool);
        // string_builder.declare_tuple() << TYPE_STRING;
        string_builder.declare_tuple() << TYPE_VARCHAR;
        _string_desc =
                _pool.add(new RowDescriptor(*string_builder.build(), tuple_ids, nullable_tuples));
    }

    virtual void TearDown() {
        _runtime_state = nullptr;
        _client = nullptr;
        _pool.clear();
        _mem_pool->free_all();
        _test_env.reset();
    }

    // Setup a block manager with the provided settings and client with no reservation,
    // tracked by _tracker.
    void InitBlockMgr(int64_t limit, int block_size) {
        Status status = _test_env->create_query_state(0, limit, block_size, &_runtime_state);
        ASSERT_TRUE(status.ok());
        status = _runtime_state->block_mgr2()->register_client(0, _tracker, _runtime_state,
                                                               &_client);
        ASSERT_TRUE(status.ok());
    }

    // Generate the ith element of a sequence of int values.
    int GenIntValue(int i) {
        // Multiply by large prime to get varied bit patterns.
        return i * PRIME;
    }

    // Generate the ith element of a sequence of bool values.
    bool GenBoolValue(int i) {
        // Use a middle bit of the int value.
        return ((GenIntValue(i) >> 8) & 0x1) != 0;
    }

    virtual RowBatch* CreateIntBatch(int offset, int num_rows, bool gen_null) {
        RowBatch* batch = _pool.add(new RowBatch(*_int_desc, num_rows, _tracker.get()));
        int tuple_size = _int_desc->tuple_descriptors()[0]->byte_size();
        uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(
                batch->tuple_data_pool()->allocate(tuple_size * num_rows));
        memset(tuple_mem, 0, tuple_size * num_rows);

        const int int_tuples = _int_desc->tuple_descriptors().size();
        for (int i = 0; i < num_rows; ++i) {
            int idx = batch->add_row();
            TupleRow* row = batch->get_row(idx);
            Tuple* int_tuple = reinterpret_cast<Tuple*>(tuple_mem + i * tuple_size);
            // *reinterpret_cast<int*>(int_tuple + 1) = GenIntValue(i + offset);
            *reinterpret_cast<int*>(reinterpret_cast<uint8_t*>(int_tuple) + 1) =
                    GenIntValue(i + offset);
            for (int j = 0; j < int_tuples; ++j) {
                int idx = (i + offset) * int_tuples + j;
                if (!gen_null || GenBoolValue(idx)) {
                    row->set_tuple(j, int_tuple);
                } else {
                    row->set_tuple(j, nullptr);
                }
            }
            batch->commit_last_row();
        }
        return batch;
    }

    virtual RowBatch* CreateStringBatch(int offset, int num_rows, bool gen_null) {
        int tuple_size = sizeof(StringValue) + 1;
        RowBatch* batch = _pool.add(new RowBatch(*_string_desc, num_rows, _tracker.get()));
        uint8_t* tuple_mem = batch->tuple_data_pool()->allocate(tuple_size * num_rows);
        memset(tuple_mem, 0, tuple_size * num_rows);
        const int string_tuples = _string_desc->tuple_descriptors().size();
        for (int i = 0; i < num_rows; ++i) {
            TupleRow* row = batch->get_row(batch->add_row());
            *reinterpret_cast<StringValue*>(tuple_mem + 1) = STRINGS[(i + offset) % NUM_STRINGS];
            for (int j = 0; j < string_tuples; ++j) {
                int idx = (i + offset) * string_tuples + j;
                if (!gen_null || GenBoolValue(idx)) {
                    row->set_tuple(j, reinterpret_cast<Tuple*>(tuple_mem));
                } else {
                    row->set_tuple(j, nullptr);
                }
            }
            batch->commit_last_row();
            tuple_mem += tuple_size;
        }
        return batch;
    }

    void AppendRowTuples(TupleRow* row, std::vector<int>* results) {
        DCHECK(row != nullptr);
        const int int_tuples = _int_desc->tuple_descriptors().size();
        for (int i = 0; i < int_tuples; ++i) {
            AppendValue(row->get_tuple(i), results);
        }
    }

    void AppendRowTuples(TupleRow* row, std::vector<StringValue>* results) {
        DCHECK(row != nullptr);
        const int string_tuples = _string_desc->tuple_descriptors().size();
        for (int i = 0; i < string_tuples; ++i) {
            AppendValue(row->get_tuple(i), results);
        }
    }

    void AppendValue(Tuple* t, std::vector<int>* results) {
        if (t == nullptr) {
            // For the tests indicate null-ability using the max int value
            results->push_back(std::numeric_limits<int>::max());
        } else {
            results->push_back(*reinterpret_cast<int*>(reinterpret_cast<uint8_t*>(t) + 1));
        }
    }

    void AppendValue(Tuple* t, std::vector<StringValue>* results) {
        if (t == nullptr) {
            results->push_back(StringValue());
        } else {
            uint8_t* mem = reinterpret_cast<uint8_t*>(t);
            StringValue sv = *reinterpret_cast<StringValue*>(mem + 1);
            uint8_t* copy = _mem_pool->allocate(sv.len);
            memcpy(copy, sv.ptr, sv.len);
            sv.ptr = reinterpret_cast<char*>(copy);
            results->push_back(sv);
        }
    }

    template <typename T>
    void ReadValues(BufferedTupleStream2* stream, RowDescriptor* desc, std::vector<T>* results,
                    int num_batches = -1) {
        bool eos = false;
        RowBatch batch(*desc, BATCH_SIZE, _tracker.get());
        int batches_read = 0;
        do {
            batch.reset();
            Status status = stream->get_next(&batch, &eos);
            EXPECT_TRUE(status.ok());
            ++batches_read;
            for (int i = 0; i < batch.num_rows(); ++i) {
                AppendRowTuples(batch.get_row(i), results);
            }
        } while (!eos && (num_batches < 0 || batches_read <= num_batches));
    }

    virtual void VerifyResults(const std::vector<int>& results, int exp_rows, bool gen_null) {
        const int int_tuples = _int_desc->tuple_descriptors().size();
        EXPECT_EQ(results.size(), exp_rows * int_tuples);
        for (int i = 0; i < exp_rows; ++i) {
            for (int j = 0; j < int_tuples; ++j) {
                int idx = i * int_tuples + j;
                if (!gen_null || GenBoolValue(idx)) {
                    ASSERT_EQ(results[idx], GenIntValue(i))
                            << " results[" << idx << "]: " << results[idx]
                            << " != " << GenIntValue(i) << " gen_null=" << gen_null;
                } else {
                    ASSERT_TRUE(results[idx] == std::numeric_limits<int>::max())
                            << "i: " << i << " j: " << j << " results[" << idx
                            << "]: " << results[idx] << " != " << std::numeric_limits<int>::max();
                }
            }
        }
    }

    virtual void VerifyResults(const std::vector<StringValue>& results, int exp_rows,
                               bool gen_null) {
        const int string_tuples = _string_desc->tuple_descriptors().size();
        EXPECT_EQ(results.size(), exp_rows * string_tuples);
        for (int i = 0; i < exp_rows; ++i) {
            for (int j = 0; j < string_tuples; ++j) {
                int idx = i * string_tuples + j;
                if (!gen_null || GenBoolValue(idx)) {
                    ASSERT_TRUE(results[idx] == STRINGS[i % NUM_STRINGS])
                            << "results[" << idx << "] " << results[idx]
                            << " != " << STRINGS[i % NUM_STRINGS] << " i=" << i
                            << " gen_null=" << gen_null;
                } else {
                    ASSERT_TRUE(results[idx] == StringValue())
                            << "results[" << idx << "] " << results[idx] << " not nullptr";
                }
            }
        }
    }

    // Test adding num_batches of ints to the stream and reading them back.
    template <typename T>
    void TestValues(int num_batches, RowDescriptor* desc, bool gen_null) {
        BufferedTupleStream2 stream(_runtime_state, *desc, _runtime_state->block_mgr2(), _client,
                                    true, false);
        Status status = stream.init(-1, nullptr, true);
        ASSERT_TRUE(status.ok()) << status.get_error_msg();
        status = stream.unpin_stream();
        ASSERT_TRUE(status.ok());

        // Add rows to the stream
        int offset = 0;
        for (int i = 0; i < num_batches; ++i) {
            RowBatch* batch = nullptr;
            if (sizeof(T) == sizeof(int)) {
                batch = CreateIntBatch(offset, BATCH_SIZE, gen_null);
            } else if (sizeof(T) == sizeof(StringValue)) {
                batch = CreateStringBatch(offset, BATCH_SIZE, gen_null);
            } else {
                DCHECK(false);
            }
            for (int j = 0; j < batch->num_rows(); ++j) {
                bool b = stream.add_row(batch->get_row(j), &status);
                ASSERT_TRUE(status.ok());
                if (!b) {
                    ASSERT_TRUE(stream.using_small_buffers());
                    bool got_buffer;
                    status = stream.switch_to_io_buffers(&got_buffer);
                    ASSERT_TRUE(status.ok());
                    ASSERT_TRUE(got_buffer);
                    b = stream.add_row(batch->get_row(j), &status);
                    ASSERT_TRUE(status.ok());
                }
                ASSERT_TRUE(b);
            }
            offset += batch->num_rows();
            // Reset the batch to make sure the stream handles the memory correctly.
            batch->reset();
        }

        status = stream.prepare_for_read(false);
        ASSERT_TRUE(status.ok());

        // Read all the rows back
        std::vector<T> results;
        ReadValues(&stream, desc, &results);

        // Verify result
        VerifyResults(results, BATCH_SIZE * num_batches, gen_null);

        stream.close();
    }

    void TestIntValuesInterleaved(int num_batches, int num_batches_before_read) {
        for (int small_buffers = 0; small_buffers < 2; ++small_buffers) {
            BufferedTupleStream2 stream(_runtime_state, *_int_desc, _runtime_state->block_mgr2(),
                                        _client, small_buffers == 0, // initial small buffers
                                        true);                       // read_write
            Status status = stream.init(-1, nullptr, true);
            ASSERT_TRUE(status.ok());
            status = stream.prepare_for_read(true);
            ASSERT_TRUE(status.ok());
            status = stream.unpin_stream();
            ASSERT_TRUE(status.ok());

            std::vector<int> results;

            for (int i = 0; i < num_batches; ++i) {
                RowBatch* batch = CreateIntBatch(i * BATCH_SIZE, BATCH_SIZE, false);
                for (int j = 0; j < batch->num_rows(); ++j) {
                    bool b = stream.add_row(batch->get_row(j), &status);
                    ASSERT_TRUE(b);
                    ASSERT_TRUE(status.ok());
                }
                // Reset the batch to make sure the stream handles the memory correctly.
                batch->reset();
                if (i % num_batches_before_read == 0) {
                    ReadValues(&stream, _int_desc, &results,
                               (rand() % num_batches_before_read) + 1);
                }
            }
            ReadValues(&stream, _int_desc, &results);

            VerifyResults(results, BATCH_SIZE * num_batches, false);

            stream.close();
        }
    }

    std::unique_ptr<TestEnv> _test_env;
    RuntimeState* _runtime_state;
    BufferedBlockMgr2::Client* _client;

    std::shared_ptr<MemTracker> _tracker;
    ObjectPool _pool;
    RowDescriptor* _int_desc;
    RowDescriptor* _string_desc;
    std::unique_ptr<MemPool> _mem_pool;
};

// Tests with a non-NULLable tuple per row.
class SimpleNullStreamTest : public SimpleTupleStreamTest {
protected:
    virtual void create_descriptors() {
        std::vector<bool> nullable_tuples(1, true);
        std::vector<TTupleId> tuple_ids(1, static_cast<TTupleId>(0));

        DescriptorTblBuilder int_builder(&_pool);
        int_builder.declare_tuple() << TYPE_INT;
        _int_desc = _pool.add(new RowDescriptor(*int_builder.build(), tuple_ids, nullable_tuples));

        DescriptorTblBuilder string_builder(&_pool);
        string_builder.declare_tuple() << TYPE_VARCHAR;
        _string_desc =
                _pool.add(new RowDescriptor(*string_builder.build(), tuple_ids, nullable_tuples));
    }
}; // SimpleNullStreamTest

// Tests with multiple non-NULLable tuples per row.
class MultiTupleStreamTest : public SimpleTupleStreamTest {
protected:
    virtual void create_descriptors() {
        std::vector<bool> nullable_tuples;
        nullable_tuples.push_back(false);
        nullable_tuples.push_back(false);
        nullable_tuples.push_back(false);

        std::vector<TTupleId> tuple_ids;
        tuple_ids.push_back(static_cast<TTupleId>(0));
        tuple_ids.push_back(static_cast<TTupleId>(1));
        tuple_ids.push_back(static_cast<TTupleId>(2));

        DescriptorTblBuilder int_builder(&_pool);
        int_builder.declare_tuple() << TYPE_INT;
        int_builder.declare_tuple() << TYPE_INT;
        int_builder.declare_tuple() << TYPE_INT;
        _int_desc = _pool.add(new RowDescriptor(*int_builder.build(), tuple_ids, nullable_tuples));

        DescriptorTblBuilder string_builder(&_pool);
        string_builder.declare_tuple() << TYPE_VARCHAR;
        string_builder.declare_tuple() << TYPE_VARCHAR;
        string_builder.declare_tuple() << TYPE_VARCHAR;
        _string_desc =
                _pool.add(new RowDescriptor(*string_builder.build(), tuple_ids, nullable_tuples));
    }
};

// Tests with multiple NULLable tuples per row.
class MultiNullableTupleStreamTest : public SimpleTupleStreamTest {
protected:
    virtual void create_descriptors() {
        std::vector<bool> nullable_tuples;
        nullable_tuples.push_back(false);
        nullable_tuples.push_back(true);
        nullable_tuples.push_back(true);

        std::vector<TTupleId> tuple_ids;
        tuple_ids.push_back(static_cast<TTupleId>(0));
        tuple_ids.push_back(static_cast<TTupleId>(1));
        tuple_ids.push_back(static_cast<TTupleId>(2));

        DescriptorTblBuilder int_builder(&_pool);
        int_builder.declare_tuple() << TYPE_INT;
        int_builder.declare_tuple() << TYPE_INT;
        int_builder.declare_tuple() << TYPE_INT;
        _int_desc = _pool.add(new RowDescriptor(*int_builder.build(), tuple_ids, nullable_tuples));

        DescriptorTblBuilder string_builder(&_pool);
        string_builder.declare_tuple() << TYPE_VARCHAR;
        string_builder.declare_tuple() << TYPE_VARCHAR;
        string_builder.declare_tuple() << TYPE_VARCHAR;
        _string_desc =
                _pool.add(new RowDescriptor(*string_builder.build(), tuple_ids, nullable_tuples));
    }
};

#if 0
// Tests with collection types.
class ArrayTupleStreamTest : public SimpleTupleStreamTest {
protected:
    RowDescriptor* _array_desc;

    virtual void create_descriptors() {
        // tuples: (array<string>, array<array<int>>) (array<int>)
        std::vector<bool> nullable_tuples(2, true);
        std::vector<TTupleId> tuple_ids;
        tuple_ids.push_back(static_cast<TTupleId>(0));
        tuple_ids.push_back(static_cast<TTupleId>(1));
        TypeDescriptor string_array_type;
        string_array_type.type = TYPE_ARRAY;
        string_array_type.children.push_back(TYPE_VARCHAR);

        TypeDescriptor int_array_type;
        int_array_type.type = TYPE_ARRAY;
        int_array_type.children.push_back(TYPE_VARCHAR);

        TypeDescriptor nested_array_type;
        nested_array_type.type = TYPE_ARRAY;
        nested_array_type.children.push_back(int_array_type);

        DescriptorTblBuilder builder(&_pool);
        builder.declare_tuple() << string_array_type << nested_array_type;
        builder.declare_tuple() << int_array_type;
        _array_desc = _pool.add(new RowDescriptor(
                    *builder.build(), tuple_ids, nullable_tuples));
    }
};
#endif

// Basic API test. No data should be going to disk.
TEST_F(SimpleTupleStreamTest, Basic) {
    InitBlockMgr(-1, 8 * 1024 * 1024);
    TestValues<int>(1, _int_desc, false);
    TestValues<int>(10, _int_desc, false);
    TestValues<int>(100, _int_desc, false);

    TestValues<StringValue>(1, _string_desc, false);
    TestValues<StringValue>(10, _string_desc, false);
    TestValues<StringValue>(100, _string_desc, false);

    TestIntValuesInterleaved(1, 1);
    TestIntValuesInterleaved(10, 5);
    TestIntValuesInterleaved(100, 15);
}

// #if 0
// Test with only 1 buffer.
TEST_F(SimpleTupleStreamTest, OneBufferSpill) {
    // Each buffer can only hold 100 ints, so this spills quite often.
    int buffer_size = 100 * sizeof(int);
    InitBlockMgr(buffer_size, buffer_size);
    TestValues<int>(1, _int_desc, false);
    TestValues<int>(10, _int_desc, false);

    TestValues<StringValue>(1, _string_desc, false);
    TestValues<StringValue>(10, _string_desc, false);
}

// Test with a few buffers.
TEST_F(SimpleTupleStreamTest, ManyBufferSpill) {
    int buffer_size = 100 * sizeof(int);
    InitBlockMgr(10 * buffer_size, buffer_size);

    TestValues<int>(1, _int_desc, false);
    TestValues<int>(10, _int_desc, false);
    TestValues<int>(100, _int_desc, false);
    TestValues<StringValue>(1, _string_desc, false);
    TestValues<StringValue>(10, _string_desc, false);
    TestValues<StringValue>(100, _string_desc, false);

    TestIntValuesInterleaved(1, 1);
    TestIntValuesInterleaved(10, 5);
    TestIntValuesInterleaved(100, 15);
}

TEST_F(SimpleTupleStreamTest, UnpinPin) {
    int buffer_size = 100 * sizeof(int);
    InitBlockMgr(3 * buffer_size, buffer_size);

    BufferedTupleStream2 stream(_runtime_state, *_int_desc, _runtime_state->block_mgr2(), _client,
                                true, false);
    Status status = stream.init(-1, nullptr, true);
    ASSERT_TRUE(status.ok());

    int offset = 0;
    bool full = false;
    while (!full) {
        RowBatch* batch = CreateIntBatch(offset, BATCH_SIZE, false);
        int j = 0;
        for (; j < batch->num_rows(); ++j) {
            full = !stream.add_row(batch->get_row(j), &status);
            ASSERT_TRUE(status.ok());
            if (full) {
                break;
            }
        }
        offset += j;
    }

    status = stream.unpin_stream();
    ASSERT_TRUE(status.ok());

    bool pinned = false;
    status = stream.pin_stream(false, &pinned);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(pinned);

    std::vector<int> results;

    // Read and verify result a few times. We should be able to reread the stream if
    // we don't use delete on read mode.
    int read_iters = 3;
    for (int i = 0; i < read_iters; ++i) {
        bool delete_on_read = i == read_iters - 1;
        status = stream.prepare_for_read(delete_on_read);
        ASSERT_TRUE(status.ok());
        results.clear();
        ReadValues(&stream, _int_desc, &results);
        VerifyResults(results, offset, false);
    }

    // After delete_on_read, all blocks aside from the last should be deleted.
    // Note: this should really be 0, but the BufferedTupleStream2 returns eos before
    // deleting the last block, rather than after, so the last block isn't deleted
    // until the stream is closed.
    DCHECK_EQ(stream.bytes_in_mem(false), buffer_size);

    stream.close();

    DCHECK_EQ(stream.bytes_in_mem(false), 0);
}

TEST_F(SimpleTupleStreamTest, SmallBuffers) {
    int buffer_size = 8 * 1024 * 1024;
    InitBlockMgr(2 * buffer_size, buffer_size);

    BufferedTupleStream2 stream(_runtime_state, *_int_desc, _runtime_state->block_mgr2(), _client,
                                true, false);
    Status status = stream.init(-1, nullptr, false);
    ASSERT_TRUE(status.ok());

    // Initial buffer should be small.
    EXPECT_LT(stream.bytes_in_mem(false), buffer_size);

    RowBatch* batch = CreateIntBatch(0, 1024, false);
    for (int i = 0; i < batch->num_rows(); ++i) {
        bool ret = stream.add_row(batch->get_row(i), &status);
        EXPECT_TRUE(ret);
        ASSERT_TRUE(status.ok());
    }
    EXPECT_LT(stream.bytes_in_mem(false), buffer_size);
    EXPECT_LT(stream.byte_size(), buffer_size);
    ASSERT_TRUE(stream.using_small_buffers());

    // 40 MB of ints
    batch = CreateIntBatch(0, 10 * 1024 * 1024, false);
    for (int i = 0; i < batch->num_rows(); ++i) {
        bool ret = stream.add_row(batch->get_row(i), &status);
        ASSERT_TRUE(status.ok());
        if (!ret) {
            ASSERT_TRUE(stream.using_small_buffers());
            bool got_buffer;
            status = stream.switch_to_io_buffers(&got_buffer);
            ASSERT_TRUE(status.ok());
            ASSERT_TRUE(got_buffer);
            ret = stream.add_row(batch->get_row(i), &status);
            ASSERT_TRUE(status.ok());
        }
        ASSERT_TRUE(ret);
    }
    EXPECT_EQ(stream.bytes_in_mem(false), buffer_size);

    // TODO: Test for IMPALA-2330. In case switch_to_io_buffers() fails to get buffer then
    // using_small_buffers() should still return true.
    stream.close();
}

// Basic API test. No data should be going to disk.
TEST_F(SimpleNullStreamTest, Basic) {
    InitBlockMgr(-1, 8 * 1024 * 1024);
    TestValues<int>(1, _int_desc, false);
    TestValues<int>(10, _int_desc, false);
    TestValues<int>(100, _int_desc, false);
    TestValues<int>(1, _int_desc, true);
    TestValues<int>(10, _int_desc, true);
    TestValues<int>(100, _int_desc, true);

    TestValues<StringValue>(1, _string_desc, false);
    TestValues<StringValue>(10, _string_desc, false);
    TestValues<StringValue>(100, _string_desc, false);
    TestValues<StringValue>(1, _string_desc, true);
    TestValues<StringValue>(10, _string_desc, true);
    TestValues<StringValue>(100, _string_desc, true);

    TestIntValuesInterleaved(1, 1);
    TestIntValuesInterleaved(10, 5);
    TestIntValuesInterleaved(100, 15);
}

// Test tuple stream with only 1 buffer and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleOneBufferSpill) {
    // Each buffer can only hold 100 ints, so this spills quite often.
    int buffer_size = 100 * sizeof(int);
    InitBlockMgr(buffer_size, buffer_size);
    TestValues<int>(1, _int_desc, false);
    TestValues<int>(10, _int_desc, false);

    TestValues<StringValue>(1, _string_desc, false);
    TestValues<StringValue>(10, _string_desc, false);
}

// Test with a few buffers and rows with multiple tuples.
TEST_F(MultiTupleStreamTest, MultiTupleManyBufferSpill) {
    int buffer_size = 100 * sizeof(int);
    InitBlockMgr(10 * buffer_size, buffer_size);

    TestValues<int>(1, _int_desc, false);
    TestValues<int>(10, _int_desc, false);
    TestValues<int>(100, _int_desc, false);

    TestValues<StringValue>(1, _string_desc, false);
    TestValues<StringValue>(10, _string_desc, false);
    TestValues<StringValue>(100, _string_desc, false);

    TestIntValuesInterleaved(1, 1);
    TestIntValuesInterleaved(10, 5);
    TestIntValuesInterleaved(100, 15);
}

// Test with rows with multiple nullable tuples.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleOneBufferSpill) {
    // Each buffer can only hold 100 ints, so this spills quite often.
    int buffer_size = 100 * sizeof(int);
    InitBlockMgr(buffer_size, buffer_size);
    TestValues<int>(1, _int_desc, false);
    TestValues<int>(10, _int_desc, false);
    TestValues<int>(1, _int_desc, true);
    TestValues<int>(10, _int_desc, true);

    TestValues<StringValue>(1, _string_desc, false);
    TestValues<StringValue>(10, _string_desc, false);
    TestValues<StringValue>(1, _string_desc, true);
    TestValues<StringValue>(10, _string_desc, true);
}

// Test with a few buffers.
TEST_F(MultiNullableTupleStreamTest, MultiNullableTupleManyBufferSpill) {
    int buffer_size = 100 * sizeof(int);
    InitBlockMgr(10 * buffer_size, buffer_size);

    TestValues<int>(1, _int_desc, false);
    TestValues<int>(10, _int_desc, false);
    TestValues<int>(100, _int_desc, false);
    TestValues<int>(1, _int_desc, true);
    TestValues<int>(10, _int_desc, true);
    TestValues<int>(100, _int_desc, true);

    TestValues<StringValue>(1, _string_desc, false);
    TestValues<StringValue>(10, _string_desc, false);
    TestValues<StringValue>(100, _string_desc, false);
    TestValues<StringValue>(1, _string_desc, true);
    TestValues<StringValue>(10, _string_desc, true);
    TestValues<StringValue>(100, _string_desc, true);

    TestIntValuesInterleaved(1, 1);
    TestIntValuesInterleaved(10, 5);
    TestIntValuesInterleaved(100, 15);
}
// #endif

#if 0
// Test that deep copy works with arrays by copying into a BufferedTupleStream2, freeing
// the original rows, then reading back the rows and verifying the contents.
TEST_F(ArrayTupleStreamTest, TestArrayDeepCopy) {
    Status status;
    InitBlockMgr(-1, 8 * 1024 * 1024);
    const int NUM_ROWS = 4000;
    BufferedTupleStream2 stream(_runtime_state, *_array_desc, _runtime_state->block_mgr2(),
            _client, false, false);
    const std::vector<TupleDescriptor*>& tuple_descs = _array_desc->tuple_descriptors();
    // Write out a predictable pattern of data by iterating over arrays of constants.
    int strings_index = 0; // we take the mod of this as index into STRINGS.
    int array_lens[] = { 0, 1, 5, 10, 1000, 2, 49, 20 };
    int num_array_lens = sizeof(array_lens) / sizeof(array_lens[0]);
    int array_len_index = 0;
    for (int i = 0; i < NUM_ROWS; ++i) {
        int expected_row_size = tuple_descs[0]->byte_size() + tuple_descs[1]->byte_size();
        // gscoped_ptr<TupleRow, FreeDeleter> row(reinterpret_cast<TupleRow*>(
        //             malloc(tuple_descs.size() * sizeof(Tuple*))));
        // gscoped_ptr<Tuple, FreeDeleter> tuple0(reinterpret_cast<Tuple*>(
        //             malloc(tuple_descs[0]->byte_size())));
        // gscoped_ptr<Tuple, FreeDeleter> tuple1(reinterpret_cast<Tuple*>(
        //             malloc(tuple_descs[1]->byte_size())));
        std::unique_ptr<TupleRow> row(reinterpret_cast<TupleRow*>(
                    malloc(tuple_descs.size() * sizeof(Tuple*))));
        std::unique_ptr<Tuple> tuple0(reinterpret_cast<Tuple*>(
                    malloc(tuple_descs[0]->byte_size())));
        std::unique_ptr<Tuple> tuple1(reinterpret_cast<Tuple*>(
                    malloc(tuple_descs[1]->byte_size())));
        memset(tuple0.get(), 0, tuple_descs[0]->byte_size());
        memset(tuple1.get(), 0, tuple_descs[1]->byte_size());
        row->set_tuple(0, tuple0.get());
        row->set_tuple(1, tuple1.get());

        // Only array<string> is non-null.
        tuple0->set_null(tuple_descs[0]->slots()[1]->null_indicator_offset());
        tuple1->set_null(tuple_descs[1]->slots()[0]->null_indicator_offset());
        const SlotDescriptor* array_slot_desc = tuple_descs[0]->slots()[0];
        const TupleDescriptor* item_desc = array_slot_desc->collection_item_descriptor();

        int array_len = array_lens[array_len_index++ % num_array_lens];
        CollectionValue* cv = tuple0->GetCollectionSlot(array_slot_desc->tuple_offset());
        cv->ptr = nullptr;
        cv->num_tuples = 0;
        CollectionValueBuilder builder(cv, *item_desc, _mem_pool.get(), array_len);
        Tuple* array_data;
        builder.GetFreeMemory(&array_data);
        expected_row_size += item_desc->byte_size() * array_len;

        // Fill the array with pointers to our constant strings.
        for (int j = 0; j < array_len; ++j) {
            const StringValue* string = &STRINGS[strings_index++ % NUM_STRINGS];
            array_data->SetNotNull(item_desc->slots()[0]->null_indicator_offset());
            RawValue::Write(string, array_data, item_desc->slots()[0], _mem_pool.get());
            array_data += item_desc->byte_size();
            expected_row_size += string->len;
        }
        builder.CommitTuples(array_len);

        // Check that internal row size computation gives correct result.
        EXPECT_EQ(expected_row_size, stream.ComputeRowSize(row.get()));
        bool b = stream.add_row(row.get(), &status);
        ASSERT_TRUE(b);
        ASSERT_TRUE(status.ok());
        _mem_pool->FreeAll(); // Free data as soon as possible to smoke out issues.
    }

    // Read back and verify data.
    stream.prepare_for_read(false);
    strings_index = 0;
    array_len_index = 0;
    bool eos = false;
    int rows_read = 0;
    RowBatch batch(*_array_desc, BATCH_SIZE, _tracker.get());
    do {
        batch.reset();
        ASSERT_TRUE(stream.get_next(&batch, &eos).ok());
        for (int i = 0; i < batch.num_rows(); ++i) {
            TupleRow* row = batch.GetRow(i);
            Tuple* tuple0 = row->get_tuple(0);
            Tuple* tuple1 = row->get_tuple(1);
            ASSERT_TRUE(tuple0 != nullptr);
            ASSERT_TRUE(tuple1 != nullptr);
            const SlotDescriptor* array_slot_desc = tuple_descs[0]->slots()[0];
            ASSERT_FALSE(tuple0->IsNull(array_slot_desc->null_indicator_offset()));
            ASSERT_TRUE(tuple0->IsNull(tuple_descs[0]->slots()[1]->null_indicator_offset()));
            ASSERT_TRUE(tuple1->IsNull(tuple_descs[1]->slots()[0]->null_indicator_offset()));

            const TupleDescriptor* item_desc = array_slot_desc->collection_item_descriptor();
            int expected_array_len = array_lens[array_len_index++ % num_array_lens];
            CollectionValue* cv = tuple0->GetCollectionSlot(array_slot_desc->tuple_offset());
            ASSERT_EQ(expected_array_len, cv->num_tuples);
            for (int j = 0; j < cv->num_tuples; ++j) {
                Tuple* item = reinterpret_cast<Tuple*>(cv->ptr + j * item_desc->byte_size());
                const SlotDescriptor* string_desc = item_desc->slots()[0];
                ASSERT_FALSE(item->IsNull(string_desc->null_indicator_offset()));
                const StringValue* expected = &STRINGS[strings_index++ % NUM_STRINGS];
                const StringValue* actual = item->GetStringSlot(string_desc->tuple_offset());
                ASSERT_EQ(*expected, *actual);
            }
        }
        rows_read += batch.num_rows();
    } while (!eos);
    ASSERT_EQ(NUM_ROWS, rows_read);
}
#endif

// TODO: more tests.
//  - The stream can operate in many modes

} // namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::config::query_scratch_dirs = "/tmp";
    // doris::config::max_free_io_buffers = 128;
    doris::config::read_size = 8388608;
    doris::config::min_buffer_size = 1024;

    doris::config::disable_mem_pools = false;

    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    doris::CpuInfo::init();
    doris::DiskInfo::init();

    return RUN_ALL_TESTS();
}
