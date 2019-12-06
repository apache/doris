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

#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/key_coder.h"

#include <gtest/gtest.h>

#include "common/logging.h"
#include "env/env.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "olap/column_block.h"
#include "util/file_utils.h"
#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"

namespace doris {
namespace segment_v2 {

class BloomFilterIndexReaderWriterTest : public testing::Test {
   public:
    BloomFilterIndexReaderWriterTest() { }
    virtual ~BloomFilterIndexReaderWriterTest() {
    }
};

const std::string dname = "./ut_dir/bloom_filter_index_reader_writer_test";

template<FieldType type>
void write_bloom_filter_index_file(const std::string& file_name, const void* values,
                      size_t value_count, size_t null_count,
                      BloomFilterIndexPB* bloom_filter_index_meta) {
    const TypeInfo* type_info = get_type_info(type);
    FileUtils::create_dir(dname);
    std::string fname = dname + "/" + file_name;
    {
        std::unique_ptr<WritableFile> wfile;
        auto st = Env::Default()->new_writable_file(fname, &wfile);
        ASSERT_TRUE(st.ok());
        std::unique_ptr<BloomFilterIndexWriter> bloom_filter_index_writer;
        BloomFilterIndexWriter::create(type_info, wfile.get(), &bloom_filter_index_writer);
        bloom_filter_index_writer->add_values(values, value_count);
        bloom_filter_index_writer->add_nulls(null_count);
        st = bloom_filter_index_writer->finish(bloom_filter_index_meta);
        ASSERT_TRUE(st.ok()) << "writer finish status:" << st.to_string();
        wfile.reset();
    }
}

void get_bloom_filter_reader_iter(const std::string& file_name, const BloomFilterIndexPB& bloom_filter_index_meta,
                            std::unique_ptr<RandomAccessFile>* rfile,
                            BloomFilterIndexReader** reader,
                            std::unique_ptr<BloomFilterIndexIterator>* iter) {
    std::string fname = dname + "/" + file_name;
    auto st = Env::Default()->new_random_access_file(fname, rfile);
    ASSERT_TRUE(st.ok());

    *reader = new BloomFilterIndexReader(rfile->get(), bloom_filter_index_meta);
    st = (*reader)->load();
    ASSERT_TRUE(st.ok());

    st = (*reader)->new_iterator(iter);
    ASSERT_TRUE(st.ok());
}

template <FieldType Type>
void test_bloom_filter_index_reader_writer_template(const std::string file_name,
        typename TypeTraits<Type>::CppType* val, size_t num, size_t null_num,
        typename TypeTraits<Type>::CppType* not_exist_value,
        bool is_slice_type = false) {
     typedef typename TypeTraits<Type>::CppType CppType;
    BloomFilterIndexPB bloom_filter_index_meta;
    write_bloom_filter_index_file<Type>(file_name, val, num, null_num,
                                          &bloom_filter_index_meta);
    {
        std::unique_ptr<RandomAccessFile> rfile;
        BloomFilterIndexReader* reader = nullptr;
        std::unique_ptr<BloomFilterIndexIterator> iter;
        get_bloom_filter_reader_iter(file_name, bloom_filter_index_meta,
                                                    &rfile, &reader, &iter);

        // rowid 1023 will be value 11024, in page 0
        std::unique_ptr<BloomFilter> bf;
        auto st = iter->read_bloom_filter(1023, &bf);
        ASSERT_TRUE(st.ok());
        for (int i = 0; i < 1024; ++i) {
            if (is_slice_type) {
                Slice* value = (Slice*)(val + i);
                ASSERT_TRUE(bf->test_bytes(value->data, value->size));
            } else {
                ASSERT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
            }
        }
        ASSERT_EQ(0, iter->current_bloom_filter_index());

        // rowid 1024 will be value 11025, in page 1
        st = iter->read_bloom_filter(1024, &bf);
        ASSERT_TRUE(st.ok());
        for (int i = 1024; i < 2048; ++i) {
            if (is_slice_type) {
                Slice* value = (Slice*)(val + i);
                ASSERT_TRUE(bf->test_bytes(value->data, value->size));
            } else {
                ASSERT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
            }
        }
        ASSERT_EQ(1, iter->current_bloom_filter_index());

        // rowid 2047 will be value 12048, in page 1
        st = iter->read_bloom_filter(2047, &bf);
        ASSERT_TRUE(st.ok());
        if (is_slice_type) {
            Slice* value = (Slice*)(val + 2047);
            ASSERT_TRUE(bf->test_bytes(value->data, value->size));
        } else {
            ASSERT_TRUE(bf->test_bytes((char*)&val[2047], sizeof(CppType)));
        }
        ASSERT_EQ(1, iter->current_bloom_filter_index());

        // rowid 2048 will be value 12049, in page 2
        st = iter->read_bloom_filter(2048, &bf);
        ASSERT_TRUE(st.ok());
        for (int i = 2048; i < 3071; ++i) {
            if (is_slice_type) {
                Slice* value = (Slice*)(val + i);
                ASSERT_TRUE(bf->test_bytes(value->data, value->size));
            } else {
                ASSERT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
            }
        }
        // test nullptr
        ASSERT_TRUE(bf->test_bytes(nullptr, 1));
        ASSERT_EQ(2, iter->current_bloom_filter_index());

        // rowid 3070 will be in page 2
        st = iter->read_bloom_filter(3070, &bf);
        ASSERT_TRUE(st.ok());
        if (is_slice_type) {
            Slice* value = (Slice*)(val + 3070);
            ASSERT_TRUE(bf->test_bytes(value->data, value->size));
        } else {
            ASSERT_TRUE(bf->test_bytes((char*)&val[3070], sizeof(CppType)));
        }
        ASSERT_EQ(2, iter->current_bloom_filter_index());

        if (is_slice_type) {
            Slice* value = (Slice*)not_exist_value;
            ASSERT_FALSE(bf->test_bytes(value->data, value->size));
        } else {
            ASSERT_FALSE(bf->test_bytes((char*)not_exist_value, sizeof(CppType)));
        }

        // rowid range: [10, 1024), will return only one page, and 1024th value is not in bf
        std::map<RowRange, std::unique_ptr<BloomFilter>, RowRange::Comparator> bfs;
        st = iter->read_range_bloom_filters(10, 1024, &bfs);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(1, bfs.size());
        for (auto& bf : bfs) {
            ASSERT_EQ(10, bf.first.from());
            ASSERT_EQ(1024, bf.first.to());
            for (int i = 0; i < 1024; ++i) {
                if (is_slice_type) {
                    Slice* value = (Slice*)(val + i);
                    ASSERT_TRUE(bf.second->test_bytes(value->data, value->size));
                } else {
                    ASSERT_TRUE(bf.second->test_bytes((char*)&val[i], sizeof(CppType)));
                }
            }
        }
        bfs.clear();

        // rowid range: [10, 1025), will return the first two page, and 1024th value is in the second bf
        std::map<RowRange, std::unique_ptr<BloomFilter>, RowRange::Comparator> bfs2;
        st = iter->read_range_bloom_filters(10, 1025, &bfs2);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(2, bfs2.size());
        int index = 0;
        for (auto& bf : bfs2) {
            if (index == 0) {
                ASSERT_EQ(10, bf.first.from());
                ASSERT_EQ(1024, bf.first.to());
                for (int i = 0; i < 1024; ++i) {
                    if (is_slice_type) {
                        Slice* value = (Slice*)(val + i);
                        ASSERT_TRUE(bf.second->test_bytes(value->data, value->size));
                    } else {
                        ASSERT_TRUE(bf.second->test_bytes((char*)&val[i], sizeof(CppType)));
                    }
                }
            } else if (index == 1) {
                ASSERT_EQ(1024, bf.first.from());
                ASSERT_EQ(2048, bf.first.to());
                for (int i = 1024; i < 2048; ++i) {
                    if (is_slice_type) {
                        Slice* value = (Slice*)(val + i);
                        ASSERT_TRUE(bf.second->test_bytes(value->data, value->size));
                    } else {
                        ASSERT_TRUE(bf.second->test_bytes((char*)&val[i], sizeof(CppType)));
                    }
                }
            }
            ++index;
        }
        bfs2.clear();

        // rowid range: [10, 2049), will return all three page
        std::map<RowRange, std::unique_ptr<BloomFilter>, RowRange::Comparator> bfs3;
        st = iter->read_range_bloom_filters(10, 2049, &bfs3);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(3, bfs3.size());
        index = 0;
        for (auto& bf : bfs3) {
            if (index == 0) {
                ASSERT_EQ(10, bf.first.from());
                ASSERT_EQ(1024, bf.first.to());
                for (int i = 0; i < 1024; ++i) {
                    if (is_slice_type) {
                        Slice* value = (Slice*)(val + i);
                        ASSERT_TRUE(bf.second->test_bytes(value->data, value->size));
                    } else {
                        ASSERT_TRUE(bf.second->test_bytes((char*)&val[i], sizeof(CppType)));
                    }
                }
            } else if (index == 1) {
                ASSERT_EQ(1024, bf.first.from());
                ASSERT_EQ(2048, bf.first.to());
                for (int i = 1024; i < 2048; ++i) {
                    if (is_slice_type) {
                        Slice* value = (Slice*)(val + i);
                        ASSERT_TRUE(bf.second->test_bytes(value->data, value->size));
                    } else {
                        ASSERT_TRUE(bf.second->test_bytes((char*)&val[i], sizeof(CppType)));
                    }
                }
            } else if (index == 2) {
                ASSERT_EQ(2048, bf.first.from());
                ASSERT_EQ(3072, bf.first.to());
                for (int i = 2048; i < 3071; ++i) {
                    if (is_slice_type) {
                        Slice* value = (Slice*)(val + i);
                        ASSERT_TRUE(bf.second->test_bytes(value->data, value->size));
                    } else {
                        ASSERT_TRUE(bf.second->test_bytes((char*)&val[i], sizeof(CppType)));
                    }
                }
                // test nullptr
                ASSERT_TRUE(bf.second->test_bytes(nullptr, 1));
            }
            ++index;
        }
        bfs3.clear();

        delete reader;
    }
}

TEST_F(BloomFilterIndexReaderWriterTest, test_int) {
    size_t num = 1024 * 3 - 1;
    int* val = new int[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 10000 + i + 1;
    }

    std::string file_name = "bloom_filter_int";
    int not_exist_value = 18888;
    test_bloom_filter_index_reader_writer_template<OLAP_FIELD_TYPE_INT>(file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_bigint) {
    size_t num = 1024 * 3 - 1;
    int64_t* val = new int64_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 100000000 + i + 1;
    }

    std::string file_name = "bloom_filter_bigint";
    int64_t not_exist_value = 18888;
    test_bloom_filter_index_reader_writer_template<OLAP_FIELD_TYPE_BIGINT>(file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_varchar_type) {
    size_t num = 1024 * 3 - 1;
    std::string* val = new std::string[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = "prefix_" + std::to_string(i);
    }
    Slice* slices = new Slice[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        slices[i] = Slice(val[i].c_str(), val[i].size());
    }
    std::string file_name = "bloom_filter_varchar";
    Slice not_exist_value("value_not_exist");
    test_bloom_filter_index_reader_writer_template<OLAP_FIELD_TYPE_VARCHAR>(file_name, slices, num, 1, &not_exist_value, true);
    delete[] val;
    delete[] slices;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_char) {
    size_t num = 1024 * 3 - 1;
    std::string* val = new std::string[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = "prefix_" + std::to_string(10000 + i);
    }
    Slice* slices = new Slice[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        slices[i] = Slice(val[i].c_str(), val[i].size());
    }
    std::string file_name = "bloom_filter_char";
    Slice not_exist_value("value_not_exist");
    test_bloom_filter_index_reader_writer_template<OLAP_FIELD_TYPE_CHAR>(file_name, slices, num, 1, &not_exist_value, true);
    delete[] val;
    delete[] slices;
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}