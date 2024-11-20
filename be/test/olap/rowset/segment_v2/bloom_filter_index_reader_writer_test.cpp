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

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/decimal12.h"
#include "olap/itoken_extractor.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/types.h"
#include "olap/uint24.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

const std::string dname = "./ut_dir/bloom_filter_index_reader_writer_test";

class BloomFilterIndexReaderWriterTest : public testing::Test {
public:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(dname);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(dname);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(dname).ok());
    }
};

template <FieldType type>
Status write_bloom_filter_index_file(const std::string& file_name, const void* values,
                                     size_t value_count, size_t null_count,
                                     ColumnIndexMetaPB* index_meta,
                                     bool use_primary_key_bloom_filter = false) {
    const auto* type_info = get_scalar_type_info<type>();
    using CppType = typename CppTypeTraits<type>::CppType;
    std::string fname = dname + "/" + file_name;
    auto fs = io::global_local_filesystem();
    {
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(fs->create_file(fname, &file_writer));

        std::unique_ptr<BloomFilterIndexWriter> bloom_filter_index_writer;
        BloomFilterOptions bf_options;

        if (use_primary_key_bloom_filter) {
            RETURN_IF_ERROR(PrimaryKeyBloomFilterIndexWriterImpl::create(
                    bf_options, type_info, &bloom_filter_index_writer));
        } else {
            RETURN_IF_ERROR(BloomFilterIndexWriter::create(bf_options, type_info,
                                                           &bloom_filter_index_writer));
        }

        const CppType* vals = (const CppType*)values;
        for (int i = 0; i < value_count;) {
            size_t num = std::min(1024, (int)value_count - i);
            RETURN_IF_ERROR(bloom_filter_index_writer->add_values(vals + i, num));
            if (i == 2048) {
                // second page
                bloom_filter_index_writer->add_nulls(null_count);
            }
            RETURN_IF_ERROR(bloom_filter_index_writer->flush());
            i += 1024;
        }
        RETURN_IF_ERROR(bloom_filter_index_writer->finish(file_writer.get(), index_meta));
        EXPECT_TRUE(file_writer->close().ok());
        EXPECT_EQ(BLOOM_FILTER_INDEX, index_meta->type());
        EXPECT_EQ(bf_options.strategy, index_meta->bloom_filter_index().hash_strategy());
    }
    return Status::OK();
}

void get_bloom_filter_reader_iter(const std::string& file_name, const ColumnIndexMetaPB& meta,
                                  BloomFilterIndexReader** reader,
                                  std::unique_ptr<BloomFilterIndexIterator>* iter) {
    std::string fname = dname + "/" + file_name;
    io::FileReaderSPtr file_reader;
    ASSERT_EQ(io::global_local_filesystem()->open_file(fname, &file_reader), Status::OK());
    *reader = new BloomFilterIndexReader(std::move(file_reader), meta.bloom_filter_index());
    auto st = (*reader)->load(true, false);
    EXPECT_TRUE(st.ok());

    st = (*reader)->new_iterator(iter);
    EXPECT_TRUE(st.ok());
}

template <FieldType Type>
Status test_bloom_filter_index_reader_writer_template(
        const std::string file_name, typename TypeTraits<Type>::CppType* val, size_t num,
        size_t null_num, typename TypeTraits<Type>::CppType* not_exist_value,
        bool is_slice_type = false, bool use_primary_key_bloom_filter = false) {
    using CppType = typename TypeTraits<Type>::CppType;
    ColumnIndexMetaPB meta;
    RETURN_IF_ERROR(write_bloom_filter_index_file<Type>(file_name, val, num, null_num, &meta,
                                                        use_primary_key_bloom_filter));
    {
        BloomFilterIndexReader* reader = nullptr;
        std::unique_ptr<BloomFilterIndexIterator> iter;
        get_bloom_filter_reader_iter(file_name, meta, &reader, &iter);

        // page 0
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(iter->read_bloom_filter(0, &bf));
        for (int i = 0; i < 1024; ++i) {
            if (is_slice_type) {
                Slice* value = (Slice*)(val + i);
                EXPECT_TRUE(bf->test_bytes(value->data, value->size));
            } else {
                EXPECT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
            }
        }

        // page 1
        RETURN_IF_ERROR(iter->read_bloom_filter(1, &bf));
        for (int i = 1024; i < 2048; ++i) {
            if (is_slice_type) {
                Slice* value = (Slice*)(val + i);
                EXPECT_TRUE(bf->test_bytes(value->data, value->size));
            } else {
                EXPECT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
            }
        }

        // page 2
        RETURN_IF_ERROR(iter->read_bloom_filter(2, &bf));
        for (int i = 2048; i < 3071; ++i) {
            if (is_slice_type) {
                Slice* value = (Slice*)(val + i);
                EXPECT_TRUE(bf->test_bytes(value->data, value->size));
            } else {
                EXPECT_TRUE(bf->test_bytes((char*)&val[i], sizeof(CppType)));
            }
        }
        // test nullptr
        EXPECT_TRUE(bf->test_bytes(nullptr, 1));

        delete reader;
    }
    return Status::OK();
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
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_INT>(
            file_name, val, num, 1, &not_exist_value);
    EXPECT_TRUE(st.ok());
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
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_BIGINT>(
            file_name, val, num, 1, &not_exist_value);
    EXPECT_TRUE(st.ok());
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_largeint) {
    size_t num = 1024 * 3 - 1;
    int128_t* val = new int128_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 100000000 + i + 1;
    }

    std::string file_name = "bloom_filter_largeint";
    int128_t not_exist_value = 18888;
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_LARGEINT>(
            file_name, val, num, 1, &not_exist_value);
    EXPECT_TRUE(st.ok());
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
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
            file_name, slices, num, 1, &not_exist_value, true);
    EXPECT_TRUE(st.ok());
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
    Slice not_exist_value("char_value_not_exist");
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_CHAR>(
            file_name, slices, num, 1, &not_exist_value, true);
    EXPECT_TRUE(st.ok());
    delete[] val;
    delete[] slices;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_date) {
    size_t num = 1024 * 3 - 1;
    uint24_t* val = new uint24_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 10000 + i + 1;
    }

    std::string file_name = "bloom_filter_date";
    uint24_t not_exist_value = 18888;
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DATE>(
            file_name, val, num, 1, &not_exist_value);
    EXPECT_TRUE(st.ok());
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_datetime) {
    size_t num = 1024 * 3 - 1;
    int64_t* val = new int64_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 10000 + i + 1;
    }

    std::string file_name = "bloom_filter_datetime";
    int64_t not_exist_value = 18888;
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DATETIME>(
            file_name, val, num, 1, &not_exist_value);
    EXPECT_TRUE(st.ok());
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_decimal) {
    size_t num = 1024 * 3 - 1;
    decimal12_t* val = new decimal12_t[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = {i + 1, i + 1};
    }

    std::string file_name = "bloom_filter_decimal";
    decimal12_t not_exist_value = {666, 666};
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DECIMAL>(
            file_name, val, num, 1, &not_exist_value);
    EXPECT_TRUE(st.ok());
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_primary_key_bloom_filter_index) {
    size_t num = 1024 * 3 - 1;
    std::vector<std::string> val_strings(num);
    for (size_t i = 0; i < num; ++i) {
        val_strings[i] = "primary_key_" + std::to_string(i);
    }
    std::vector<Slice> slices(num);
    for (size_t i = 0; i < num; ++i) {
        slices[i] = Slice(val_strings[i]);
    }

    std::string file_name = "primary_key_bloom_filter_index";
    Slice not_exist_value("primary_key_not_exist");

    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
            file_name, slices.data(), num, 0, &not_exist_value, true, true);
    EXPECT_TRUE(st.ok());
}

TEST_F(BloomFilterIndexReaderWriterTest, test_primary_key_bloom_filter_index_int) {
    size_t num = 1024 * 3 - 1;
    int* val = new int[num];
    for (int i = 0; i < num; ++i) {
        // there will be 3 bloom filter pages
        val[i] = 10000 + i + 1;
    }

    std::string file_name = "primary_key_bloom_filter_index_int";
    int not_exist_value = 18888;
    auto st = test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_INT>(
            file_name, val, num, 1, &not_exist_value, false, true);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), TStatusCode::NOT_IMPLEMENTED_ERROR);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_datev2) {
    size_t num = 1024 * 3 - 1;
    uint32_t* val = new uint32_t[num];
    for (size_t i = 0; i < num; ++i) {
        val[i] = 20210101 + i; // YYYYMMDD
    }

    std::string file_name = "bloom_filter_datev2";
    uint32_t not_exist_value = 20211231;
    test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DATEV2>(
            file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_datetimev2) {
    size_t num = 1024 * 3 - 1;
    uint64_t* val = new uint64_t[num];
    for (size_t i = 0; i < num; ++i) {
        val[i] = 20210101000000 + i; // YYYYMMDDHHMMSS
    }

    std::string file_name = "bloom_filter_datetimev2";
    uint64_t not_exist_value = 20211231235959;
    test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>(
            file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_decimal32) {
    size_t num = 1024 * 3 - 1;
    int32_t* val = new int32_t[num];
    for (size_t i = 0; i < num; ++i) {
        val[i] = static_cast<int32_t>(i * 100 + 1);
    }

    std::string file_name = "bloom_filter_decimal32";
    int32_t not_exist_value = 99999;
    test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DECIMAL32>(
            file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_decimal64) {
    size_t num = 1024 * 3 - 1;
    ;
    int64_t* val = new int64_t[num];
    for (size_t i = 0; i < num; ++i) {
        val[i] = static_cast<int64_t>(i * 1000 + 123);
    }

    std::string file_name = "bloom_filter_decimal64";
    int64_t not_exist_value = 9999999;
    test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DECIMAL64>(
            file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_ipv4) {
    size_t num = 1024 * 3 - 1;
    uint32_t* val = new uint32_t[num];
    for (size_t i = 0; i < num; ++i) {
        val[i] = (192 << 24) | (168 << 16) | (i >> 8) | (i & 0xFF);
    }

    std::string file_name = "bloom_filter_ipv4";
    uint32_t not_exist_value = (10 << 24) | (0 << 16) | (0 << 8) | 1;
    test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_IPV4>(
            file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_decimal128i) {
    size_t num = 1024 * 3 - 1;
    int128_t* val = new int128_t[num];

    int128_t base_value = int128_t(1000000000ULL) * int128_t(1000000000ULL);

    for (size_t i = 0; i < num; ++i) {
        val[i] = base_value + int128_t(i);
    }

    std::string file_name = "bloom_filter_decimal128i";
    int128_t not_exist_value = int128_t(9999999999999999999ULL);

    test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>(
            file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_decimal256) {
    size_t num = 1024 * 3 - 1;
    using Decimal256Type = wide::Int256;

    Decimal256Type* val = new Decimal256Type[num];

    Decimal256Type base_value = Decimal256Type(1000000000ULL); // 1e9
    base_value *= Decimal256Type(1000000000ULL);               // base_value = 1e18
    base_value *= Decimal256Type(100000000ULL);                // base_value = 1e26
    base_value *= Decimal256Type(100000000ULL);                // base_value = 1e34
    base_value *= Decimal256Type(10000ULL);                    // base_value = 1e38

    for (size_t i = 0; i < num; ++i) {
        val[i] = base_value + Decimal256Type(i);
    }

    std::string file_name = "bloom_filter_decimal256";

    Decimal256Type not_exist_value = base_value + Decimal256Type(9999999ULL);

    test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_DECIMAL256>(
            file_name, val, num, 1, &not_exist_value);

    delete[] val;
}

TEST_F(BloomFilterIndexReaderWriterTest, test_ipv6) {
    size_t num = 1024 * 3 - 1;
    uint128_t* val = new uint128_t[num];
    for (size_t i = 0; i < num; ++i) {
        val[i] = (uint128_t(0x20010DB800000000) << 64) | uint128_t(i);
    }

    std::string file_name = "bloom_filter_ipv6";
    uint128_t not_exist_value = (uint128_t(0x20010DB800000000) << 64) | uint128_t(999999);

    test_bloom_filter_index_reader_writer_template<FieldType::OLAP_FIELD_TYPE_IPV6>(
            file_name, val, num, 1, &not_exist_value);
    delete[] val;
}

void test_ngram_bloom_filter_index_reader_writer(const std::string& file_name, Slice* values,
                                                 size_t num_values, uint8_t gram_size,
                                                 uint16_t bf_size) {
    ColumnIndexMetaPB meta;
    {
        auto type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
        auto fs = io::global_local_filesystem();
        std::string fname = dname + "/" + file_name;
        io::FileWriterPtr file_writer;
        Status st = fs->create_file(fname, &file_writer);
        EXPECT_TRUE(st.ok()) << st.to_string();

        BloomFilterOptions bf_options;
        std::unique_ptr<BloomFilterIndexWriter> bf_index_writer;
        st = NGramBloomFilterIndexWriterImpl::create(bf_options, type_info, gram_size, bf_size,
                                                     &bf_index_writer);
        EXPECT_TRUE(st.ok());

        size_t i = 0;
        {
            size_t num = std::min(static_cast<size_t>(1024), num_values - i);
            st = bf_index_writer->add_values(values + i, num);
            EXPECT_TRUE(st.ok());
            st = bf_index_writer->flush();
            EXPECT_TRUE(st.ok());
            i += num;
        }
        if (i < num_values) {
            size_t num = num_values - i;
            st = bf_index_writer->add_values(values + i, num);
            EXPECT_TRUE(st.ok());
            st = bf_index_writer->flush();
            EXPECT_TRUE(st.ok());
            i += num;
        }

        st = bf_index_writer->finish(file_writer.get(), &meta);
        EXPECT_TRUE(st.ok()) << "writer finish status:" << st.to_string();
        EXPECT_TRUE(file_writer->close().ok());
    }
    {
        BloomFilterIndexReader* reader = nullptr;
        std::unique_ptr<BloomFilterIndexIterator> iter;
        get_bloom_filter_reader_iter(file_name, meta, &reader, &iter);

        std::vector<std::string> test_patterns = {"ngram15", "ngram1000", "ngram1499",
                                                  "non-existent-string"};

        NgramTokenExtractor extractor(gram_size);
        uint16_t gram_bf_size = bf_size;

        {
            std::unique_ptr<BloomFilter> bf;
            auto st = iter->read_bloom_filter(0, &bf);
            EXPECT_TRUE(st.ok());

            for (const auto& pattern : test_patterns) {
                std::unique_ptr<BloomFilter> query_bf;
                st = BloomFilter::create(NGRAM_BLOOM_FILTER, &query_bf, gram_bf_size);
                EXPECT_TRUE(st.ok());

                if (extractor.string_like_to_bloom_filter(pattern.data(), pattern.size(),
                                                          *query_bf)) {
                    bool contains = bf->contains(*query_bf);
                    bool expected = false;
                    if (pattern == "ngram15" || pattern == "ngram1000") {
                        expected = true;
                    }
                    EXPECT_EQ(contains, expected) << "Pattern: " << pattern;
                }
            }
        }
        {
            if (num_values > 1024) {
                std::unique_ptr<BloomFilter> bf;
                auto st = iter->read_bloom_filter(1, &bf);
                EXPECT_TRUE(st.ok());

                for (const auto& pattern : test_patterns) {
                    std::unique_ptr<BloomFilter> query_bf;
                    st = BloomFilter::create(NGRAM_BLOOM_FILTER, &query_bf, gram_bf_size);
                    EXPECT_TRUE(st.ok());

                    if (extractor.string_like_to_bloom_filter(pattern.data(), pattern.size(),
                                                              *query_bf)) {
                        bool contains = bf->contains(*query_bf);
                        bool expected = false;
                        if (pattern == "ngram1499") {
                            expected = true;
                        }
                        EXPECT_EQ(contains, expected) << "Pattern: " << pattern;
                    }
                }
            }
        }
        delete reader;
    }
}

TEST_F(BloomFilterIndexReaderWriterTest, test_ngram_bloom_filter) {
    size_t num = 1500;
    std::vector<std::string> val(num);
    for (size_t i = 0; i < num; ++i) {
        val[i] = "ngram" + std::to_string(i);
    }
    std::vector<Slice> slices(num);
    for (size_t i = 0; i < num; ++i) {
        slices[i] = Slice(val[i].data(), val[i].size());
    }

    std::string file_name = "bloom_filter_ngram";
    uint8_t gram_size = 5;
    uint16_t bf_size = 65535;

    test_ngram_bloom_filter_index_reader_writer(file_name, slices.data(), num, gram_size, bf_size);
}

TEST_F(BloomFilterIndexReaderWriterTest, test_unsupported_type) {
    auto type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_FLOAT>();
    BloomFilterOptions bf_options;
    std::unique_ptr<BloomFilterIndexWriter> bloom_filter_index_writer;
    auto st = BloomFilterIndexWriter::create(bf_options, type_info, &bloom_filter_index_writer);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), TStatusCode::NOT_IMPLEMENTED_ERROR);
}

} // namespace segment_v2
} // namespace doris
