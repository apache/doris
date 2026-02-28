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

#include "vec/functions/function_ip.h"

#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/rowset/segment_v2/index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/columns/column_const.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(FunctionIpTest, FunctionIsIPAddressInRangeTest) {
    std::string func_name = "is_ip_address_in_range";

    DataSet data_set = {
            {{std::string("127.0.0.1"), std::string("127.0.0.0/8")}, (uint8_t)1},
            {{std::string("128.0.0.1"), std::string("127.0.0.0/8")}, (uint8_t)0},
            {{std::string("ffff::1"), std::string("ffff::/16")}, (uint8_t)1},
            {{std::string("fffe::1"), std::string("ffff::/16")}, (uint8_t)0},
            {{std::string("192.168.99.255"), std::string("192.168.100.0/22")}, (uint8_t)0},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/22")}, (uint8_t)1},
            {{std::string("192.168.103.255"), std::string("192.168.100.0/22")}, (uint8_t)1},
            {{std::string("192.168.104.0"), std::string("192.168.100.0/22")}, (uint8_t)0},
            {{std::string("::192.168.99.255"), std::string("::192.168.100.0/118")}, (uint8_t)0},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/118")}, (uint8_t)1},
            {{std::string("::192.168.103.255"), std::string("::192.168.100.0/118")}, (uint8_t)1},
            {{std::string("::192.168.104.0"), std::string("::192.168.100.0/118")}, (uint8_t)0},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/22")}, (uint8_t)1},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/24")}, (uint8_t)1},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/32")}, (uint8_t)0},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/118")}, (uint8_t)1},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/120")}, (uint8_t)1},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/128")}, (uint8_t)0},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/22")}, (uint8_t)1},
            {{std::string("192.168.103.255"), std::string("192.168.100.0/24")}, (uint8_t)0},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/118")}, (uint8_t)1},
            {{std::string("::192.168.103.255"), std::string("::192.168.100.0/120")}, (uint8_t)0},
            {{std::string("127.0.0.1"), std::string("ffff::/16")}, (uint8_t)0},
            {{std::string("127.0.0.1"), std::string("::127.0.0.1/128")}, (uint8_t)0},
            {{std::string("::1"), std::string("127.0.0.0/8")}, (uint8_t)0},
            {{std::string("::127.0.0.1"), std::string("127.0.0.1/32")}, (uint8_t)0}};

    {
        // vector vs vector
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    {
        // vector vs scalar
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        for (const auto& line : data_set) {
            DataSet const_cidr_dataset = {line};
            static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types,
                                                                  const_cidr_dataset));
        }
    }

    {
        // scalar vs vector
        InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR},
                                    PrimitiveType::TYPE_VARCHAR};
        for (const auto& line : data_set) {
            DataSet const_addr_dataset = {line};
            static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types,
                                                                  const_addr_dataset));
        }
    }
}

TEST(FunctionIpTest, FunctionIPv4ToIPv6Test) {
    std::string func_name = "ipv4_to_ipv6";

    DataSet data_set = {
            {{static_cast<IPv4>(0)}, static_cast<IPv6>(0xFFFF00000000ULL)},          // 0.0.0.0
            {{static_cast<IPv4>(1)}, static_cast<IPv6>(0xFFFF00000001ULL)},          // 0.0.0.1
            {{static_cast<IPv4>(2130706433)}, static_cast<IPv6>(0xFFFF7F000001ULL)}, // 127.0.0.1
            {{static_cast<IPv4>(3232235521)}, static_cast<IPv6>(0xFFFFC0A80001ULL)}, // 192.168.0.1
            {{static_cast<IPv4>(4294967294)},
             static_cast<IPv6>(0xFFFFFFFFFFFEULL)}, // 255.255.255.254
            {{static_cast<IPv4>(4294967295)},
             static_cast<IPv6>(0xFFFFFFFFFFFFULL)} // 255.255.255.255
    };

    InputTypeSet input_types = {PrimitiveType::TYPE_IPV4};
    static_cast<void>(check_function<DataTypeIPv6, true>(func_name, input_types, data_set));
}

TEST(FunctionIpTest, FunctionCutIPv6Test) {
    std::string func_name = "cut_ipv6";

    std::array<std::array<uint8_t, 16>, 9> ipv6s = {
            std::array<uint8_t, 16> {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},   // ::
            std::array<uint8_t, 16> {0x1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // ::1
            std::array<uint8_t, 16> {0x02, 0, 0x02, 0xb1, 0, 0, 0, 0, 0x10, 0x06, 0xa1, 0, 0x70,
                                     0x1b, 0x01, 0x20}, // 2001:1b70:a1:610::b102:2
            std::array<uint8_t, 16> {0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                                     0xff, 0xff, 0xff, 0xff, 0xff,
                                     0xff}, // ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe
            std::array<uint8_t, 16> {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                                     0xff, 0xff, 0xff, 0xff, 0xff,
                                     0xff}, // ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff
            std::array<uint8_t, 16> {0x01, 0, 0xa8, 0xc0, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0}, // ::ffff:192.168.0.1
            std::array<uint8_t, 16> {0x01, 0, 0, 0x7f, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0}, // ::ffff:127.0.0.1
            std::array<uint8_t, 16> {0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0}, // ::ffff:255.255.255.254
            std::array<uint8_t, 16> {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                     0} // ::ffff:255.255.255.255
    };

    std::vector<int8_t> bytes = {0, 2, 4, 8, 16, 2, 4, 8, 16};

    std::vector<std::string> results = {"::",
                                        "::",
                                        "2001:1b70:a1:610::",
                                        "ffff:ffff:ffff:ffff::",
                                        "::",
                                        "::ffff:192.168.0.0",
                                        "::ffff:0.0.0.0",
                                        "::",
                                        "::"};

    DataSet data_set;

    for (int i = 0; i < 5; ++i) {
        IPv6 ipv6;
        std::memcpy(&ipv6, &ipv6s[i], sizeof(IPv6));
        // *reinterpret_cast<uint128_t*> will result in core dump, using std::memcpy instead.
        data_set.push_back({{ipv6, bytes[i], (int8_t)0}, results[i]});
    }

    for (int i = 5; i < results.size(); ++i) {
        IPv6 ipv6;
        std::memcpy(&ipv6, &ipv6s[i], sizeof(IPv6));
        // *reinterpret_cast<uint128_t*> will result in core dump, using std::memcpy instead.
        data_set.push_back({{ipv6, (int8_t)0, bytes[i]}, results[i]});
    }

    InputTypeSet input_types = {PrimitiveType::TYPE_IPV6, PrimitiveType::TYPE_TINYINT,
                                PrimitiveType::TYPE_TINYINT};
    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

class MockIndexReader : public segment_v2::InvertedIndexReader {
public:
    MockIndexReader(const TabletIndex& index_meta)
            : segment_v2::InvertedIndexReader(&index_meta, nullptr) {}
    ~MockIndexReader() override = default;
    segment_v2::InvertedIndexReaderType type() override {
        return segment_v2::InvertedIndexReaderType::BKD;
    }
    Status query(const segment_v2::IndexQueryContextPtr& context, const std::string& column_name,
                 const void* query_value, segment_v2::InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        return Status::OK();
    }
    Status try_query(const segment_v2::IndexQueryContextPtr& context,
                     const std::string& column_name, const void* query_value,
                     segment_v2::InvertedIndexQueryType query_type, size_t* count) override {
        return Status::OK();
    }
    Status new_iterator(std::unique_ptr<segment_v2::IndexIterator>* iterator) override {
        return Status::OK();
    }
};

class MockIndexIterator : public segment_v2::IndexIterator {
public:
    MockIndexIterator(std::shared_ptr<MockIndexReader> reader) : _reader(reader) {}
    ~MockIndexIterator() override = default;
    segment_v2::IndexReaderPtr get_reader(segment_v2::IndexReaderType reader_type) const override {
        if (std::holds_alternative<segment_v2::InvertedIndexReaderType>(reader_type)) {
            if (std::get<segment_v2::InvertedIndexReaderType>(reader_type) ==
                segment_v2::InvertedIndexReaderType::BKD) {
                return _reader;
            }
        }
        return nullptr;
    }
    Status read_from_index(const segment_v2::IndexParam& param) override {
        auto* p = std::get<segment_v2::InvertedIndexParam*>(param);
        if (p->query_type == segment_v2::InvertedIndexQueryType::GREATER_EQUAL_QUERY) {
            p->roaring->addRange(10, 20);
        } else if (p->query_type == segment_v2::InvertedIndexQueryType::LESS_EQUAL_QUERY) {
            p->roaring->addRange(15, 25);
        }
        return Status::OK();
    }
    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* cache_handle) override {
        return Status::OK();
    }
    Result<bool> has_null() override { return false; }

private:
    std::shared_ptr<MockIndexReader> _reader;
};

TEST(FunctionIpTest, evaluate_inverted_index) {
    FunctionIsIPAddressInRange func;

    // IPv4 test
    {
        auto cidr_col = ColumnString::create();
        cidr_col->insert_data("127.0.0.0/8", 11);
        auto const_cidr_col = ColumnConst::create(std::move(cidr_col), 1);

        ColumnsWithTypeAndName arguments = {
                {std::move(const_cidr_col), std::make_shared<DataTypeString>(), "cidr"}};

        std::vector<IndexFieldNameAndTypePair> data_type_with_names = {
                {"ip_addr", std::make_shared<DataTypeIPv4>()}};

        TabletIndex index_meta;
        auto reader = std::make_shared<MockIndexReader>(index_meta);
        auto iter = std::make_unique<MockIndexIterator>(reader);
        std::vector<segment_v2::IndexIterator*> iterators = {iter.get()};

        segment_v2::InvertedIndexResultBitmap bitmap_result;
        auto status = func.evaluate_inverted_index(arguments, data_type_with_names, iterators, 100,
                                                   nullptr, bitmap_result);
        ASSERT_TRUE(status.ok());

        // min_param: [10, 20), max_param: [15, 25)
        // intersection: [15, 20) -> 15, 16, 17, 18, 19
        ASSERT_EQ(bitmap_result.get_data_bitmap()->cardinality(), 5);
        for (int i = 15; i < 20; ++i) {
            ASSERT_TRUE(bitmap_result.get_data_bitmap()->contains(i));
        }
    }

    // IPv6 test
    {
        auto cidr_col = ColumnString::create();
        cidr_col->insert_data("ffff::/16", 9);
        auto const_cidr_col = ColumnConst::create(std::move(cidr_col), 1);

        ColumnsWithTypeAndName arguments = {
                {std::move(const_cidr_col), std::make_shared<DataTypeString>(), "cidr"}};

        std::vector<IndexFieldNameAndTypePair> data_type_with_names = {
                {"ip_addr", std::make_shared<DataTypeIPv6>()}};

        TabletIndex index_meta;
        auto reader = std::make_shared<MockIndexReader>(index_meta);
        auto iter = std::make_unique<MockIndexIterator>(reader);
        std::vector<segment_v2::IndexIterator*> iterators = {iter.get()};

        segment_v2::InvertedIndexResultBitmap bitmap_result;
        auto status = func.evaluate_inverted_index(arguments, data_type_with_names, iterators, 100,
                                                   nullptr, bitmap_result);
        ASSERT_TRUE(status.ok());

        ASSERT_EQ(bitmap_result.get_data_bitmap()->cardinality(), 5);
        for (int i = 15; i < 20; ++i) {
            ASSERT_TRUE(bitmap_result.get_data_bitmap()->contains(i));
        }
    }
}

} // namespace doris::vectorized
