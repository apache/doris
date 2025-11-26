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

#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/rowset/segment_v2/inverted_index/util/reader.h"

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {

class MockDorisCharFilter : public DorisCharFilter {
public:
    MockDorisCharFilter(ReaderPtr reader) : DorisCharFilter(std::move(reader)) {}
    ~MockDorisCharFilter() override = default;

    void initialize() override {}

    // 实现必需的虚函数
    void init(const void* _value, int32_t _length, bool copyData) override {
        if (_reader) {
            _reader->init(_value, _length, copyData);
        }
    }

    int32_t read(const void** start, int32_t min, int32_t max) override {
        if (_reader) {
            return _reader->read(start, min, max);
        }
        return -1;
    }

    int32_t readCopy(void* start, int32_t off, int32_t len) override {
        if (_reader) {
            return _reader->readCopy(start, off, len);
        }
        return -1;
    }
};

class DorisCharFilterTest : public ::testing::Test {
protected:
    void SetUp() override { _mock_reader = std::make_shared<lucene::util::SStringReader<char>>(); }

    ReaderPtr _mock_reader;
};

TEST_F(DorisCharFilterTest, ExceptionThrowing) {
    auto filter = std::make_shared<MockDorisCharFilter>(_mock_reader);

    EXPECT_THROW(filter->position(), doris::Exception);

    EXPECT_THROW(filter->skip(10), doris::Exception);

    EXPECT_THROW(filter->size(), doris::Exception);
}

} // namespace doris::segment_v2::inverted_index