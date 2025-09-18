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

#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"

#include <gtest/gtest.h>

#include "common/exception.h"

namespace doris {

using namespace segment_v2::inverted_index::query_v2;

class DocSetTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(DocSetTest, TerminatorConstant) {
    EXPECT_EQ(TERMINATED, static_cast<uint32_t>(INT_MAX));
}

TEST_F(DocSetTest, AdvanceNotImplemented) {
    DocSet ds;
    try {
        (void)ds.advance();
        FAIL() << "Expected doris::Exception for NOT_IMPLEMENTED_ERROR";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    } catch (...) {
        FAIL() << "Expected doris::Exception";
    }
}

TEST_F(DocSetTest, SeekNotImplemented) {
    DocSet ds;
    try {
        (void)ds.seek(10);
        FAIL() << "Expected doris::Exception for NOT_IMPLEMENTED_ERROR";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    } catch (...) {
        FAIL() << "Expected doris::Exception";
    }
}

TEST_F(DocSetTest, DocNotImplemented) {
    DocSet ds;
    try {
        (void)ds.doc();
        FAIL() << "Expected doris::Exception for NOT_IMPLEMENTED_ERROR";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    } catch (...) {
        FAIL() << "Expected doris::Exception";
    }
}

TEST_F(DocSetTest, SizeHintNotImplemented) {
    DocSet ds;
    try {
        (void)ds.size_hint();
        FAIL() << "Expected doris::Exception for NOT_IMPLEMENTED_ERROR";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    } catch (...) {
        FAIL() << "Expected doris::Exception";
    }
}

} // namespace doris
