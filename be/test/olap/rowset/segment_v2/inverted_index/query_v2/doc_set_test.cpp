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

TEST_F(DocSetTest, FreqNotImplemented) {
    DocSet ds;
    try {
        (void)ds.freq();
        FAIL() << "Expected doris::Exception for NOT_IMPLEMENTED_ERROR";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    } catch (...) {
        FAIL() << "Expected doris::Exception";
    }
}

TEST_F(DocSetTest, NormNotImplemented) {
    DocSet ds;
    try {
        (void)ds.norm();
        FAIL() << "Expected doris::Exception for NOT_IMPLEMENTED_ERROR";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    } catch (...) {
        FAIL() << "Expected doris::Exception";
    }
}

// MockDocSet tests
TEST_F(DocSetTest, MockDocSetEmptyDocs) {
    MockDocSet ds({});
    EXPECT_EQ(ds.doc(), TERMINATED);
    EXPECT_EQ(ds.advance(), TERMINATED);
    EXPECT_EQ(ds.seek(10), TERMINATED);
    EXPECT_EQ(ds.size_hint(), 0);
    EXPECT_EQ(ds.norm(), 1);
}

TEST_F(DocSetTest, MockDocSetSingleDoc) {
    MockDocSet ds({5});
    EXPECT_EQ(ds.doc(), 5);
    EXPECT_EQ(ds.size_hint(), 1);
    EXPECT_EQ(ds.norm(), 1);
    EXPECT_EQ(ds.advance(), TERMINATED);
    EXPECT_EQ(ds.doc(), TERMINATED);
}

TEST_F(DocSetTest, MockDocSetAdvance) {
    MockDocSet ds({1, 5, 10, 15, 20});
    EXPECT_EQ(ds.doc(), 1);
    EXPECT_EQ(ds.advance(), 5);
    EXPECT_EQ(ds.doc(), 5);
    EXPECT_EQ(ds.advance(), 10);
    EXPECT_EQ(ds.advance(), 15);
    EXPECT_EQ(ds.advance(), 20);
    EXPECT_EQ(ds.advance(), TERMINATED);
    EXPECT_EQ(ds.doc(), TERMINATED);
}

TEST_F(DocSetTest, MockDocSetSeekExactMatch) {
    MockDocSet ds({1, 5, 10, 15, 20});
    EXPECT_EQ(ds.doc(), 1);
    EXPECT_EQ(ds.seek(10), 10);
    EXPECT_EQ(ds.doc(), 10);
    EXPECT_EQ(ds.seek(20), 20);
    EXPECT_EQ(ds.doc(), 20);
}

TEST_F(DocSetTest, MockDocSetSeekNextHigher) {
    MockDocSet ds({1, 5, 10, 15, 20});
    EXPECT_EQ(ds.doc(), 1);
    EXPECT_EQ(ds.seek(7), 10);
    EXPECT_EQ(ds.doc(), 10);
    EXPECT_EQ(ds.seek(12), 15);
    EXPECT_EQ(ds.doc(), 15);
}

TEST_F(DocSetTest, MockDocSetSeekBeyondLast) {
    MockDocSet ds({1, 5, 10, 15, 20});
    EXPECT_EQ(ds.seek(25), TERMINATED);
    EXPECT_EQ(ds.doc(), TERMINATED);
}

TEST_F(DocSetTest, MockDocSetSeekCurrentDoc) {
    MockDocSet ds({1, 5, 10, 15, 20});
    EXPECT_EQ(ds.doc(), 1);
    EXPECT_EQ(ds.seek(1), 1);
    EXPECT_EQ(ds.doc(), 1);
    EXPECT_EQ(ds.advance(), 5);
    EXPECT_EQ(ds.seek(5), 5);
    EXPECT_EQ(ds.doc(), 5);
}

TEST_F(DocSetTest, MockDocSetSeekBeforeCurrent) {
    MockDocSet ds({1, 5, 10, 15, 20});
    EXPECT_EQ(ds.advance(), 5);
    EXPECT_EQ(ds.doc(), 5);
    // Seeking to a value less than current should return current
    EXPECT_EQ(ds.seek(3), 5);
    EXPECT_EQ(ds.doc(), 5);
}

TEST_F(DocSetTest, MockDocSetUnsortedInput) {
    // MockDocSet should sort the input
    MockDocSet ds({20, 5, 15, 1, 10});
    EXPECT_EQ(ds.doc(), 1);
    EXPECT_EQ(ds.advance(), 5);
    EXPECT_EQ(ds.advance(), 10);
    EXPECT_EQ(ds.advance(), 15);
    EXPECT_EQ(ds.advance(), 20);
    EXPECT_EQ(ds.advance(), TERMINATED);
}

TEST_F(DocSetTest, MockDocSetCustomSizeHint) {
    MockDocSet ds({1, 2, 3}, 100);
    EXPECT_EQ(ds.size_hint(), 100);
}

TEST_F(DocSetTest, MockDocSetDefaultSizeHint) {
    MockDocSet ds({1, 2, 3, 4, 5});
    EXPECT_EQ(ds.size_hint(), 5);
}

TEST_F(DocSetTest, MockDocSetCustomNorm) {
    MockDocSet ds({1, 2, 3}, 0, 42);
    EXPECT_EQ(ds.norm(), 42);
}

TEST_F(DocSetTest, MockDocSetDefaultNorm) {
    MockDocSet ds({1, 2, 3});
    EXPECT_EQ(ds.norm(), 1);
}

TEST_F(DocSetTest, MockDocSetAdvanceAfterSeek) {
    MockDocSet ds({1, 5, 10, 15, 20, 25, 30});
    EXPECT_EQ(ds.seek(10), 10);
    EXPECT_EQ(ds.advance(), 15);
    EXPECT_EQ(ds.advance(), 20);
    EXPECT_EQ(ds.seek(25), 25);
    EXPECT_EQ(ds.advance(), 30);
    EXPECT_EQ(ds.advance(), TERMINATED);
}

TEST_F(DocSetTest, MockDocSetSeekAfterTerminated) {
    MockDocSet ds({1, 2, 3});
    EXPECT_EQ(ds.advance(), 2);
    EXPECT_EQ(ds.advance(), 3);
    EXPECT_EQ(ds.advance(), TERMINATED);
    EXPECT_EQ(ds.seek(100), TERMINATED);
}

TEST_F(DocSetTest, MockDocSetAdvanceAfterTerminated) {
    MockDocSet ds({1, 2});
    EXPECT_EQ(ds.advance(), 2);
    EXPECT_EQ(ds.advance(), TERMINATED);
    EXPECT_EQ(ds.advance(), TERMINATED);
}

} // namespace doris
