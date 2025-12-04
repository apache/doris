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

#include "olap/rowset/segment_v2/inverted_index/query_v2/composite_reader.h"

#include <gtest/gtest.h>

#include <string>

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris::segment_v2 {

using namespace inverted_index;

TEST(CompositeReaderTest, SetAndGetNullptr) {
    query_v2::CompositeReader cr;

    std::wstring field = StringHelper::to_wstring("f1");
    lucene::index::IndexReader* reader = nullptr;

    cr.set_reader(field, reader);
    auto* got = cr.get_reader(field);
    EXPECT_EQ(got, reader);
}

TEST(CompositeReaderTest, GetNonExistingThrowsNotFound) {
    query_v2::CompositeReader cr;

    std::wstring field = StringHelper::to_wstring("no_such_field");

    try {
        (void)cr.get_reader(field);
        FAIL() << "Expected doris::Exception to be thrown";
    } catch (const doris::Exception& e) {
        EXPECT_EQ(e.code(), doris::ErrorCode::NOT_FOUND);
    } catch (...) {
        FAIL() << "Unexpected exception type";
    }
}

TEST(CompositeReaderTest, DuplicateSetThrowsIndexInvalidParameters) {
    query_v2::CompositeReader cr;

    std::wstring field = StringHelper::to_wstring("dup");
    lucene::index::IndexReader* reader = nullptr;

    cr.set_reader(field, reader);

    try {
        cr.set_reader(field, reader);
        FAIL() << "Expected doris::Exception to be thrown";
    } catch (const doris::Exception& e) {
        EXPECT_EQ(e.code(), doris::ErrorCode::INDEX_INVALID_PARAMETERS);
    } catch (...) {
        FAIL() << "Unexpected exception type";
    }
}

TEST(CompositeReaderTest, CloseOnEmptyDoesNotCrash) {
    query_v2::CompositeReader cr;
    cr.close();
}

} // namespace doris::segment_v2