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

#include "format/table/es/es_http_reader.h"

#include <gtest/gtest.h>

#include <vector>

#include "runtime/runtime_profile.h"

namespace doris {

class EsHttpReaderTest : public testing::Test {
protected:
    std::vector<SlotDescriptor*> file_slot_descs;
    TFileRangeDesc range;
    TFileScanRangeParams params;
};

TEST_F(EsHttpReaderTest, RegistersProfileCounters) {
    RuntimeProfile profile("es_http_reader_test");
    EsHttpReader reader(file_slot_descs, nullptr, &profile, range, params, nullptr);

    reader.collect_profile_before_close();

    auto* es_reader = profile.get_counter("EsHttpReader");
    auto* read_time = profile.get_counter("EsReadTime");
    auto* materialize_time = profile.get_counter("EsMaterializeTime");
    auto* batches_read = profile.get_counter("EsBatchesRead");
    auto* rows_read = profile.get_counter("EsRowsRead");

    ASSERT_NE(es_reader, nullptr);
    ASSERT_NE(read_time, nullptr);
    ASSERT_NE(materialize_time, nullptr);
    ASSERT_NE(batches_read, nullptr);
    ASSERT_NE(rows_read, nullptr);
    EXPECT_EQ(read_time->value(), 0);
    EXPECT_EQ(materialize_time->value(), 0);
    EXPECT_EQ(batches_read->value(), 0);
    EXPECT_EQ(rows_read->value(), 0);
}

TEST_F(EsHttpReaderTest, NullProfileIsSafe) {
    EsHttpReader reader(file_slot_descs, nullptr, nullptr, range, params, nullptr);
    reader.collect_profile_before_close();
    SUCCEED();
}

TEST_F(EsHttpReaderTest, ExtractHostnameHandlesIpv4AndIpv6) {
    EXPECT_EQ("10.0.0.1", EsHttpReader::_extract_hostname("10.0.0.1:9200"));
    EXPECT_EQ("::1", EsHttpReader::_extract_hostname("[::1]:9200"));
    EXPECT_EQ("::1", EsHttpReader::_extract_hostname("http://[::1]:9200"));
    EXPECT_EQ("fd00::1", EsHttpReader::_extract_hostname("[fd00::1]:9200"));
    EXPECT_EQ("fd00::1", EsHttpReader::_extract_hostname("fd00::1:9200"));
}

} // namespace doris
