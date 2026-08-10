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

#include "format/table/paimon_jni_reader.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

TFileRangeDesc make_legacy_paimon_jni_range() {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_paimon_split("serialized-split");
    table_format_params.__set_paimon_params(std::move(paimon_params));
    range.__set_table_format_params(std::move(table_format_params));
    return range;
}

TEST(LegacyPaimonJniReaderTest, GeneratesMissingOrEmptySerializedTableCacheKey) {
    const auto range = make_legacy_paimon_jni_range();
    TFileScanRangeParams scan_params;
    scan_params.__set_serialized_table("serialized-table");
    scan_params.__set_paimon_predicate("serialized-predicate");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    const std::vector<SlotDescriptor*> file_slot_descs;

    PaimonJniReader missing_key_reader(file_slot_descs, &state, nullptr, range, &scan_params);
    const auto& missing_params = missing_key_reader.TEST_scanner_params();
    EXPECT_EQ(missing_params.at("serialized_table"), "serialized-table");
    const auto& missing_key = missing_params.at("serialized_table_cache_key");
    EXPECT_FALSE(missing_key.empty());

    scan_params.__set_serialized_table_cache_key("");
    PaimonJniReader empty_key_reader(file_slot_descs, &state, nullptr, range, &scan_params);
    const auto& empty_params = empty_key_reader.TEST_scanner_params();
    EXPECT_EQ(empty_params.at("serialized_table"), "serialized-table");
    const auto& empty_key = empty_params.at("serialized_table_cache_key");
    EXPECT_FALSE(empty_key.empty());
    EXPECT_NE(missing_key, empty_key);
}

} // namespace
} // namespace doris
