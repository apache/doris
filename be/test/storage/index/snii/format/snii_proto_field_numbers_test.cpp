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

// SNII core metadata field numbers are ON DISK.
//
// protobuf identifies a field by its tag, never by its name, so renaming is free
// and RENUMBERING silently changes what an existing segment decodes to. The SNII
// format shipped before these messages reached upstream; the shipped layout is
// the one every existing segment was written with, and it is what these cases
// pin. Two of the fields are especially unforgiving:
//
//   SniiStatsPB tag 4 is null_count. Upstream had inserted sum_total_term_freq
//   there. Both are uint64, so a mismatched read yields a plausible wrong number
//   -- a null count used as a token sum, which then feeds avgdl -- with no error
//   anywhere.
//
//   SniiSectionRefsPB tags 3 and 4 are null_bitmap and bsbf. Upstream had
//   inserted norms at 3, which makes a reader take the null bitmap's
//   offset/length for the norms region and lose bsbf entirely.
//
// APPEND new fields. Never insert, never renumber, never reuse.

#include <gen_cpp/snii.pb.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace doris::snii {
namespace {

void expect_field_numbers(const google::protobuf::Descriptor* descriptor,
                          const std::vector<std::pair<std::string, int>>& expected) {
    ASSERT_NE(descriptor, nullptr);
    for (const auto& [name, number] : expected) {
        const auto* field = descriptor->FindFieldByName(name);
        ASSERT_NE(field, nullptr) << descriptor->full_name() << " lost field '" << name << "'";
        EXPECT_EQ(field->number(), number)
                << descriptor->full_name() << "." << name << " moved from tag " << number << " to "
                << field->number() << "; existing segments decode this tag as "
                << "something else";
    }
}

} // namespace

TEST(SniiProtoFieldNumbers, StatsMatchesTheShippedLayout) {
    expect_field_numbers(SniiStatsPB::descriptor(), {{"doc_count", 1},
                                                     {"indexed_doc_count", 2},
                                                     {"term_count", 3},
                                                     {"null_count", 4},
                                                     {"sum_total_term_freq", 5}});
}

TEST(SniiProtoFieldNumbers, SectionRefsMatchTheShippedLayout) {
    expect_field_numbers(SniiSectionRefsPB::descriptor(), {{"dict_region", 1},
                                                           {"posting_region", 2},
                                                           {"null_bitmap", 3},
                                                           {"bsbf", 4},
                                                           {"norms", 5}});
}

TEST(SniiProtoFieldNumbers, CoreMetadataMatchesTheShippedLayout) {
    expect_field_numbers(SniiCoreMetadataPB::descriptor(), {{"index_config", 1},
                                                            {"stats", 2},
                                                            {"section_refs", 3},
                                                            {"common_grams", 4},
                                                            {"common_grams_posting_policy", 5}});
}

// The CommonGrams block was APPENDED upstream (tags 8-12 were free in the
// shipped layout), which is the shape every future addition must copy.
TEST(SniiProtoFieldNumbers, CommonGramsMetadataAppendsRatherThanInserts) {
    expect_field_numbers(SniiCommonGramsMetadataPB::descriptor(),
                         {{"plain_term_key_version", 1},
                          {"common_grams_coverage", 2},
                          {"common_grams_semantics_version", 3},
                          {"common_grams_key_version", 4},
                          {"common_grams_dictionary_identity", 5},
                          {"base_analyzer_fingerprint", 6},
                          {"common_grams_fingerprint", 7},
                          {"scoring_coverage", 8},
                          {"scoring_stats_version", 9},
                          {"norm_semantics_version", 10},
                          {"scoring_doc_count", 11},
                          {"scoring_token_count", 12}});
}

} // namespace doris::snii
