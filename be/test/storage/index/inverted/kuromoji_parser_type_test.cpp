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

#include <gtest/gtest.h>

#include <map>
#include <string>

#include "storage/index/inverted/inverted_index_parser.h"

namespace doris {

TEST(KuromojiParserTypeTest, StringToEnum) {
    EXPECT_EQ(get_inverted_index_parser_type_from_string("kuromoji"),
              InvertedIndexParserType::PARSER_KUROMOJI);
    EXPECT_EQ(get_inverted_index_parser_type_from_string("KUROMOJI"),
              InvertedIndexParserType::PARSER_KUROMOJI);
}

TEST(KuromojiParserTypeTest, EnumToString) {
    EXPECT_EQ(inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_KUROMOJI),
              "kuromoji");
}

TEST(KuromojiParserTypeTest, DefaultModeIsSearch) {
    std::map<std::string, std::string> props = {{"parser", "kuromoji"}};
    EXPECT_EQ(get_parser_mode_string_from_properties(props), "search");
}

} // namespace doris
