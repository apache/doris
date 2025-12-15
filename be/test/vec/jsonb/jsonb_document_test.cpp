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

#include "util/jsonb_document.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "vec/core/types.h"

namespace doris {
class JsonbDocumentTest : public testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(JsonbDocumentTest, invaild_jsonb_document) {
    const JsonbDocument* doc = nullptr;
    auto st = JsonbDocument::checkAndCreateDocument(nullptr, 0, &doc);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(doc != nullptr);
    EXPECT_TRUE(doc->getValue()->isNull());

    JsonbToJson jsonb_to_json;
    std::string json_null = jsonb_to_json.to_json_string(doc->getValue());
    EXPECT_EQ(json_null, "null");

    std::string json_string = JsonbToJson::jsonb_to_json_string(nullptr, 0);
    EXPECT_EQ(json_null, json_string);
}

} // namespace doris