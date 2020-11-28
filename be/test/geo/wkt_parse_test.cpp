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

#include "geo/wkt_parse.h"

#include <gtest/gtest.h>

#include "common/logging.h"
#include "geo/geo_types.h"
#include "geo/wkt_parse_ctx.h"

namespace doris {

class WktParseTest : public testing::Test {
public:
    WktParseTest() {}
    virtual ~WktParseTest() {}
};

TEST_F(WktParseTest, normal) {
    const char* wkt = "POINT(1 2)";

    GeoShape* shape = nullptr;
    auto status = WktParse::parse_wkt(wkt, strlen(wkt), &shape);
    ASSERT_EQ(GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
    LOG(INFO) << "parse result: " << shape->to_string();
    delete shape;
}

TEST_F(WktParseTest, invalid_wkt) {
    const char* wkt = "POINT(1,2)";

    GeoShape* shape = nullptr;
    auto status = WktParse::parse_wkt(wkt, strlen(wkt), &shape);
    ASSERT_NE(GEO_PARSE_OK, status);
    ASSERT_EQ(nullptr, shape);
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
