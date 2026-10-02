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

#include "util/security.h"

#include <gtest/gtest.h>

namespace doris {

TEST(SecurityTest, masks_tokens_and_gcs_upload_sessions) {
    EXPECT_EQ(mask_token("https://host/path?token=abc.def&x=1"),
              "https://host/path?token=******&x=1");
    EXPECT_EQ(mask_token("https://host/path?upload_id=secret-session_1&x=1"),
              "https://host/path?upload_id=******&x=1");
}

} // namespace doris
