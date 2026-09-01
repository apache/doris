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

#include "service/outfile_marker_state.h"

#include <gtest/gtest.h>

namespace doris {

TEST(OutfileMarkerStateTest, ReclaimsSuccessfulStateAfterProtectionWindow) {
    const auto now = std::chrono::steady_clock::now();
    OutfileMarkerState state {.updated_at = now - OUTFILE_MARKER_STATE_TTL,
                              .owned_path = "success-marker",
                              .tombstoned = false};

    EXPECT_TRUE(should_expire_outfile_marker_state(state, now));
}

TEST(OutfileMarkerStateTest, RetainsFailedDeleteTombstone) {
    const auto now = std::chrono::steady_clock::now();
    OutfileMarkerState state {.updated_at = now - OUTFILE_MARKER_STATE_TTL,
                              .owned_path = "success-marker",
                              .tombstoned = true};

    EXPECT_FALSE(should_expire_outfile_marker_state(state, now));

    state.owned_path.clear();
    EXPECT_TRUE(should_expire_outfile_marker_state(state, now));
}

TEST(OutfileMarkerStateTest, SyncsOnlyLocalSuccessMarker) {
    EXPECT_TRUE(should_sync_outfile_marker(TStorageBackendType::LOCAL));
    EXPECT_FALSE(should_sync_outfile_marker(TStorageBackendType::S3));
}

} // namespace doris
