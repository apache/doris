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

#include "snii/format/format_constants.h"

#include <gtest/gtest.h>

#include "common/status.h"

using namespace snii::format;

// Lock down on-disk contract values to prevent accidental changes from breaking
// readability of already-written files.
TEST(SniiFormatConstants, MagicAndVersionStable) {
    EXPECT_EQ(kContainerMagic, 0x49494E53U);
    EXPECT_EQ(kTailMagic, 0x4C494154U);
    EXPECT_EQ(kFormatVersion, 2);
    EXPECT_EQ(kMinReaderVersion, 2);
}

TEST(SniiFormatConstants, ConfigToTierMapping) {
    EXPECT_EQ(tier_of(IndexConfig::kDocsOnly), IndexTier::kT1);
    EXPECT_EQ(tier_of(IndexConfig::kDocsPositions), IndexTier::kT2);
    EXPECT_EQ(tier_of(IndexConfig::kDocsPositionsScoring), IndexTier::kT3);
}

TEST(SniiFormatConstants, CapabilityPredicates) {
    EXPECT_FALSE(has_positions(IndexConfig::kDocsOnly));
    EXPECT_TRUE(has_positions(IndexConfig::kDocsPositions));
    EXPECT_TRUE(has_scoring(IndexConfig::kDocsPositionsScoring));
    EXPECT_FALSE(has_scoring(IndexConfig::kDocsPositions));
}

TEST(SniiFormatConstants, DictFlagBitsDistinct) {
    EXPECT_EQ(dict_flags::kKind, 0x01);
    EXPECT_EQ(dict_flags::kEnc, 0x02);
    EXPECT_EQ(dict_flags::kHasSb, 0x04);
}
