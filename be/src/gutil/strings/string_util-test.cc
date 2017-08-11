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
//
// Some portions Copyright 2013 The Chromium Authors. All rights reserved.
#include "gutil/strings/util.h"

#include <gtest/gtest.h>

namespace kudu {

TEST(StringUtilTest, MatchPatternTest) {
  EXPECT_TRUE(MatchPattern("www.google.com", "*.com"));
  EXPECT_TRUE(MatchPattern("www.google.com", "*"));
  EXPECT_FALSE(MatchPattern("www.google.com", "www*.g*.org"));
  EXPECT_TRUE(MatchPattern("Hello", "H?l?o"));
  EXPECT_FALSE(MatchPattern("www.google.com", "http://*)"));
  EXPECT_FALSE(MatchPattern("www.msn.com", "*.COM"));
  EXPECT_TRUE(MatchPattern("Hello*1234", "He??o\\*1*"));
  EXPECT_FALSE(MatchPattern("", "*.*"));
  EXPECT_TRUE(MatchPattern("", "*"));
  EXPECT_TRUE(MatchPattern("", "?"));
  EXPECT_TRUE(MatchPattern("", ""));
  EXPECT_FALSE(MatchPattern("Hello", ""));
  EXPECT_TRUE(MatchPattern("Hello*", "Hello*"));
  // Stop after a certain recursion depth.
  EXPECT_FALSE(MatchPattern("123456789012345678", "?????????????????*"));

  // Test UTF8 matching.
  EXPECT_TRUE(MatchPattern("heart: \xe2\x99\xa0", "*\xe2\x99\xa0"));
  EXPECT_TRUE(MatchPattern("heart: \xe2\x99\xa0.", "heart: ?."));
  EXPECT_TRUE(MatchPattern("hearts: \xe2\x99\xa0\xe2\x99\xa0", "*"));
  // Invalid sequences should be handled as a single invalid character.
  EXPECT_TRUE(MatchPattern("invalid: \xef\xbf\xbe", "invalid: ?"));
  // If the pattern has invalid characters, it shouldn't match anything.
  EXPECT_FALSE(MatchPattern("\xf4\x90\x80\x80", "\xf4\x90\x80\x80"));

  // This test verifies that consecutive wild cards are collapsed into 1
  // wildcard (when this doesn't occur, MatchPattern reaches it's maximum
  // recursion depth).
  EXPECT_TRUE(MatchPattern("Hello" ,
                           "He********************************o")) ;
}

} // namespace kudu
