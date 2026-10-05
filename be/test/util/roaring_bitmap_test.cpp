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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "roaring/roaring.hh"

namespace doris {

TEST(RaringBitmapTest, IsAllOne) {
    std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
    bitmap->addRange(0, 1024);
    EXPECT_TRUE(bitmap->contains(1));
    EXPECT_FALSE(bitmap->contains(1025));
    EXPECT_EQ(bitmap->cardinality(), 1024);

    std::shared_ptr<roaring::Roaring> bitmap2 = std::make_shared<roaring::Roaring>();
    bitmap2->addRange(26, 31);
    // and
    *bitmap &= *bitmap2;
    EXPECT_TRUE(bitmap->contains(26));
    EXPECT_FALSE(bitmap->contains(25));
    EXPECT_EQ(bitmap->cardinality(), 5);

    // or
    std::shared_ptr<roaring::Roaring> bitmap3 = std::make_shared<roaring::Roaring>();
    bitmap3->addRange(0, 1024);
    *bitmap |= *bitmap3;
    EXPECT_TRUE(bitmap->contains(1));
    EXPECT_TRUE(bitmap->contains(31));
    EXPECT_FALSE(bitmap->contains(1025));

    // not
    std::shared_ptr<roaring::Roaring> bitmap4 = std::make_shared<roaring::Roaring>();
    bitmap4->addRange(32, 2048);
    *bitmap -= *bitmap4;
    EXPECT_EQ(0, bitmap->minimum());
    EXPECT_EQ(bitmap->maximum(), 31);
    EXPECT_EQ(bitmap->cardinality(), 32);
}

} // namespace doris