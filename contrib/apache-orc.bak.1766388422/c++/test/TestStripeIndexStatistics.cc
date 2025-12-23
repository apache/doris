/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "Adaptor.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestStripeIndexStatistics, testIndexStatistics) {
    std::stringstream ss;
    if (const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
      ss << example_dir;
    } else {
      ss << "../../../examples";
    }
    ss << "/orc_index_int_string.orc";
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader =
        createReader(readLocalFile(ss.str().c_str(), readerOpts.getReaderMetrics()), readerOpts);
    std::unique_ptr<orc::StripeStatistics> stripeStats = reader->getStripeStatistics(0);
    EXPECT_EQ(3, stripeStats->getNumberOfRowIndexStats(0));
    EXPECT_EQ(3, stripeStats->getNumberOfRowIndexStats(1));
    EXPECT_EQ(3, stripeStats->getNumberOfRowIndexStats(2));

    const orc::IntegerColumnStatistics* intColStats;
    intColStats = reinterpret_cast<const orc::IntegerColumnStatistics*>(
        stripeStats->getRowIndexStatistics(1, 0));
    // (syt) For forward compatibility, if has_null is not set in the orc file, it should be
    // regarded as has_null is true
    EXPECT_EQ(
        "Data type: Integer\nValues: 2000\nHas null: yes\nMinimum: 1\nMaximum: 2000\nSum: "
        "2001000\n",
        intColStats->toString());
    intColStats = reinterpret_cast<const orc::IntegerColumnStatistics*>(
        stripeStats->getRowIndexStatistics(1, 1));
    EXPECT_EQ(
        "Data type: Integer\nValues: 2000\nHas null: yes\nMinimum: 2001\nMaximum: 4000\nSum: "
        "6001000\n",
        intColStats->toString());
    intColStats = reinterpret_cast<const orc::IntegerColumnStatistics*>(
        stripeStats->getRowIndexStatistics(1, 2));
    EXPECT_EQ(
        "Data type: Integer\nValues: 2000\nHas null: yes\nMinimum: 4001\nMaximum: 6000\nSum: "
        "10001000\n",
        intColStats->toString());

    const orc::StringColumnStatistics* stringColStats;
    stringColStats = reinterpret_cast<const orc::StringColumnStatistics*>(
        stripeStats->getRowIndexStatistics(2, 0));
    EXPECT_EQ(
        "Data type: String\nValues: 2000\nHas null: yes\nMinimum: 1000\nMaximum: 9a\nTotal length: "
        "7892\n",
        stringColStats->toString());
    stringColStats = reinterpret_cast<const orc::StringColumnStatistics*>(
        stripeStats->getRowIndexStatistics(2, 1));
    EXPECT_EQ(
        "Data type: String\nValues: 2000\nHas null: yes\nMinimum: 2001\nMaximum: 4000\nTotal "
        "length: "
        "8000\n",
        stringColStats->toString());
    stringColStats = reinterpret_cast<const orc::StringColumnStatistics*>(
        stripeStats->getRowIndexStatistics(2, 2));
    EXPECT_EQ(
        "Data type: String\nValues: 2000\nHas null: yes\nMinimum: 4001\nMaximum: 6000\nTotal "
        "length: "
        "8000\n",
        stringColStats->toString());
  }

}  // namespace orc
