/*
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

package org.apache.orc.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDateUtils {
  /**
   * Test case for DateColumnVector's changeCalendar
   * epoch days, hybrid representation, proleptic representation
   *   16768: hybrid: 2015-11-29 proleptic: 2015-11-29
   * -141418: hybrid: 1582-10-24 proleptic: 1582-10-24
   * -141427: hybrid: 1582-10-15 proleptic: 1582-10-15
   * -141428: hybrid: 1582-10-04 proleptic: 1582-10-14
   * -141430: hybrid: 1582-10-02 proleptic: 1582-10-12
   * -141437: hybrid: 1582-09-25 proleptic: 1582-10-05
   * -141438: hybrid: 1582-09-24 proleptic: 1582-10-04
   * -499952: hybrid: 0601-03-04 proleptic: 0601-03-07
   * -499955: hybrid: 0601-03-01 proleptic: 0601-03-04
   * @throws Exception
   */
  @Test
  public void testConversion() throws Exception {
    checkConversion(16768, "2015-11-29", "2015-11-29");
    checkConversion(-141418, "1582-10-24", "1582-10-24");
    checkConversion(-141427, "1582-10-15", "1582-10-15");
    checkConversion(-141428, "1582-10-04", "1582-10-14");
    checkConversion(-141430, "1582-10-02", "1582-10-12");
    checkConversion(-141437, "1582-09-25", "1582-10-05");
    checkConversion(-499952, "0601-03-04", "0601-03-07");
    checkConversion(-499955, "0601-03-01", "0601-03-04");
  }

  void checkConversion(int dayOfEpoch, String hybrid, String proleptic) {
    String result = DateUtils.printDate(dayOfEpoch, false);
    assertEquals(hybrid, result, "day " + dayOfEpoch);
    assertEquals(dayOfEpoch, (int) DateUtils.parseDate(result, false));
    result = DateUtils.printDate(dayOfEpoch, true);
    assertEquals(proleptic, result, "day " + dayOfEpoch);
    assertEquals(dayOfEpoch, (int) DateUtils.parseDate(result, true));
  }
}
