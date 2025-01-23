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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

public class TimeLiteralTest {

  @Test
  public void testTimeCreate() throws AnalysisException {
      TimeLiteral literal = new TimeLiteral("12:12:12");
      String s = literal.getStringValue();
      Assert.assertEquals(s, "12:12:12");
      literal = new TimeLiteral("112:00:00");
      s = literal.getStringValue();
      Assert.assertEquals(s, "112:00:00");
      literal = new TimeLiteral(21, 12, 21);
      s = literal.getStringValue();
      Assert.assertEquals(s, "21:12:21");
      literal = new TimeLiteral(838, 59, 59);
      s = literal.getStringValue();
      Assert.assertEquals(s, "838:59:59");
  }

}
