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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

/*
 * Author: Chenmingyu
 * Date: Mar 13, 2019
 */

public class FEFunctionsTest {

    @Test
    public void unixtimestampTest() {
        try {
            IntLiteral timestamp = FEFunctions.unix_timestamp(new DateLiteral("2018-01-01", Type.DATE));
            Assert.assertEquals(1514736000, timestamp.getValue());
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
