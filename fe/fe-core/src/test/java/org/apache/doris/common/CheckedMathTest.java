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

package org.apache.doris.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CheckedMathTest {

    @Test
    void checkedMultiply() {
        double a = 12.91;
        double b = 21.44;
        double res = CheckedMath.checkedMultiply(a, b);
        Assertions.assertEquals(a * b, res, 0.01);
        a = Double.MAX_VALUE;
        b = 5;
        res = CheckedMath.checkedMultiply(a, b);
        Assertions.assertEquals(Double.MAX_VALUE, res, 0.01);
    }
}
