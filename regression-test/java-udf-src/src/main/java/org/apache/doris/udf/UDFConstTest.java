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

package org.apache.doris.udf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

public class UDFConstTest {
    public static class ConstBoolean {
        public Boolean evaluate(Integer i, Boolean value) {
            return value;
        }
    }

    public static class ConstInt {
        public Integer evaluate(Integer i, Integer value) {
            return value;
        }
    }

    public static class ConstFloat {
        public Float evaluate(Integer i, Float value) {
            return value;
        }
    }

    public static class ConstDouble {
        public Double evaluate(Integer i, Double value) {
            return value;
        }
    }

    public static class ConstLargeInt {
        public BigInteger evaluate(Integer i, BigInteger value) {
            return value;
        }
    }

    public static class ConstDecimal {
        public BigDecimal evaluate(Integer i, BigDecimal value) {
            return value;
        }
    }

    public static class ConstDate {
        public LocalDate evaluate(Integer i, LocalDate value) {
            return value;
        }
    }

    public static class ConstDateTime {
        public LocalDateTime evaluate(Integer i, LocalDateTime value) {
            return value;
        }
    }

    public static class ConstString {
        public String evaluate(Integer i, String value) {
            return value;
        }
    }

    public static class ConstArray {
        public ArrayList<String> evaluate(Integer i, ArrayList<String> value) {
            return value;
        }
    }

    public static class ConstMap {
        public HashMap<String, String> evaluate(Integer i, HashMap<String, String> value) {
            return value;
        }
    }

    public static class ConstStruct {
        public ArrayList<Object> evaluate(Integer i, ArrayList<Object> value) {
            return value;
        }
    }
}
