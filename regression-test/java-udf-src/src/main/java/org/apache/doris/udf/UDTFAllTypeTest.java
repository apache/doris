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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;

public class UDTFAllTypeTest {
    public static class UdtfBoolean {
        public ArrayList<Boolean> evaluate(Boolean value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<Boolean> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add((i % 2 == 0));
            }
            return result;
        }
    }

    public static class UdtfByte {
        public ArrayList<Byte> evaluate(Byte value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<Byte> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add((byte) (value + i));
            }
            return result;
        }
    }

    public static class UdtfShort {
        public ArrayList<Short> evaluate(Short value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<Short> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add((short) (value + i * 2));
            }
            return result;
        }
    }

    public static class UdtfInt {
        public ArrayList<Integer> evaluate(Integer value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<Integer> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add(value + i * 3);
            }
            return result;
        }
    }

    public static class UdtfLong {
        public ArrayList<Long> evaluate(Long value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<Long> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add((long) (value + i * 4));
            }
            return result;
        }
    }

    public static class UdtfLargeInt {
        public ArrayList<BigInteger> evaluate(BigInteger value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<BigInteger> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add(new BigInteger(String.valueOf(i * 5)).add(value));
            }
            return result;
        }
    }

    public static class UdtfFloat {
        public ArrayList<Float> evaluate(Float value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<Float> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add((float) (value + i * 0.1));
            }
            return result;
        }
    }

    public static class UdtfDouble {
        public ArrayList<Double> evaluate(Double value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<Double> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add(value + i * 0.01);
            }
            return result;
        }
    }

    public static class UdtfDecimal {
        public ArrayList<BigDecimal> evaluate(BigDecimal value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<BigDecimal> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add(new BigDecimal(String.valueOf(i * 0.001)).add(value));
            }
            return result;
        }
    }

    public static class UdtfDate {
        public ArrayList<LocalDate> evaluate(LocalDate value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<LocalDate> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add(value.plusMonths(i));
            }
            return result;
        }
    }

    public static class UdtfDateTime {
        public ArrayList<LocalDateTime> evaluate(LocalDateTime value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<LocalDateTime> result = new ArrayList<>();
            for (int i = 0; i < count; ++i) {
                result.add(value.plusDays(i));
            }
            return result;
        }
    }

    public static class UdtfString {
        public ArrayList<String> evaluate(String value, String separator) {
            if (value == null || separator == null) {
                return null;
            } else {
                return new ArrayList<>(Arrays.asList(value.split(separator)));
            }
        }
    }

    public static class UdtfList {
        public ArrayList<String> evaluate(ArrayList<String> value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            if (count % 2 == 1) {
                Collections.reverse(value);
            }
            return value;
        }
    }

    public static class UdtfMap {
        public ArrayList<String> evaluate(HashMap<String, String> value, Integer count) {
            if (value == null || count == null) {
                return null;
            }
            ArrayList<String> result;
            if (count % 2 == 1) {
                result = new ArrayList<>(value.keySet());
            } else {
                result = new ArrayList<>(value.values());
            }
            return result;
        }
    }

}
