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

import org.apache.hadoop.hive.ql.exec.UDF;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

public class Echo {
    public static class EchoBoolean extends UDF {
        public Boolean evaluate(Boolean value) {
            return value;
        }
    }

    public static class EchoByte extends UDF {
        public Byte evaluate(Byte value) {
            return value;
        }
    }

    public static class EchoShort extends UDF {
        public Short evaluate(Short value) {
            return value;
        }
    }

    public static class EchoInt extends UDF {
        public Integer evaluate(Integer value) {
            return value;
        }
    }

    public static class EchoLong extends UDF {
        public Long evaluate(Long value) {
            return value;
        }
    }

    public static class EchoLargeInt extends UDF {
        public BigInteger evaluate(BigInteger value) {
            return value;
        }
    }

    public static class EchoFloat extends UDF {
        public Float evaluate(Float value) {
            return value;
        }
    }

    public static class EchoDouble extends UDF {
        public Double evaluate(Double value) {
            return value;
        }
    }

    public static class EchoDecimal extends UDF {
        public BigDecimal evaluate(BigDecimal value) {
            return value;
        }
    }

    public static class EchoDate extends UDF {
        public LocalDate evaluate(LocalDate value) {
            return value;
        }
    }

    public static class EchoDate2 extends UDF {
        public java.util.Date evaluate(java.util.Date value) {
            return value;
        }
    }

    public static class EchoDate3 extends UDF {
        public org.joda.time.LocalDate evaluate(org.joda.time.LocalDate value) {
            return value;
        }
    }

    public static class EchoDateTime extends UDF {
        public LocalDateTime evaluate(LocalDateTime value) {
            return value;
        }
    }

    public static class EchoDateTime2 extends UDF {
        public org.joda.time.DateTime evaluate(org.joda.time.DateTime value) {
            return value;
        }
    }

    public static class EchoDateTime3 extends UDF {
        public org.joda.time.LocalDateTime evaluate(org.joda.time.LocalDateTime value) {
            return value;
        }
    }

    public static class EchoString extends UDF {
        public String evaluate(String value) {
            return value;
        }
    }

    public static class EchoList extends UDF {
        public ArrayList<String> evaluate(ArrayList<String> value) {
            return value;
        }
    }

    public static class EchoMap extends UDF {
        public HashMap<String, Integer> evaluate(HashMap<String, Integer> value) {
            return value;
        }
    }
}
