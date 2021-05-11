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

package org.apache.doris.load.loadv2.dpp;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

// Parser to validate value for different type
public abstract class ColumnParser implements Serializable {

    protected static final Logger LOG = LoggerFactory.getLogger(ColumnParser.class);

    // thread safe formatter
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    public static ColumnParser create(EtlJobConfig.EtlColumn etlColumn) throws SparkDppException {
        String columnType = etlColumn.columnType;
        if (columnType.equalsIgnoreCase("TINYINT")) {
            return new TinyIntParser();
        } else if (columnType.equalsIgnoreCase("SMALLINT")) {
            return new SmallIntParser();
        } else if (columnType.equalsIgnoreCase("INT")) {
            return new IntParser();
        } else if (columnType.equalsIgnoreCase("BIGINT")) {
            return new BigIntParser();
        } else if (columnType.equalsIgnoreCase("FLOAT")) {
            return new FloatParser();
        } else if (columnType.equalsIgnoreCase("DOUBLE")) {
            return new DoubleParser();
        } else if (columnType.equalsIgnoreCase("BOOLEAN")) {
            return new BooleanParser();
        } else if (columnType.equalsIgnoreCase("DATE")) {
            return new DateParser();
        } else if (columnType.equalsIgnoreCase("DATETIME")) {
            return new DatetimeParser();
        } else if (columnType.equalsIgnoreCase("VARCHAR")
                || columnType.equalsIgnoreCase("CHAR")
                || columnType.equalsIgnoreCase("BITMAP")
                || columnType.equalsIgnoreCase("HLL")) {
            return new StringParser(etlColumn);
        } else if (columnType.equalsIgnoreCase("DECIMALV2")) {
            return new DecimalParser(etlColumn);
        } else if (columnType.equalsIgnoreCase("LARGEINT")) {
            return new LargeIntParser();
        } else {
            throw new SparkDppException("unsupported type:" + columnType);
        }
    }

    public abstract boolean parse(String value);
}

class TinyIntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Byte.parseByte(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class SmallIntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Short.parseShort(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class IntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class BigIntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Long.parseLong(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class FloatParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Float ret = Float.parseFloat(value);
            return !ret.isNaN() && !ret.isInfinite();
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

class DoubleParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Double ret = Double.parseDouble(value);
            return !ret.isInfinite() && !ret.isNaN();
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

class BooleanParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        if (value.equalsIgnoreCase("true")
                || value.equalsIgnoreCase("false")
                || value.equals("0") || value.equals("1")) {
            return true;
        }
        return false;
    }
}

class DateParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            DATE_FORMATTER.parseDateTime(value);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }
}

class DatetimeParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            DATE_TIME_FORMATTER.parseDateTime(value);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }
}

class StringParser extends ColumnParser {

    private EtlJobConfig.EtlColumn etlColumn;

    public StringParser(EtlJobConfig.EtlColumn etlColumn) {
        this.etlColumn = etlColumn;
    }

    @Override
    public boolean parse(String value) {
        try {
            return value.getBytes("UTF-8").length <= etlColumn.stringLength;
        } catch (Exception e) {
            throw new RuntimeException("string check failed ", e);
        }
    }
}

class DecimalParser extends ColumnParser {

    public static int PRECISION = 27;
    public static int SCALE = 9;

    private BigDecimal maxValue;
    private BigDecimal minValue;

    public DecimalParser(EtlJobConfig.EtlColumn etlColumn) {
        StringBuilder precisionStr = new StringBuilder();
        for (int i = 0; i < etlColumn.precision - etlColumn.scale; i++) {
            precisionStr.append("9");
        }
        StringBuilder scaleStr = new StringBuilder();
        for (int i = 0; i < etlColumn.scale; i++) {
            scaleStr.append("9");
        }
        maxValue = new BigDecimal(precisionStr.toString() + "." + scaleStr.toString());
        minValue = new BigDecimal("-" + precisionStr.toString() + "." + scaleStr.toString());
    }

    @Override
    public boolean parse(String value) {
        try {
            BigDecimal bigDecimal = new BigDecimal(value);
            return bigDecimal.precision() - bigDecimal.scale() <= PRECISION - SCALE && bigDecimal.scale() <= SCALE;
        } catch (NumberFormatException e) {
            return false;
        } catch (Exception e) {
            throw new RuntimeException("decimal parse failed ", e);
        }
    }

    public BigDecimal getMaxValue() {
        return maxValue;
    }

    public BigDecimal getMinValue() {
        return minValue;
    }
}

class LargeIntParser extends ColumnParser {

    private BigInteger maxValue = new BigInteger("170141183460469231731687303715884105727");
    private BigInteger minValue = new BigInteger("-170141183460469231731687303715884105728");

    @Override
    public boolean parse(String value) {
        try {
            BigInteger inputValue = new BigInteger(value);
            return inputValue.compareTo(maxValue) < 0 && inputValue.compareTo(minValue) > 0;
        } catch (NumberFormatException e) {
            return false;
        } catch (ArithmeticException e) {
            LOG.warn("int value is too big even for java BigInteger,value={}" + value);
            return false;
        } catch (Exception e) {
            throw new RuntimeException("large int parse failed:" + value, e);
        }
    }
}