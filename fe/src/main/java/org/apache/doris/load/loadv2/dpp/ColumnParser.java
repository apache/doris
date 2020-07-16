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

import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Date;

// Parser to validate value for different type
public abstract class ColumnParser implements Serializable {
    public static ColumnParser create(EtlJobConfig.EtlColumn etlColumn) throws UserException {
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
        } else {
            throw new UserException("unsupported type:" + columnType);
        }
    }

    public abstract boolean parse(String value);
}

class TinyIntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Short parsed = Short.parseShort(value);
            if (parsed > 127 || parsed < -128) {
                return false;
            }
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
            Integer.parseInt(value);
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
            Float.parseFloat(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class DoubleParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class BooleanParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        if (value.equalsIgnoreCase("true")
                || value.equalsIgnoreCase("false")) {
            return true;
        }
        return false;
    }
}

class DateParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Date.parse(value);
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
            DateTime.parse(value);
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