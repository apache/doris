/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
  -->
 */
package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class DorisJsonCodec {

    private static String timeZone = "GMT+8";
    private static TimeZone timeZoner = TimeZone.getTimeZone(timeZone);

    private final List<String> fieldNames;

    public DorisJsonCodec(final List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public String serialize(final Record row) {
        if (null == this.fieldNames) {
            return "";
        }
        final Map<String, Object> rowMap = new HashMap<String, Object>(this.fieldNames.size());
        int idx = 0;
        for (final String fieldName : this.fieldNames) {
            rowMap.put(fieldName, this.columnConvert2String(row.getColumn(idx)));
            ++idx;
        }
        return JSON.toJSONString(rowMap);
    }


    /**
     *  convert datax internal  data to string
     *
     * @param col
     * @return
     */
    private String columnConvert2String(final Column col) {
        if (null == col.getRawData()) {
            return null;
        }
        if (Column.Type.BOOL == col.getType()) {
            return String.valueOf(col.asLong());
        }
        if (Column.Type.DATE != col.getType()) {
            return col.asString();
        }
        final DateColumn.DateType type = ((DateColumn) col).getSubType();
        if (type == DateColumn.DateType.DATE) {
            return DateFormatUtils.format(col.asDate(), "yyyy-MM-dd", timeZoner);
        }
        if (type == DateColumn.DateType.TIME) {
            return DateFormatUtils.format(col.asDate(), "HH:mm:ss", timeZoner);
        }
        if (type == DateColumn.DateType.DATETIME) {
            return DateFormatUtils.format(col.asDate(), "yyyy-MM-dd HH:mm:ss", timeZoner);
        }
        return null;
    }


}