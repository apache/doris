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

import com.alibaba.datax.common.element.Record;

import java.util.ArrayList;
import java.util.List;

// Convert DataX data to csv
public class DorisCsvCodec extends DorisCodec {

    private final String columnSeparator;

    public DorisCsvCodec(final List<String> fieldNames, String columnSeparator) {
        super(fieldNames);
        this.columnSeparator = columnSeparator;
    }

    @Override
    public String serialize(final Record row) {
        if (null == this.fieldNames) {
            return "";
        }
        List<String> list = new ArrayList<>();

        for (int i = 0; i < this.fieldNames.size(); i++) {
            Object value = this.convertColumn(row.getColumn(i));
            list.add(value != null ? value.toString() : "\\N");
        }

        return String.join(columnSeparator, list);
    }

}