/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl.filter;

import com.google.auto.service.AutoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.orc.filter.BatchFilter;
import org.apache.orc.filter.PluginFilterService;

import java.util.Locale;

@AutoService(PluginFilterService.class)
public class MyFilterService implements PluginFilterService {
  @Override
  public BatchFilter getFilter(String filePath, Configuration conf) {
    if (!filePath.matches(conf.get("my.filter.scope", ""))) {
      return null;
    }

    switch (conf.get("my.filter.name", "")) {
      case "my_str_i_eq":
        return makeFilter(new StringIgnoreCaseEquals(conf));
      case "my_long_abs_eq":
        return makeFilter(new LongAbsEquals(conf));
      default:
        return null;
    }
  }

  private static BatchFilter makeFilter(LeafFilter filter) {
    return BatchFilterFactory.create(filter, new String[] {filter.getColName()});
  }

  public static class StringIgnoreCaseEquals extends LeafFilter {
    private final String value;
    private final Locale locale;

    protected StringIgnoreCaseEquals(Configuration conf) {
      this(conf.get("my.filter.col.name"),
           conf.get("my.filter.col.value"),
           conf.get("my.filter.lang_tag") == null ?
             Locale.ROOT :
             Locale.forLanguageTag(conf.get("my.filter.lang_tag")));
    }

    protected StringIgnoreCaseEquals(String colName, String value, Locale locale) {
      super(colName, false);
      if (colName.isEmpty()) {
        throw new IllegalArgumentException("Filter needs a valid column name");
      }
      this.locale = locale;
      this.value = value.toLowerCase(locale);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((BytesColumnVector) v).toString(rowIdx).toLowerCase(locale).equals(value);
    }
  }

  public static class LongAbsEquals extends LeafFilter {
    private final long value;

    protected LongAbsEquals(Configuration conf) {
      this(conf.get("my.filter.col.name"),
           conf.getLong("my.filter.col.value", -1));
    }

    protected LongAbsEquals(String colName, long value) {
      super(colName, false);
      assert !colName.isEmpty() : "Filter needs a valid column name";
      this.value = Math.abs(value);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return Math.abs(((LongColumnVector) v).vector[rowIdx]) == value;
    }
  }

}
