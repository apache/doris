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

package org.apache.orc;

import org.apache.orc.impl.ColumnStatisticsImpl;

import java.util.List;

/**
 * The statistics for a stripe.
 */
public class StripeStatistics {
  protected final List<OrcProto.ColumnStatistics> cs;
  protected final TypeDescription schema;
  private final boolean writerUsedProlepticGregorian;
  private final boolean convertToProlepticGregorian;

  public StripeStatistics(List<OrcProto.ColumnStatistics> list) {
    this(null, list, false, false);
  }

  public StripeStatistics(TypeDescription schema,
                          List<OrcProto.ColumnStatistics> list,
                          boolean writerUsedProlepticGregorian,
                          boolean convertToProlepticGregorian) {
    this.schema = schema;
    this.cs = list;
    this.writerUsedProlepticGregorian = writerUsedProlepticGregorian;
    this.convertToProlepticGregorian = convertToProlepticGregorian;
  }

  private int getBase() {
    return schema == null ? 0 : schema.getId();
  }

  /**
   * Return list of column statistics
   *
   * @return column stats
   */
  public ColumnStatistics[] getColumnStatistics() {
    ColumnStatistics[] result = new ColumnStatistics[cs.size()];
    int base = getBase();
    for (int c = 0; c < result.length; ++c) {
      TypeDescription column = schema == null ? null : schema.findSubtype(base + c);
      result[c] = ColumnStatisticsImpl.deserialize(column, cs.get(c),
          writerUsedProlepticGregorian, convertToProlepticGregorian);
    }
    return result;
  }

  public OrcProto.ColumnStatistics getColumn(int column) {
    return cs.get(column);
  }
}
