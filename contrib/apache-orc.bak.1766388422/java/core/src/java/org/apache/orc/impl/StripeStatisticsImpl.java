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

package org.apache.orc.impl;

import org.apache.orc.OrcProto;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;

public class StripeStatisticsImpl extends StripeStatistics  {
  public StripeStatisticsImpl(TypeDescription schema,
                              List<OrcProto.ColumnStatistics> list,
                              boolean writerUsedProlepticGregorian,
                              boolean convertToProlepticGregorian) {
    super(schema, list, writerUsedProlepticGregorian, convertToProlepticGregorian);
  }

  public StripeStatisticsImpl(TypeDescription schema,
                              boolean writerUsedProlepticGregorian,
                              boolean convertToProlepticGregorian) {
    super(schema, createList(schema), writerUsedProlepticGregorian,
        convertToProlepticGregorian);
  }

  /**
   * Create a list that will be filled in later.
   * @param schema the schema for this stripe statistics
   * @return a new list of nulls for each column
   */
  private static List<OrcProto.ColumnStatistics> createList(TypeDescription schema) {
    int len = schema.getMaximumId() - schema.getId() + 1;
    List<OrcProto.ColumnStatistics> result = new ArrayList<>(len);
    for(int c=0; c < len; ++c) {
      result.add(null);
    }
    return result;
  }

  public void updateColumn(int column, OrcProto.ColumnStatistics elem) {
    cs.set(column, elem);
  }
}
