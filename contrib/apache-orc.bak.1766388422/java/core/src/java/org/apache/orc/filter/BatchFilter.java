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

package org.apache.orc.filter;

import org.apache.orc.OrcFilterContext;

import java.util.function.Consumer;

/**
 * Defines a batch filter that can operate on a
 * {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch} and filter rows by using the
 * selected vector to determine the eligible rows.
 */
public interface BatchFilter extends Consumer<OrcFilterContext> {

  /**
   * Identifies the filter column names. These columns will be read before the filter is applied.
   *
   * @return Names of the filter columns
   */
  String[] getColumnNames();
}
