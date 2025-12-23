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

/**
 * Statistics that are available for all types of columns.
 */
public interface ColumnStatistics {
  /**
   * Get the number of values in this column. It will differ from the number
   * of rows because of NULL values and repeated values.
   * @return the number of values
   */
  long getNumberOfValues();

  /**
   * Returns true if there are nulls in the scope of column statistics.
   * @return true if null present else false
   */
  boolean hasNull();

  /**
   * Get the number of bytes for this column.
   * @return the number of bytes
   */
  long getBytesOnDisk();
}
