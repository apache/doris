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

import java.sql.Timestamp;

/**
 * Statistics for Timestamp columns.
 */
public interface TimestampColumnStatistics extends ColumnStatistics {
  /**
   * Get the minimum value for the column.
   * @return minimum value
   */
  Timestamp getMinimum();

  /**
   * Get the maximum value for the column.
   * @return maximum value
   */
  Timestamp getMaximum();

  /**
   * Get the minimum value for the column in UTC.
   * @return minimum value in UTC
   */
  Timestamp getMinimumUTC();

  /**
   * Get the maximum value for the column in UTC.
   * @return maximum value in UTC
   */
  Timestamp getMaximumUTC();
}
