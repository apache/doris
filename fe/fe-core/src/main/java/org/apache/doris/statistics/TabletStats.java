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

package org.apache.doris.statistics;

/**
 * There are the statistics of one tablet.
 * The tablet stats are mainly used to provide input for the Optimizer's cost model.
 *
 * The description of tablet stats are following:
 * 1. @rowCount: The row count of tablet.
 * 2. @dataSize: The data size of tablet.
 *
 * @rowCount: The row count of tablet. There are two ways to update:
 *           1. The rowCount from tablet meta. The value obtained by this update method
 *           may be an inaccurate value.
 *           2. The result of count(*) query from one tablet. The value obtained by this update method
 *           is accurate.
 * @dataSize: The data size of tablet. This is a inaccurate value of one tablet.
 *
 * The granularity of the statistics is one tablet.
 * For example:
 * "@rowCount = 10" means that the row count is 1000 in one tablet.
 */
public class TabletStats {

    private long rowCount;
    private long dataSize;
}
