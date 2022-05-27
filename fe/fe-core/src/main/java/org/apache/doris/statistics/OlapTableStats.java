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

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * There are the statistics of OlapTable.
 * The @OlapTableStats are mainly used to provide input for the Optimizer's cost model.
 *
 * There are three kinds of statistics of OlapTable.
 * @rowCount: The row count of OlapTable. There are two ways to obtain value:
 *            1. The sum row count of @TabletStats which maybe an inaccurate value.
 *            2. count(*) of OlapTable which is an accurate value.
 * @dataSize: The data size of OlapTable. This is an inaccurate value,
 *             which is obtained by summing the @dataSize of @TabletStats.
 * @idToTabletStats: <@Long tabletId, @TabletStats tabletStats>
 *     Each tablet in the OlapTable will have corresponding @TabletStats.
 *     Those @TabletStats are recorded in @idToTabletStats form of MAP.
 *     This facilitates the optimizer to quickly find the corresponding
 *     @TabletStats based on the tablet id.
 *     At the same time, both @rowCount and @dataSize can also be obtained
 *     from the sum of all @TabletStats.
 *
 */
public class OlapTableStats extends TableStats {

    private Map<Long, TabletStats> idToTabletStats = Maps.newHashMap();
}
