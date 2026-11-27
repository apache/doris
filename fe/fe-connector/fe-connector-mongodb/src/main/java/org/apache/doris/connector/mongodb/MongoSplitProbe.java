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

package org.apache.doris.connector.mongodb;

import org.bson.BsonValue;

import java.util.List;

/**
 * Bounded tuning hints; neither samples nor statistics prove that source rows are absent.
 *
 * @param samples projected fixed-width BSON scalars; unsupported/missing values are null hints
 * @param logicalBytes collStats.size, or -1 when unavailable; never compressed storageSize
 * @param sampleBytes all fetched index specs, full statistics and projected sample reply bytes;
 *                    each batch/reply is also charged immediately, including failed probes
 * @param indexed whether a complete simple B-tree prefix was admitted for the chosen field
 */
record MongoSplitProbe(List<BsonValue> samples, long logicalBytes, long sampleBytes, boolean indexed) {
    MongoSplitProbe {
        samples = List.copyOf(samples);
    }
}
