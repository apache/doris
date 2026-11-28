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

package org.apache.doris.connector.spi.search;

/**
 * Whether the engine may read some of a search's columns after it has merged the candidates, only for the rows that
 * survive the merge.
 *
 * <p>With lazy materialization the scan returns, besides the columns the merge and the filters need, a row id for
 * every candidate; after the merge the engine asks the backend that produced each surviving row for the remaining
 * columns of that row. The row ids are the connector's: its backend reader produces them and reads by them.</p>
 */
public enum ConnectorSearchLazyMaterialization {

    /** The scan reads every column the query uses. */
    NONE,

    /**
     * The engine may fetch whole top-level columns by row id. A column that the query reads only through a nested
     * field, by an access path, stays in the scan.
     */
    TOP_LEVEL_COLUMNS
}
