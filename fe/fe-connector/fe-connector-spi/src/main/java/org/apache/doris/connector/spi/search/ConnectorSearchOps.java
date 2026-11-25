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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

/**
 * Serving the engine's search table functions ({@link ConnectorSearchFunction}) over a catalog's tables, such as
 * {@code vector_search("table" = "ctl.db.tbl", ...)}.
 *
 * <p><b>Optional: implement this interface on the catalog's metadata, or do not.</b> Implementing it is the
 * declaration; there is no capability flag, as for {@code ConnectorPassthroughSqlOps}. The engine routes a search
 * by the catalog its {@code table} argument names, checks the user's SELECT privilege on that table, and hands the
 * other arguments to {@link #prepareSearch}. It then requires SELECT on every table column
 * {@link ConnectorSearch#getColumns()} names and refuses the search while a row policy or a data mask applies to the
 * user on the table, since it can apply neither to a search. It scans {@link ConnectorSearch#getScanHandle()} as it
 * scans a table, merges the ranges' candidates with a TopN over the function's result column, and applies the WHERE
 * written on the function to the candidates before the merge; any other filter applies after it. Everything the
 * search itself reads is the connector's: the arguments, the snapshot, the indexes and the ranges.</p>
 */
public interface ConnectorSearchOps {

    /**
     * Prepares one search over {@code table} while the engine analyzes the statement: validates the arguments and
     * pins what the search reads. Called once per table function and statement, and once more for every execution
     * of a prepared statement.
     *
     * @throws org.apache.doris.connector.spi.DorisConnectorException with a message for the user when an argument
     *         is missing, unknown or invalid, when a deferred argument must be a constant, or when the table
     *         cannot serve this function
     */
    ConnectorSearch prepareSearch(ConnectorSession session, ConnectorTableHandle table,
            ConnectorSearchRequest request);
}
