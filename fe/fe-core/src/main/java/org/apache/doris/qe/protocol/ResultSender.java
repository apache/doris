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

package org.apache.doris.qe.protocol;

import org.apache.doris.catalog.Type;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.qe.ResultSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The result-encoding half of a connection: how a statement's result reaches the client of this
 * wire protocol.
 *
 * <p>A statement produces its result in one of two ways. Either the frontend materializes it -- a
 * {@code SHOW}, an {@code EXPLAIN}, a query the planner answers on its own -- and hands it over as a
 * {@link ResultSet}; or a backend produces it and the executor relays the column definitions and
 * then the rows, already in wire format, as they arrive. What "reaching the client" means is the
 * protocol's business: writing packets to a MySQL channel, or caching an Arrow batch for the
 * client's later DoGet.
 *
 * <p>Obtained from {@link ProtocolAdapter#resultSender}. Everything else about a statement -- its
 * state, the rows it returned, the audit record -- stays on the session.
 */
public interface ResultSender {

    /**
     * Delivers a result set the frontend materialized.
     *
     * @param resultSet   the rows and their metadata
     * @param fieldInfos  per-column details some protocols carry in the column definitions
     *                    (table and original column names); null when there are none
     * @param binaryRows  encode the rows in the protocol's binary row format, if it has one,
     *                    instead of the text format
     */
    void sendResultSet(ResultSet resultSet, List<FieldInfo> fieldInfos, boolean binaryRows) throws IOException;

    /**
     * Delivers the column definitions of a result stream a backend produces. Only a protocol
     * whose results go through the frontend implements this; one whose client pulls the result
     * from the backend never receives a call.
     */
    void sendFields(List<String> colNames, List<FieldInfo> fieldInfos, List<Type> types) throws IOException;

    /**
     * Delivers one packet of a result stream that is already in this protocol's wire format: a
     * row a backend produced, or a packet the master produced for a forwarded query. Same
     * restriction as {@link #sendFields}.
     */
    void sendRow(ByteBuffer row) throws IOException;

    /**
     * Forgets whatever the previous statement of the same request left unsent, so that a
     * multi-statement request delivers only the last result. Called when a query starts.
     */
    void reset();
}
