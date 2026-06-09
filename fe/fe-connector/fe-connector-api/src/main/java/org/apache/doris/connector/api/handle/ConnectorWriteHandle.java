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

package org.apache.doris.connector.api.handle;

import org.apache.doris.connector.api.ConnectorColumn;

import java.util.List;
import java.util.Map;

/**
 * A bound write request passed to
 * {@link org.apache.doris.connector.api.write.ConnectorWritePlanProvider#planWrite}.
 *
 * <p>Carries the engine-resolved facts about a single DML write: the target
 * table handle, the column list, whether it is an OVERWRITE, and a free-form
 * write context (static partition spec, write path, etc.). The connector reads
 * these to build its Thrift data sink.</p>
 */
public interface ConnectorWriteHandle {

    /** The target table handle (the connector's own opaque table handle). */
    ConnectorTableHandle getTableHandle();

    /** The columns being written, ordered to match the INSERT column list. */
    List<ConnectorColumn> getColumns();

    /** Whether this is an INSERT OVERWRITE. */
    boolean isOverwrite();

    /**
     * Free-form write context: static partition spec, write path, and other
     * connector-defined keys carried from the bound sink to {@code planWrite}.
     */
    Map<String, String> getWriteContext();
}
