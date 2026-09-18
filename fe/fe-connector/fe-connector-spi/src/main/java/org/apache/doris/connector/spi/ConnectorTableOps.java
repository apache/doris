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

package org.apache.doris.connector.spi;

/**
 * Operations on tables within a connector catalog: the aggregate of the per-domain table interfaces.
 *
 * <p>This interface declares nothing itself. It exists so that {@link ConnectorMetadata} keeps one
 * table-operations supertype and connectors keep compiling unchanged, while the operations themselves live in
 * the domain each belongs to:</p>
 *
 * <ul>
 * <li>{@link ConnectorTableMetadataOps} &mdash; resolving a table by name and reading what it looks like. The
 *     one domain no connector can skip.</li>
 * <li>{@link ConnectorViewOps} &mdash; views.</li>
 * <li>{@link ConnectorTableDdlOps} &mdash; create / drop / rename / truncate.</li>
 * <li>{@link ConnectorColumnEvolutionOps} &mdash; column schema evolution, flat and nested.</li>
 * <li>{@link ConnectorSnapshotRefOps} &mdash; branches, tags and partition-spec evolution.</li>
 * <li>{@link ConnectorPartitionListingOps} &mdash; enumerating partitions.</li>
 * </ul>
 *
 * <p>Passing a SQL string through to the remote source is deliberately NOT here: it is the escape hatch of a
 * connector whose source speaks SQL, not a table operation, and it lives in the optional
 * {@link ConnectorPassthroughSqlOps}, which a connector implements or does not.</p>
 *
 * <p><b>Start with each domain's class javadoc: it states that domain's minimum implementation set</b>
 * &mdash; which methods a connector must override to work at all, which become mandatory once a given
 * capability is declared, and which are optional. Every method has a default body, so the compiler demands
 * nothing; those lists are the only statement of what is actually required.</p>
 */
public interface ConnectorTableOps extends
        ConnectorTableMetadataOps,
        ConnectorViewOps,
        ConnectorTableDdlOps,
        ConnectorColumnEvolutionOps,
        ConnectorSnapshotRefOps,
        ConnectorPartitionListingOps {
}
