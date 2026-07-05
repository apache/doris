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

package org.apache.doris.datasource;

import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;

/**
 * Converts the fe-core/nereids partition-field op types ({@link AddPartitionFieldOp} etc.) to the neutral
 * connector SPI carrier ({@link PartitionFieldChange}).
 *
 * <p>Lives in fe-core because it bridges the nereids ALTER op types and the SPI DTO. The mapping is a plain
 * field copy: add/drop populate only the "new/primary" field; replace also fills the {@code old*} side. The
 * iceberg transform interpretation ({@code Term}/{@code UpdatePartitionSpec}) lives entirely in the connector.</p>
 */
public final class ConnectorPartitionFieldConverter {

    private ConnectorPartitionFieldConverter() {
    }

    public static PartitionFieldChange toAddChange(AddPartitionFieldOp op) {
        return new PartitionFieldChange(
                op.getTransformName(), op.getTransformArg(), op.getColumnName(), op.getPartitionFieldName(),
                null, null, null, null);
    }

    public static PartitionFieldChange toDropChange(DropPartitionFieldOp op) {
        return new PartitionFieldChange(
                op.getTransformName(), op.getTransformArg(), op.getColumnName(), op.getPartitionFieldName(),
                null, null, null, null);
    }

    /** Maps the op's {@code new*} side to the primary field and its {@code old*} side to the old field. */
    public static PartitionFieldChange toReplaceChange(ReplacePartitionFieldOp op) {
        return new PartitionFieldChange(
                op.getNewTransformName(), op.getNewTransformArg(), op.getNewColumnName(),
                op.getNewPartitionFieldName(),
                op.getOldPartitionFieldName(), op.getOldTransformName(), op.getOldTransformArg(),
                op.getOldColumnName());
    }
}
