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

package org.apache.doris.connector.api.ddl;

/**
 * Neutral carrier for a partition-field evolution request ({@code ALTER TABLE ... ADD/DROP/REPLACE PARTITION
 * KEY}), decoupling the connector SPI from the fe-core/nereids {@code AddPartitionFieldOp}/
 * {@code DropPartitionFieldOp}/{@code ReplacePartitionFieldOp} types.
 *
 * <p>A "partition field" is a column reference run through an optional transform. The four leading fields describe
 * ONE such field; how the connector reads them depends on the SPI method it is passed to:</p>
 * <ul>
 *   <li>{@code addPartitionField} — the field to ADD: {@code transformName}/{@code transformArg}/
 *       {@code columnName} build the transform, {@code partitionFieldName} is the optional {@code AS} alias.</li>
 *   <li>{@code dropPartitionField} — the field to REMOVE: when {@code partitionFieldName} is set it names the
 *       field directly; otherwise the transform triple identifies it.</li>
 *   <li>{@code replacePartitionField} — the NEW field (same reading as add); the {@code old*} fields below
 *       identify the OLD field to remove first (by {@code oldPartitionFieldName}, else by old transform triple).</li>
 * </ul>
 *
 * <p>A transform is identity when {@code transformName} is {@code null} (a bare column ref); {@code transformArg}
 * carries the width for {@code bucket(n)}/{@code truncate(n)} and is {@code null} otherwise. The {@code old*}
 * fields are only populated for {@code replacePartitionField}; they are {@code null} for add/drop.</p>
 */
public final class PartitionFieldChange {

    // ---- The field to add / drop, or the NEW field of a replace ----
    private final String transformName;
    private final Integer transformArg;
    private final String columnName;
    private final String partitionFieldName;

    // ---- The OLD field of a replace (null for add / drop) ----
    private final String oldPartitionFieldName;
    private final String oldTransformName;
    private final Integer oldTransformArg;
    private final String oldColumnName;

    public PartitionFieldChange(String transformName, Integer transformArg, String columnName,
            String partitionFieldName, String oldPartitionFieldName, String oldTransformName,
            Integer oldTransformArg, String oldColumnName) {
        this.transformName = transformName;
        this.transformArg = transformArg;
        this.columnName = columnName;
        this.partitionFieldName = partitionFieldName;
        this.oldPartitionFieldName = oldPartitionFieldName;
        this.oldTransformName = oldTransformName;
        this.oldTransformArg = oldTransformArg;
        this.oldColumnName = oldColumnName;
    }

    /** Transform name (e.g. {@code bucket}/{@code truncate}/{@code year}); {@code null} = identity. */
    public String getTransformName() {
        return transformName;
    }

    /** Width for {@code bucket(n)}/{@code truncate(n)}; {@code null} otherwise. */
    public Integer getTransformArg() {
        return transformArg;
    }

    /** Source column the transform is applied to. */
    public String getColumnName() {
        return columnName;
    }

    /** Optional partition-field name: the {@code AS} alias (add/replace) or the field to remove (drop). */
    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    /** Replace only: the existing partition field to remove by name; {@code null} = remove by old transform. */
    public String getOldPartitionFieldName() {
        return oldPartitionFieldName;
    }

    /** Replace only: old transform name; {@code null} = identity. */
    public String getOldTransformName() {
        return oldTransformName;
    }

    /** Replace only: old transform width. */
    public Integer getOldTransformArg() {
        return oldTransformArg;
    }

    /** Replace only: old source column. */
    public String getOldColumnName() {
        return oldColumnName;
    }
}
