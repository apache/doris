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

package org.apache.doris.connector.api.write;

/**
 * One field of a connector's write-time partitioning, in an engine-neutral form.
 *
 * <p>A write-capable connector returns these from {@link ConnectorWritePlanProvider#getWritePartitioning}
 * so the engine can build its merge-write distribution (the iceberg {@code DistributionSpecMerge}) without
 * importing the connector's native partition-spec types. The engine resolves {@link #getSourceColumnName()}
 * to a bound output expr id locally and constructs the distribution field from
 * {@code (transform, exprId, transformParam, fieldName, sourceId)}.</p>
 *
 * <p>The fields map 1:1 onto the legacy iceberg partition walk
 * ({@code PhysicalIcebergMergeSink.buildInsertPartitionFields}): {@code transform} is the iceberg
 * {@code PartitionField.transform().toString()} (e.g. {@code "identity"}, {@code "bucket[16]"},
 * {@code "day"}); {@code transformParam} is its parsed bracket argument ({@code 16} for {@code bucket[16]},
 * {@code null} when absent); {@code sourceColumnName} is the base column name the field is derived from
 * (resolved from {@code sourceId} against the schema); {@code fieldName} is the partition field's own name
 * (e.g. {@code "id_bucket"}); {@code sourceId} is the iceberg source field id.</p>
 */
public final class ConnectorWritePartitionField {

    private final String transform;
    private final Integer transformParam;
    private final String sourceColumnName;
    private final String fieldName;
    private final int sourceId;

    public ConnectorWritePartitionField(String transform, Integer transformParam,
            String sourceColumnName, String fieldName, int sourceId) {
        this.transform = transform;
        this.transformParam = transformParam;
        this.sourceColumnName = sourceColumnName;
        this.fieldName = fieldName;
        this.sourceId = sourceId;
    }

    /** The transform name, verbatim from the native spec (e.g. {@code "identity"}, {@code "bucket[16]"}). */
    public String getTransform() {
        return transform;
    }

    /** The parsed bracket argument of the transform ({@code 16} for {@code bucket[16]}), or {@code null}. */
    public Integer getTransformParam() {
        return transformParam;
    }

    /** The base (source) column name the partition field is derived from; may be {@code null} if unresolvable. */
    public String getSourceColumnName() {
        return sourceColumnName;
    }

    /** The partition field's own name (e.g. {@code "id_bucket"}). */
    public String getFieldName() {
        return fieldName;
    }

    /** The iceberg source field id of the base column. */
    public int getSourceId() {
        return sourceId;
    }
}
