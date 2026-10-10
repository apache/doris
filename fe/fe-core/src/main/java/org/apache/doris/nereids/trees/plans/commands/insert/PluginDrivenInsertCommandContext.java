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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.connector.converter.ConnectorWriteValueConverter;
import org.apache.doris.nereids.rules.analysis.BindSink;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.StringArithmetic;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Insert command context for plugin-driven connector catalogs.
 *
 * <p>{@code overwrite} is inherited from {@link BaseExternalTableInsertCommandContext}.
 * The static partition spec — a generic {@code col -> val} map — is carried here and
 * handed to the connector via the write context of
 * {@code ConnectorWritePlanProvider.planWrite}. It is populated during sink binding
 * (wired at the connector cutover) and defaults to empty, so a write with no static
 * partition contributes nothing to partition pinning.</p>
 *
 * <p>{@code branchName} carries the {@code INSERT INTO t@branch(name)} target. It is threaded onto
 * the connector write handle ({@code ConnectorWriteHandle.getBranchName}) so a versioned-table
 * connector points the commit at the branch; empty (the default) means the table's default ref.</p>
 */
public class PluginDrivenInsertCommandContext extends BaseExternalTableInsertCommandContext {

    private Map<String, String> staticPartitionSpec = Collections.emptyMap();
    private Set<String> staticPartitionNullKeys = Collections.emptySet();
    private Map<String, Literal> staticPartitionLiterals = Collections.emptyMap();
    // The target schema the sink was bound to, whose column types the static partition values are cast to.
    private List<Column> boundTargetSchema = Collections.emptyList();
    private Optional<String> branchName = Optional.empty();

    public Map<String, String> getStaticPartitionSpec() {
        return staticPartitionSpec;
    }

    public Set<String> getStaticPartitionNullKeys() {
        return staticPartitionNullKeys;
    }

    /** Retains literal partition values and distinguishes SQL NULL from the string "NULL". */
    public void setStaticPartitionSpecFromExpressions(Map<String, Expression> partitionValues) {
        setStaticPartitionSpecFromExpressions(partitionValues, Collections.emptyList());
    }

    /** Normalizes static values with the pinned write schema before encoding commit metadata. */
    public void setStaticPartitionSpecFromExpressions(Map<String, Expression> partitionValues, List<Column> schema) {
        Map<String, String> spec = new HashMap<>();
        Set<String> nullKeys = new HashSet<>();
        Map<String, Literal> literals = new LinkedHashMap<>();
        for (Map.Entry<String, Expression> entry : partitionValues.entrySet()) {
            if (entry.getValue() instanceof Literal) {
                Literal literal = (Literal) entry.getValue();
                Column column = schema.stream().filter(c -> c.getName().equalsIgnoreCase(entry.getKey()))
                        .findFirst().orElse(null);
                if (column != null) {
                    // Commit metadata must use the same semantic bytes as the materialized partition row.
                    literal = (Literal) ConnectorWriteValueConverter.convert(column, literal);
                }
                // Binary partition keys must bypass character decoding, including empty and non-UTF-8 bytes.
                spec.put(entry.getKey(), literal instanceof VarBinaryLiteral
                        ? "0x" + literal.toString() : literal.getStringValue());
                literals.put(entry.getKey(), literal);
                if (entry.getValue() instanceof NullLiteral) {
                    nullKeys.add(entry.getKey());
                }
            }
        }
        this.staticPartitionSpec = spec;
        this.staticPartitionNullKeys = nullKeys;
        this.staticPartitionLiterals = literals;
    }

    public void setBoundTargetSchema(List<Column> boundTargetSchema) {
        this.boundTargetSchema = boundTargetSchema;
    }

    /**
     * Casts the static partition values to the types of their columns in the bound target schema; see
     * {@code ConnectorWriteHandle#getCastStaticPartitionSpec}. Each value is the one BindSink materializes into
     * the rows, after constant folding, but a value that does not fit its type fails here instead of becoming
     * NULL.
     */
    public Map<String, String> castStaticPartitionSpec() {
        Preconditions.checkState(staticPartitionLiterals.isEmpty() || !boundTargetSchema.isEmpty(),
                "static partition values are cast before the sink is bound to a target schema");
        Map<String, String> spec = new LinkedHashMap<>();
        for (Map.Entry<String, Literal> entry : staticPartitionLiterals.entrySet()) {
            if (staticPartitionNullKeys.contains(entry.getKey())) {
                continue;
            }
            Literal value = entry.getValue();
            Optional<Column> column = boundTargetSchema.stream()
                    .filter(candidate -> candidate.getName().equalsIgnoreCase(entry.getKey()))
                    .findFirst();
            if (column.isPresent()) {
                value = writtenValue(value, DataType.fromCatalogType(column.get().getType()));
            }
            spec.put(entry.getKey(), value.getStringValue());
        }
        return spec;
    }

    private static Literal writtenValue(Literal value, DataType columnType) {
        if (!value.getDataType().isStringLikeType() || !columnType.isStringLikeType()) {
            Expression cast = value.checkedCastTo(columnType);
            Preconditions.checkState(cast instanceof Literal, "cast of literal %s is not a literal", value);
            return (Literal) cast;
        }
        // BindSink does not cast a string written into a string column. It keeps the value, cut to the length of
        // a CHAR / VARCHAR column in code points when the session truncates strings on insert; a cast would cut
        // UTF-16 units instead, or reject a CHAR value that is too long.
        int length = ((CharacterType) columnType).getLen();
        if (length < 0 || !BindSink.truncatesStringOnInsert()) {
            return value;
        }
        Preconditions.checkState(value instanceof StringLikeLiteral, "string literal %s has no string value", value);
        return (Literal) StringArithmetic.substringVarcharIntInt((StringLikeLiteral) value,
                new IntegerLiteral(1), new IntegerLiteral(length));
    }

    public Optional<String> getBranchName() {
        return branchName;
    }

    public void setBranchName(Optional<String> branchName) {
        this.branchName = branchName == null ? Optional.empty() : branchName;
    }
}
