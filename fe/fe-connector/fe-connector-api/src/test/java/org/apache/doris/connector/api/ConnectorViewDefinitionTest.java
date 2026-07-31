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

package org.apache.doris.connector.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Value-semantics tests for {@link ConnectorViewDefinition}: the neutral {sql, dialect, columns} carrier the
 * connector returns for a flipped external view. The sql + dialect are required (a view always has a body and a
 * dialect the body is written in); the columns carry the view's schema (DESC / SHOW COLUMNS /
 * information_schema.columns, the H8 fix). Value equality keys on all three.
 */
public class ConnectorViewDefinitionTest {

    private static ConnectorColumn col(String name) {
        return new ConnectorColumn(name, ConnectorType.of("INT"), "", true, null, true);
    }

    @Test
    public void exposesSqlDialectAndColumns() {
        List<ConnectorColumn> columns = Arrays.asList(col("a"), col("b"));
        ConnectorViewDefinition def = new ConnectorViewDefinition("SELECT 1", "spark", columns);
        Assertions.assertEquals("SELECT 1", def.getSql());
        Assertions.assertEquals("spark", def.getDialect());
        // WHY (H8): the view's columns must survive the carrier so initSchema can build the view schema from
        // them. MUTATION: dropping the columns field / not returning them -> DESC of a flipped view is empty.
        Assertions.assertEquals(columns, def.getColumns());
    }

    @Test
    public void getColumnsIsDefensiveAndUnmodifiable() {
        // WHY: the carrier is shared across the schema cache; a leaked mutable list (or aliasing the caller's
        // list) would let a later mutation corrupt a cached view schema. MUTATION: returning the live list /
        // skipping the defensive copy -> one of these assertions goes red.
        List<ConnectorColumn> source = new ArrayList<>();
        source.add(col("a"));
        ConnectorViewDefinition def = new ConnectorViewDefinition("SELECT 1", "spark", source);

        // Defensive copy: mutating the source after construction must NOT change the carrier's columns.
        source.add(col("late"));
        Assertions.assertEquals(1, def.getColumns().size(),
                "the carrier must copy the columns defensively, not alias the caller's list");

        // Unmodifiable view: callers cannot mutate the returned list.
        Assertions.assertThrows(UnsupportedOperationException.class, () -> def.getColumns().add(col("x")));
    }

    @Test
    public void nullColumnsBecomesEmptyList() {
        // WHY: a null columns argument is normalized to an empty (never-null) list so callers (initSchema)
        // never NPE on getColumns(). MUTATION: storing null -> getColumns() NPEs downstream -> red.
        ConnectorViewDefinition def = new ConnectorViewDefinition("SELECT 1", "spark", null);
        Assertions.assertTrue(def.getColumns().isEmpty());
    }

    @Test
    public void equalsAndHashCodeKeyOnAllThreeFields() {
        List<ConnectorColumn> columns = Collections.singletonList(col("a"));
        ConnectorViewDefinition base = new ConnectorViewDefinition("SELECT 1", "spark", columns);
        Assertions.assertEquals(base, new ConnectorViewDefinition("SELECT 1", "spark",
                Collections.singletonList(col("a"))));
        Assertions.assertEquals(base.hashCode(), new ConnectorViewDefinition("SELECT 1", "spark",
                Collections.singletonList(col("a"))).hashCode());
        // MUTATION: an equals/hashCode that ignores sql, dialect, OR columns would collapse distinct
        // definitions -> one of these goes red.
        Assertions.assertNotEquals(base, new ConnectorViewDefinition("SELECT 2", "spark", columns));
        Assertions.assertNotEquals(base, new ConnectorViewDefinition("SELECT 1", "trino", columns));
        Assertions.assertNotEquals(base, new ConnectorViewDefinition("SELECT 1", "spark",
                Collections.singletonList(col("b"))));
    }

    @Test
    public void rejectsNullSqlAndDialect() {
        // WHY: a view definition with a null body or null dialect is a programming error, not a valid state;
        // fail fast at construction. MUTATION: dropping requireNonNull -> a null leaks downstream -> red.
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorViewDefinition(null, "spark", Collections.emptyList()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorViewDefinition("SELECT 1", null, Collections.emptyList()));
    }
}
