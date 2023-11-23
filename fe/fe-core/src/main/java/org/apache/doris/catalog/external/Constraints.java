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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;

import com.google.common.collect.ImmutableSet;

public class Constraints {
    private final ImmutableSet<ImmutableSet<Column>> uniqueKeys;
    private final ImmutableSet<ImmutableSet<Column>> primaryKeys;
    private final ImmutableSet<ForeignKey> foreignKeys;

    private Constraints(ImmutableSet<ImmutableSet<Column>> uniqueKeys,
            ImmutableSet<ImmutableSet<Column>> primaryKeys, ImmutableSet<ForeignKey> foreignKeys) {
        this.foreignKeys = foreignKeys;
        this.primaryKeys = primaryKeys;
        this.uniqueKeys = uniqueKeys;
    }

    public static class Builder {
        private final ImmutableSet.Builder<ImmutableSet<Column>> uniqueKeysBuilder = ImmutableSet.builder();
        private final ImmutableSet.Builder<ImmutableSet<Column>> primaryKeysBuilder = ImmutableSet.builder();
        private final ImmutableSet.Builder<ForeignKey> foreignKeysBuilder = ImmutableSet.builder();

        public void addUniqueKeys(ImmutableSet<Column> uniqueKeys) {
            uniqueKeysBuilder.add(uniqueKeys);
        }

        public void addPrimaryKeys(ImmutableSet<Column> primaryKeys) {
            primaryKeysBuilder.add(primaryKeys);
        }

        public void addForeignKeys(ImmutableSet<Column> foreignKeys,
                OlapTable referencedTable, ImmutableSet<Column> referencedKeys) {
            foreignKeysBuilder.add(new ForeignKey(foreignKeys, referencedTable, referencedKeys));
        }
    }

    private static class ForeignKey {
        final ImmutableSet<Column> foreignKeys;
        final OlapTable referenceTable;
        final ImmutableSet<Column> referenceKeys;

        ForeignKey(ImmutableSet<Column> foreignKeys, OlapTable referenceTable, ImmutableSet<Column> referenceKeys) {
            this.foreignKeys = foreignKeys;
            this.referenceTable = referenceTable;
            this.referenceKeys = referenceKeys;
        }
    }
}
