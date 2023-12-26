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

package org.apache.doris.catalog.constraint;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class UniqueConstraint extends Constraint {
    private final ImmutableSet<String> columns;

    public UniqueConstraint(String name, Set<String> columns) {
        super(ConstraintType.UNIQUE, name);
        this.columns = ImmutableSet.copyOf(columns);
    }

    public ImmutableSet<String> getUniqueColumnNames() {
        return columns;
    }

    public ImmutableSet<Column> getUniqueKeys(TableIf table) {
        return columns.stream().map(table::getColumn).collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UniqueConstraint that = (UniqueConstraint) o;
        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columns);
    }
}
