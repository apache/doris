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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;

import java.util.Optional;

/**
 * column definition
 */
public class ColumnDefinition {
    private final String name;
    private final DataType type;
    private final boolean isKey;
    private final String aggType;
    private final boolean isNull;
    private final Optional<Expression> defaultValue;
    private final String comment;

    /**
     * constructor
     */
    public ColumnDefinition(String name, DataType type, boolean isKey, String aggType, boolean isNull,
            Optional<Expression> defaultValue, String comment) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.aggType = aggType;
        this.isNull = isNull;
        this.defaultValue = defaultValue;
        this.comment = comment;
    }

    public ColumnDef translateToCatalogStyle() {
        return new ColumnDef(name, new TypeDef(type.toCatalogDataType()), isKey, AggregateType.MAX, isNull,
                false, null, comment, true);
    }
}
