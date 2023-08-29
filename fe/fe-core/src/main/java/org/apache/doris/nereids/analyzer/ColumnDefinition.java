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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ColumnDef.java
// and modified by Doris

package org.apache.doris.nereids.analyzer;

import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.types.DataType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ColumnDefinition {
    private static final Logger LOG = LogManager.getLogger(ColumnDefinition.class);

    // parameter initialized in constructor
    private String name;
    private DataType dataType;
    private AggregateType aggregateType;
    private String genericAggregationName;
    private List<TypeDef> genericAggregationArguments;

    private boolean isKey;
    private boolean isAllowNull;
    private boolean isAutoInc;
    private DefaultValue defaultValue;
    private String comment;
    private boolean visible;


    public ColumnDefinition(String name, DataType dataType, boolean isKey, AggregateType aggregateType,
            boolean isAllowNull, boolean isAutoInc, DefaultValue defaultValue, String comment) {
        this(name, dataType, isKey, aggregateType, isAllowNull, isAutoInc, defaultValue, comment, true);
    }

    public ColumnDefinition(String name, DataType dataType, boolean isKey, AggregateType aggregateType,
            boolean isAllowNull, boolean isAutoInc, DefaultValue defaultValue, String comment, boolean visible) {
        this.name = name;
        this.dataType = dataType;
        this.isKey = isKey;
        this.aggregateType = aggregateType;
        this.isAllowNull = isAllowNull;
        this.isAutoInc = isAutoInc;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.visible = visible;
    }



    public boolean isAllowNull() {
        return isAllowNull;
    }

    public String getDefaultValue() {
        return defaultValue.value;
    }

    public String getName() {
        return name;
    }

    public AggregateType getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    public void setGenericAggregationName(String genericAggregationName) {
        this.genericAggregationName = genericAggregationName;
    }

    public void setGenericAggregationName(AggregateType aggregateType) {
        this.genericAggregationName = aggregateType.name().toLowerCase();
    }

    public void setGenericAggregationArguments(List<TypeDef> genericAggregationArguments) {
        this.genericAggregationArguments = genericAggregationArguments;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public String getComment() {
        return comment;
    }

    public boolean isVisible() {
        return visible;
    }

    public void analyze(boolean isOlap) throws AnalysisException {

    }

    public void setAllowNull(boolean allowNull) {
        isAllowNull = allowNull;
    }
}
