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

package org.apache.doris.optimizer.base;

import org.apache.doris.optimizer.OptExpression;

import java.util.List;

public class QueryContext {
    private OptExpression expression;
    private RequiredPhysicalProperty reqdProp;
    private List<OptColumnRef> outputColumns;
    private SearchVariable variables;

    public QueryContext(OptExpression expression,
                        RequiredPhysicalProperty reqdProp,
                        List<OptColumnRef> outputColumns,
                        SearchVariable variables) {
        this.expression = expression;
        this.reqdProp = reqdProp;
        this.outputColumns = outputColumns;
        this.variables = variables;
    }

    public OptColumnRefSet getColumnRefs() {
        OptColumnRefSet columnRefs = new OptColumnRefSet();
        columnRefs.include(outputColumns);
        columnRefs.include(reqdProp.getOrderProperty().getPropertySpec().getUsedColumns());
        return columnRefs;
    }

    public OptExpression getExpression() { return expression; }
    public RequiredPhysicalProperty getReqdProp() { return reqdProp; }
    public List<OptColumnRef> getOutputColumns() { return outputColumns; }
    public SearchVariable getVariables() { return variables; }
}
