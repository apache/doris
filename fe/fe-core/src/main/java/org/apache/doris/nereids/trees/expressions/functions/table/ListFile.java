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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.tablefunction.ListFileTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.base.Preconditions;

import java.util.List;

/** list_file */
public class ListFile extends TableValuedFunction {
    public ListFile(Properties properties) {
        super(ListFileTableValuedFunction.NAME, properties);
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(FileType.INSTANCE, getArgumentsTypes());
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            return new ListFileTableValuedFunction(getTVFProperties().getMap());
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException("Can not build list_file(): " + e.getMessage(), e);
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitListFile(this, context);
    }

    @Override
    public ListFile withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 && children.get(0) instanceof Properties);
        return new ListFile((Properties) children.get(0));
    }
}
