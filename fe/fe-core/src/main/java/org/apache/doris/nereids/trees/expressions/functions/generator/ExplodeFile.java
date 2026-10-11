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

package org.apache.doris.nereids.trees.expressions.functions.generator;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.types.FileType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Expand FILE to its six public nullable fields. */
public class ExplodeFile extends TableGeneratingFunction implements AlwaysNullable {
    public ExplodeFile(Expression argument) {
        this("explode_file", argument);
    }

    protected ExplodeFile(String name, Expression argument) {
        super(name, argument);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!getArgument(0).getDataType().isFileType() && !getArgument(0).getDataType().isNullType()) {
            throw new AnalysisException(getName() + " only accepts FILE");
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(FunctionSignature.ret(FileType.INSTANCE.publicStructType()).args(FileType.INSTANCE));
    }

    @Override
    public ExplodeFile withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new ExplodeFile(children.get(0));
    }
}
