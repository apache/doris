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

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Preconditions;

import java.util.List;

/** Preserve one all-NULL output row for a NULL FILE. */
public final class ExplodeFileOuter extends ExplodeFile {
    public ExplodeFileOuter(Expression argument) {
        super("explode_file_outer", argument);
    }

    @Override
    public ExplodeFileOuter withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new ExplodeFileOuter(children.get(0));
    }
}
