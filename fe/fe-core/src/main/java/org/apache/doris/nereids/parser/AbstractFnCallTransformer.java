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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;

import java.util.List;

/**
 * Abstract function transformer, dialect function transformer should extend this.
 */
public abstract class AbstractFnCallTransformer {

    /**
     * Check source function signature is the same between function from SQL and
     * definition in function call transformer.
     * Check the targetArgs param matches the definition in function call transformer.
     */
    protected abstract boolean check(String sourceFnName,
                                     List<Expression> sourceFnTransformedArguments,
                                     ParserContext context);

    /**
     * After check, do transform for function mapping.
     */
    protected abstract Function transform(String sourceFnName,
                                          List<Expression> sourceFnTransformedArguments,
                                          ParserContext context);
}
