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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.literal.Literal;

/**
 * Functions whose legality checks require some constant arguments to be literals.
 * The analyzer folds these arguments on FE before checkLegalityBeforeTypeCoercion,
 * so a constant expression such as 1 + 1 is accepted like the literal it folds to.
 */
public interface FoldLiteralArguments {
    /** whether the argument at the given index must be folded to a literal before the legality checks */
    boolean needFoldToLiteral(int index);

    /** whether the literal the argument at the given index folds to replaces the argument */
    default boolean acceptFoldedLiteral(int index, Literal folded) {
        return true;
    }
}
