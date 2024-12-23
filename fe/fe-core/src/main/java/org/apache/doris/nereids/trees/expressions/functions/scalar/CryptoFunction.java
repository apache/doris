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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/** CryptoFunction */
public abstract class CryptoFunction extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral {

    public CryptoFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    public CryptoFunction(String name, List<Expression> arguments) {
        super(name, arguments);
    }

    @Override
    public String computeToSql() {
        List<String> args = Lists.newArrayList();
        for (int i = 0; i < arity(); i++) {
            if (i == 1) {
                args.add("\'***\'");
            } else {
                args.add(getArgument(i).toSql());
            }
        }
        return getName() + "(" + StringUtils.join(args, ", ") + ")";
    }

    @Override
    public String toString() {
        List<String> args = Lists.newArrayList();
        for (int i = 0; i < arity(); i++) {
            if (i == 1) {
                args.add("\'***\'");
            } else {
                args.add(getArgument(i).toString());
            }
        }
        return getName() + "(" + StringUtils.join(args, ", ") + ")";
    }

    /** getBlockEncryptionMode */
    static StringLiteral getDefaultBlockEncryptionMode(String defaultMode) {
        String blockEncryptionMode = "";
        if (ConnectContext.get() != null) {
            blockEncryptionMode = ConnectContext.get().getSessionVariable().getBlockEncryptionMode();
        }
        if (StringUtils.isAllBlank(blockEncryptionMode)) {
            blockEncryptionMode = defaultMode;
        }
        return new StringLiteral(blockEncryptionMode);
    }
}
