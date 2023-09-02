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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/** Sm4CryptoFunction */
public abstract class Sm4CryptoFunction extends CryptoFunction {
    public static final Set<String> SM4_MODES = ImmutableSet.of(
            "SM4_128_ECB",
            "SM4_128_CBC",
            "SM4_128_CFB128",
            "SM4_128_OFB",
            "SM4_128_CTR"
    );

    public Sm4CryptoFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    public Sm4CryptoFunction(String name, List<Expression> arguments) {
        super(name, arguments);
    }

    /** getDefaultBlockEncryptionMode */
    static StringLiteral getDefaultBlockEncryptionMode() {
        StringLiteral encryptionMode = CryptoFunction.getDefaultBlockEncryptionMode("SM4_128_ECB");
        if (!SM4_MODES.contains(encryptionMode.getValue())) {
            throw new AnalysisException(
                    "session variable block_encryption_mode is invalid with sm4");
        }
        return encryptionMode;
    }
}
