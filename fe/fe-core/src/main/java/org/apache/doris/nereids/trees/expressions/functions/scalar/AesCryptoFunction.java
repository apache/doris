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
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/** AesCryptoFunction */
public abstract class AesCryptoFunction extends CryptoFunction {
    public static final Set<String> AES_MODES = ImmutableSet.of(
            "AES_128_ECB",
            "AES_192_ECB",
            "AES_256_ECB",
            "AES_128_CBC",
            "AES_192_CBC",
            "AES_256_CBC",
            "AES_128_CFB",
            "AES_192_CFB",
            "AES_256_CFB",
            "AES_128_CFB1",
            "AES_192_CFB1",
            "AES_256_CFB1",
            "AES_128_CFB8",
            "AES_192_CFB8",
            "AES_256_CFB8",
            "AES_128_CFB128",
            "AES_192_CFB128",
            "AES_256_CFB128",
            "AES_128_CTR",
            "AES_192_CTR",
            "AES_256_CTR",
            "AES_128_OFB",
            "AES_192_OFB",
            "AES_256_OFB",
            "AES_128_GCM",
            "AES_192_GCM",
            "AES_256_GCM"
    );

    public static final Set<String> AES_GCM_MODES = ImmutableSet.of(
            "AES_128_GCM",
            "AES_192_GCM",
            "AES_256_GCM"
    );

    public AesCryptoFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    public AesCryptoFunction(String name, List<Expression> arguments) {
        super(name, arguments);
    }

    /** getDefaultBlockEncryptionMode */
    public static StringLiteral getDefaultBlockEncryptionMode() {
        StringLiteral encryptionMode = CryptoFunction.getDefaultBlockEncryptionMode("AES_128_ECB");
        if (!AES_MODES.contains(encryptionMode.getValue())) {
            throw new AnalysisException(
                    "session variable block_encryption_mode is invalid with aes");
        }
        return encryptionMode;
    }

    @Override
    public void checkLegalityAfterRewrite() {
        if (arity() >= 4 && child(3) instanceof StringLikeLiteral) {
            String mode = ((StringLikeLiteral) child(3)).getValue().toUpperCase();
            if (!AES_MODES.contains(mode)) {
                throw new AnalysisException("mode " + mode + " is not supported");
            }
            if (arity() == 5 && !AES_GCM_MODES.contains(mode)) {
                throw new AnalysisException("only GCM mode support AAD(the 5th arg)");
            }
        }
    }
}
