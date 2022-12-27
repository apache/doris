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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AesDecrypt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AesEncrypt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sm4Decrypt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sm4Encrypt;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;

/**
 * Used to translate function of new optimizer to stale expr.
 */
public class FunctionTranslatorUtil {
    private static final HashSet<String> aesModes = Sets.newHashSet(
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
            "AES_256_OFB");

    private static final HashSet<String> sm4Modes = Sets.newHashSet(
            "SM4_128_ECB",
            "SM4_128_CBC",
            "SM4_128_CFB128",
            "SM4_128_OFB",
            "SM4_128_CTR");

    private static boolean isAesFunction(ScalarFunction function) {
        return function instanceof AesEncrypt || function instanceof AesDecrypt;
    }

    private static boolean isSm4Function(ScalarFunction function) {
        return function instanceof Sm4Encrypt || function instanceof Sm4Decrypt;
    }

    public static boolean isEncryptFunction(ScalarFunction function) {
        return isAesFunction(function) || isSm4Function(function);
    }

    /**
    * Used to translate encryption function with encryption mode info.
    */
    public static void translateEncryptionFunction(ScalarFunction function, List<Expr> arguments,
            List<Type> argTypes) {
        String blockEncryptionMode = ConnectContext.get().getSessionVariable().getBlockEncryptionMode();
        if (isAesFunction(function)) {
            if (StringUtils.isAllBlank(blockEncryptionMode)) {
                blockEncryptionMode = "AES_128_ECB";
            }
            if (!aesModes.contains(blockEncryptionMode.toUpperCase())) {
                throw new AnalysisException("session variable block_encryption_mode is invalid with aes");
            }
        } else if (isSm4Function(function)) {
            if (StringUtils.isAllBlank(blockEncryptionMode)) {
                blockEncryptionMode = "SM4_128_ECB";
            }
            if (!sm4Modes.contains(blockEncryptionMode.toUpperCase())) {
                throw new AnalysisException("session variable block_encryption_mode is invalid with sm4");
            }
        }
        arguments.add(new StringLiteral(blockEncryptionMode));
        argTypes.add(function.expectedInputTypes().get(0).toCatalogDataType());
    }
}
