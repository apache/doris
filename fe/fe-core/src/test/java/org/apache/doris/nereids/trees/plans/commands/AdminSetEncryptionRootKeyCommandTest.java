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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.encryption.RootKeyInfo;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Locale;

public class AdminSetEncryptionRootKeyCommandTest {
    @Test
    public void testValidateAliyunKmsUnderTurkishLocale() throws Exception {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            AdminSetEncryptionRootKeyCommand command = new AdminSetEncryptionRootKeyCommand(ImmutableMap.of(
                    AdminSetEncryptionRootKeyCommand.PROPERTIES_TYPE, "aliyun_kms",
                    AdminSetEncryptionRootKeyCommand.PROPERTIES_ENCRYPTION_ALGORITHM, "aes256",
                    AdminSetEncryptionRootKeyCommand.PROPERTIES_REGION, "cn-hangzhou",
                    AdminSetEncryptionRootKeyCommand.PROPERTIES_CMK_ID, "test-cmk"));

            command.validate();

            Assertions.assertEquals(RootKeyInfo.RootKeyType.ALIYUN_KMS, getRootKeyInfo(command).type);
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    private RootKeyInfo getRootKeyInfo(AdminSetEncryptionRootKeyCommand command) throws Exception {
        Field field = AdminSetEncryptionRootKeyCommand.class.getDeclaredField("rootKeyInfo");
        field.setAccessible(true);
        return (RootKeyInfo) field.get(command);
    }
}
