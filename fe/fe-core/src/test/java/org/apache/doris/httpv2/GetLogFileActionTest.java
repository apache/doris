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

package org.apache.doris.httpv2;

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.rest.GetLogFileAction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class GetLogFileActionTest {

    @TempDir
    public File tempDir;

    @BeforeAll
    public static void before() {
        File tempDir = new File("test/audit.log");
        tempDir.mkdir();
        Config.audit_log_dir = tempDir.getAbsolutePath();
    }

    @Test
    public void testCheckAuditLogFileName() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        //private method checkAuditLogFileName
        GetLogFileAction action = new GetLogFileAction();
        Method method = GetLogFileAction.class.getDeclaredMethod("checkAuditLogFileName", String.class);
        method.setAccessible(true);
        method.invoke(action, "audit.log");
        method.invoke(action, "fe.audit.log.20241104-1");
        Assertions.assertThrows(InvocationTargetException.class, () -> method.invoke(action, "../etc/passwd"));
        Assertions.assertThrows(InvocationTargetException.class, () -> method.invoke(action,
                "fe.audit.log.20241104-1/../../etc/passwd"));
        Assertions.assertThrows(InvocationTargetException.class,
                () -> method.invoke(action, "fe.audit.log.20241104-1; rm -rf /"));


    }
}
