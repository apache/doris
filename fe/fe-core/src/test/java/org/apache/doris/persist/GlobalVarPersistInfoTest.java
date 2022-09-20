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

package org.apache.doris.persist;

import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class GlobalVarPersistInfoTest {
    private static final String fileName = "./GlobalVarPersistInfoTest";

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(fileName));
    }

    @Test
    public void testSerializeAlterViewInfo() throws IOException {
        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get("./BatchRemoveTransactionOperationTest"));
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        List<String> varNames = Lists.newArrayList();
        varNames.add("exec_mem_limit");
        varNames.add("default_rowset_type");
        GlobalVarPersistInfo info = new GlobalVarPersistInfo(sessionVariable, varNames);

        info.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        GlobalVarPersistInfo newInfo = GlobalVarPersistInfo.read(in);
        System.out.println(newInfo.getPersistJsonString());
        in.close();
    }
}
