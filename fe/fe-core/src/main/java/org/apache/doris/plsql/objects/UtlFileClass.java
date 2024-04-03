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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/objects/UtlFileClass.java
// and modified by Doris

package org.apache.doris.plsql.objects;

import org.apache.doris.plsql.File;
import org.apache.doris.plsql.Var;
import org.apache.doris.plsql.objects.MethodParams.Arity;

public class UtlFileClass implements PlClass {
    public static final UtlFileClass INSTANCE = new UtlFileClass();
    private final MethodDictionary<UtlFile> methodDictionary = new MethodDictionary();

    private UtlFileClass() {
        methodDictionary.put("fopen", (self, args) -> {
            MethodParams params = new MethodParams("fopen", args, Arity.min(2));
            String dir = params.stringAt(0);
            String name = params.stringAt(1);
            boolean write = true;
            boolean overwrite = false;
            if (args.size() > 2) {
                String mode = params.stringAt(2);
                if (mode.equalsIgnoreCase("r")) {
                    write = false;
                } else if (mode.equalsIgnoreCase("w")) {
                    write = true;
                    overwrite = true;
                }
            }
            File file = self.fileOpen(dir, name, write, overwrite);
            return new Var(Var.Type.FILE, file);
        });
        methodDictionary.put("get_line", (self, args) -> {
            MethodParams params = new MethodParams("get_line", args, Arity.UNARY);
            return new Var(self.getLine(params.fileAt(0)));
        });
        methodDictionary.put("put_line", (self, args) -> {
            MethodParams params = new MethodParams("put_line", args, Arity.BINARY);
            self.put(params.fileAt(0), params.stringAt(1), true);
            return null;
        });
        methodDictionary.put("put", (self, args) -> {
            MethodParams params = new MethodParams("put", args, Arity.BINARY);
            self.put(params.fileAt(0), params.stringAt(1), false);
            return null;
        });
        methodDictionary.put("fclose", (self, args) -> {
            File file = new MethodParams("fclose", args, Arity.UNARY).fileAt(0);
            self.fileClose(file);
            return null;
        });
    }

    @Override
    public UtlFile newInstance() {
        return new UtlFile(this);
    }

    @Override
    public MethodDictionary methodDictionary() {
        return methodDictionary;
    }
}
