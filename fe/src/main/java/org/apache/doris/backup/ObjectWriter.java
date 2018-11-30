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

package org.apache.doris.backup;

import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ObjectWriter {
    private static final Logger LOG = LogManager.getLogger(ObjectWriter.class);

    public static void writeAsJson(String absolutePath, Object obj) throws IOException {
        File jsonFile = new File(absolutePath);
        deleteAndRecreate(jsonFile);

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(jsonFile));
            Gson gson = new Gson();
            String jsonStr = gson.toJson(obj);
            bw.write(jsonStr);
            bw.flush();
        } catch (IOException e) {
            LOG.warn("create json file failed. " + absolutePath, e);
            throw e;
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    LOG.warn("close buffered writer error", e);
                    throw e;
                }
            }
        }
    }

    public static void write(String absolutePath, List<? extends Writable> writables) throws IOException {
        File file = new File(absolutePath);
        deleteAndRecreate(file);

        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        try {
            int count = writables.size();
            out.writeInt(count);
            for (Writable writable : writables) {
                writable.write(out);
            }
            out.flush();
        } finally {
            out.close();
        }
        
        LOG.info("finish write file: {}", absolutePath);
    }

    public static void writeReadable(String absolutePath, List<String> readables) throws IOException {
        File file = new File(absolutePath);
        deleteAndRecreate(file);

        FileWriter fw = null;
        BufferedWriter bw = null;
        
        try {
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            
            for (String readable : readables) {
                bw.write(readable);
                bw.newLine();
            }
            
            bw.flush();
        } finally {
            bw.close();
            fw.close();
        }
        
        LOG.info("finish write readable file: {}", absolutePath);
    }

    public static CreateTableStmt readCreateTableStmt(String filePath) throws IOException {
        throw new RuntimeException("Don't support CreateTableStmt serialization anymore.");
    }

    public static List<AlterTableStmt> readAlterTableStmt(String filePath) throws IOException {
        List<AlterTableStmt> stmts = null;
        try {
            stmts = read(filePath, AlterTableStmt.class);
        } catch (IOException e) {
            LOG.warn("failed to read AlterTableStmt: " + filePath, e);
            throw e;
        }
        return stmts;
    }

    public static DirSaver readManifest(String filePath) throws IOException {
        List<DirSaver> stmts = null;
        try {
            stmts = read(filePath, DirSaver.class);
        } catch (IOException e) {
            LOG.warn("failed to read manifest: " + filePath, e);
            throw e;
        }
        Preconditions.checkState(stmts.size() == 1);
        return stmts.get(0);
    }

    private static <T extends Writable> List<T> read(String filePath, Class<T> c) throws IOException {
        File file = new File(filePath);
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        try {
            int count = in.readInt();
            List<T> res = Lists.newArrayList();
            for (int i = 0; i < count; i++) {
                T t = c.newInstance();
                t.readFields(in);
                res.add(t);
            }
            return res;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IOException("", e);
        } finally {
            in.close();
        }
    }

    private static void deleteAndRecreate(File file) throws IOException {
        // try delete
        Util.deleteDirectory(file);

        // make parent dir
        File parentFile = file.getParentFile();
        if (parentFile != null && !parentFile.exists()) {
            parentFile.mkdirs();
        }

        // create new file
        if (!file.exists()) {
            file.createNewFile();
        }
    }
}
