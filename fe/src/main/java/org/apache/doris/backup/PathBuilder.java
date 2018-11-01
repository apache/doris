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

import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class PathBuilder implements Writable {
    private static final Logger LOG = LogManager.getLogger(PathBuilder.class);
    
    public static final String CREATE_TABLE_STMT_FILE = "create_table_stmt";
    public static final String ADD_ROLLUP_STMT_FILE = "add_rollup_stmt";
    public static final String ADD_PARTITION_STMT_FILE = "add_partition_stmt";

    public static final String MANIFEST_NAME = "manifest";
    public static final String READABLE_MANIFEST_NAME = "manifest.readable";
    public static final String REMOTE_PROP_POSTFIX = ".remote.properties";

    public static final String PATH_DELIMITER = "/";
    public static final String BACKUP_DIR_NAME = "backup_obj";
    public static final String BACKUP_DIR = Joiner.on(PATH_DELIMITER).join(Config.meta_dir, BACKUP_DIR_NAME + "/");

    private DirSaver root;

    public PathBuilder() {

    }

    private PathBuilder(String path) {
        DirSaver backupDir = DirSaver.createWithPath(BACKUP_DIR);
        this.root = backupDir.addDir(path);
    }

    public static PathBuilder createPathBuilder(String path) throws IOException {
        String formattedPath = formatPath(path);
        if (Strings.isNullOrEmpty(formattedPath)) {
            throw new IOException("Invalid path: " + formattedPath);
        }
        PathBuilder pathBuilder = new PathBuilder(formattedPath);
        File file = new File(pathBuilder.getRoot().getFullPath());
        if (file.exists()) {
            LOG.warn("path[{}] dir already exist", formattedPath);
        }
        return pathBuilder;
    }

    private static String formatPath(String path) {
        String formattedPath = path.replaceAll(" ", "");
        formattedPath = formattedPath.replaceAll("/+", "/");
        return formattedPath;
    }

    public DirSaver getRoot() {
        return root;
    }

    public void setRoot(DirSaver newRoot) {
        DirSaver parent = root.getParent();
        root = newRoot;
        root.setParent(parent);
    }

    public FileSaver addFile(String path) {
        return root.addFile(path);
    }

    public String createTableStmt(String dbName, String tableName) {
        // root/dbName/tableName/create_table_stmt
        FileSaver res = addFile(createPath(dbName, tableName, CREATE_TABLE_STMT_FILE));
        String fileName = res.getFullPath();
        LOG.debug("make path: {}", fileName);
        return fileName;
    }

    public String addRollupStmt(String dbName, String tableName) {
        // root/dbName/tableName/add_rollup_stmt
        FileSaver res = addFile(createPath(dbName, tableName, ADD_ROLLUP_STMT_FILE));
        String fileName = res.getFullPath();
        LOG.debug("make path: {}", fileName);
        return fileName;
    }

    public String addPartitionStmt(String dbName, String tableName, String partitionName) {
        // root/dbName/tableName/partitionName/add_partition_stmt
        FileSaver res = addFile(createPath(dbName, tableName, partitionName, ADD_PARTITION_STMT_FILE));
        String fileName = res.getFullPath();
        LOG.debug("make path: {}", fileName);
        return fileName;
    }

    public String tabletRemotePath(String dbName, String tableName, String partitionName,
                                   String indexName, Long tabletId, String remotePath, String label) {
        // root/dbName/tableName/partitionName/indexName/tabletId
        String path = createPath(dbName, tableName, partitionName, indexName, tabletId.toString());
        FileSaver res = addFile(path);
        String fileName = res.getFullPath();
        LOG.debug("make path: {}", fileName);
        return createPath(remotePath, label, path);
    }

    public String manifest() {
        // root/manifest
        FileSaver res = addFile(MANIFEST_NAME);
        String fileName = res.getFullPath();
        LOG.debug("make path: {}", fileName);
        return fileName;
    }

    public String readableManifest() {
        String fileName = createPath(root.getFullPath(), READABLE_MANIFEST_NAME);
        LOG.debug("make path: {}", fileName);
        return fileName;
    }

    public String remoteProperties() {
        // root/label.remote_prop
        FileSaver res = addFile(root.name + REMOTE_PROP_POSTFIX);
        String fileName = res.getFullPath();
        LOG.debug("make path: {}", fileName);
        return fileName;
    }

    public static String createPath(String...nodes) {
        String path = Joiner.on(PATH_DELIMITER).join(nodes);
        return path;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        DirSaver parent = null;
        while ((parent = root.getParent()) != null) {
            if (parent.getName().equals(BACKUP_DIR_NAME)) {
                break;
            }
        }

        Preconditions.checkNotNull(parent);
        Text.writeString(out, root.getName());
        parent.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        DirSaver backupDir = DirSaver.createWithPath(BACKUP_DIR);

        String rootName = Text.readString(in);

        DirSaver parent = new DirSaver();
        parent.readFields(in);
        backupDir.addChild(parent);

        DirSaver currentDir = parent;
        while (!currentDir.hasChild(rootName)) {
            Collection<String> childNames = currentDir.getChildrenName();
            Preconditions.checkState(childNames.size() == 1);
            for (String childName : childNames) {
                currentDir = (DirSaver) currentDir.getChild(childName);
            }
        }

        root = (DirSaver) currentDir.getChild(rootName);
    }
}
