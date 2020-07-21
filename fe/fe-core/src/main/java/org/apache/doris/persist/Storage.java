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

import org.apache.doris.ha.FrontendNodeType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

// VERSION file contains clusterId. eg:
//      clusterId=123456
// ROLE file contains FrontendNodeType and NodeName. eg:
//      role=OBSERVER
//      name=172.0.0.1_1234_DNwid284dasdwd
public class Storage {
    private static final Logger LOG = LogManager.getLogger(Storage.class);

    public static final String CLUSTER_ID = "clusterId";
    public static final String TOKEN = "token";
    public static final String FRONTEND_ROLE = "role";
    public static final String NODE_NAME = "name";
    public static final String EDITS = "edits";
    public static final String IMAGE = "image";
    public static final String IMAGE_NEW = "image.ckpt";
    public static final String VERSION_FILE = "VERSION";
    public static final String ROLE_FILE = "ROLE";

    private int clusterID = 0;
    private String token;
    private FrontendNodeType role = FrontendNodeType.UNKNOWN;
    private String nodeName;
    private long editsSeq;
    private long imageSeq;
    private String metaDir;
    private List<Long> editsFileSequenceNumbers;

    public Storage(int clusterID, String token, String metaDir) {
        this.clusterID = clusterID;
        this.token = token;
        this.metaDir = metaDir;
    }

    public Storage(int clusterID, String token, long imageSeq, long editsSeq, String metaDir) {
        this.clusterID = clusterID;
        this.token = token;
        this.editsSeq = editsSeq;
        this.imageSeq = imageSeq;
        this.metaDir = metaDir;
    }

    public Storage(String metaDir) throws IOException {
        this.editsFileSequenceNumbers = new ArrayList<Long>();
        this.metaDir = metaDir;

        reload();
    }

    public List<Long> getEditsFileSequenceNumbers() {
        Collections.sort(editsFileSequenceNumbers);
        return this.editsFileSequenceNumbers;
    }

    public void reload() throws IOException {
        // Read version file info
        Properties prop = new Properties();
        File versionFile = getVersionFile();
        if (versionFile.isFile()) {
            FileInputStream in = new FileInputStream(versionFile);
            prop.load(in);
            in.close();
            clusterID = Integer.parseInt(prop.getProperty(CLUSTER_ID));
            if (prop.getProperty(TOKEN) != null) {
                token = prop.getProperty(TOKEN);
            }
        }

        File roleFile = getRoleFile();
        if (roleFile.isFile()) {
            FileInputStream in = new FileInputStream(roleFile);
            prop.load(in);
            in.close();
            role = FrontendNodeType.valueOf(prop.getProperty(FRONTEND_ROLE));
            // For compatibility, NODE_NAME may not exist in ROLE file, set nodeName to null
            nodeName = prop.getProperty(NODE_NAME, null);
        }

        // Find the latest image
        File dir = new File(metaDir);
        File[] children = dir.listFiles();
        if (children == null) {
            return;
        } else {
            for (File child : children) {
                String name = child.getName();
                try {
                    if (!name.equals(EDITS) && !name.equals(IMAGE_NEW)
                            && !name.endsWith(".part") && name.contains(".")) {
                        if (name.startsWith(IMAGE)) {
                            imageSeq = Math.max(Long.parseLong(name.substring(name.lastIndexOf('.') + 1)), imageSeq);
                        } else if (name.startsWith(EDITS)) {
                            // Just record the sequence part of the file name
                            editsFileSequenceNumbers.add(Long.parseLong(name.substring(name.lastIndexOf('.') + 1)));
                            editsSeq = Math.max(Long.parseLong(name.substring(name.lastIndexOf('.') + 1)), editsSeq);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn(name + " is not a validate meta file, ignore it");
                }
            }
        }

    }

    public int getClusterID() {
        return clusterID;
    }

    public void setClusterID(int clusterID) {
        this.clusterID = clusterID;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public FrontendNodeType getRole() {
        return role;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getMetaDir() {
        return metaDir;
    }

    public void setMetaDir(String metaDir) {
        this.metaDir = metaDir;
    }

    public long getImageSeq() {
        return imageSeq;
    }

    public void setImageSeq(long imageSeq) {
        this.imageSeq = imageSeq;
    }

    public void setEditsSeq(long editsSeq) {
        this.editsSeq = editsSeq;
    }

    public long getEditsSeq() {
        return editsSeq;
    }

    public static int newClusterID() {
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());

        int newID = 0;
        while (newID == 0) {
            newID = random.nextInt(0x7FFFFFFF);
        }
        return newID;
    }

    public static String newToken() {
        return UUID.randomUUID().toString();
    }

    private void setFields(Properties properties) throws IOException {
        Preconditions.checkState(clusterID > 0);
        properties.setProperty(CLUSTER_ID, String.valueOf(clusterID));

        if (!Strings.isNullOrEmpty(token)) {
            properties.setProperty(TOKEN, token);
        }
    }

    public void writeClusterIdAndToken() throws IOException {
        Properties properties = new Properties();
        setFields(properties);

        RandomAccessFile file = new RandomAccessFile(new File(metaDir, VERSION_FILE), "rws");
        FileOutputStream out = null;

        try {
            file.seek(0);
            out = new FileOutputStream(file.getFD());
            properties.store(out, null);
            file.setLength(out.getChannel().position());
        } finally {
            if (out != null) {
                out.close();
            }
            file.close();
        }
    }

    public void writeFrontendRoleAndNodeName(FrontendNodeType role, String nameNode) throws IOException {
        Preconditions.checkState(!Strings.isNullOrEmpty(nameNode));
        Properties properties = new Properties();
        properties.setProperty(FRONTEND_ROLE, role.name());
        properties.setProperty(NODE_NAME, nameNode);

        RandomAccessFile file = new RandomAccessFile(new File(metaDir, ROLE_FILE), "rws");
        FileOutputStream out = null;

        try {
            file.seek(0);
            out = new FileOutputStream(file.getFD());
            properties.store(out, null);
            file.setLength(out.getChannel().position());
        } finally {
            if (out != null) {
                out.close();
            }
            file.close();
        }
    }

    public void clear() throws IOException {
        File metaFile = new File(metaDir);
        if (metaFile.exists()) {
            String[] children = metaFile.list();
            if (children != null) {
                for (String child : children) {
                    File file = new File(metaFile, child);
                    file.delete();
                }
            }
            metaFile.delete();
        }

        if (!metaFile.mkdirs()) {
            throw new IOException("Cannot create directory " + metaFile);
        }
    }

    public static void rename(File from, File to) throws IOException {
        if (!from.renameTo(to)) {
            throw new IOException("Failed to rename  " + from.getCanonicalPath()
                    + " to " + to.getCanonicalPath());
        }
    }

    public File getCurrentImageFile() {
        return getImageFile(imageSeq);
    }

    public File getImageFile(long version) {
        return getImageFile(new File(metaDir), version);
    }

    public static File getImageFile(File dir, long version) {
        return new File(dir, IMAGE + "." + version);
    }

    public final File getVersionFile() {
        return new File(metaDir, VERSION_FILE);
    }

    public final File getRoleFile() {
        return new File(metaDir, ROLE_FILE);
    }

    public File getCurrentEditsFile() {
        return new File(metaDir, EDITS);
    }

    public static File getCurrentEditsFile(File dir) {
        return new File(dir, EDITS);
    }

    public File getEditsFile(long seq) {
        return getEditsFile(new File(metaDir), seq);
    }

    public static File getEditsFile(File dir, long seq) {
        return new File(dir, EDITS + "." + seq);
    }

    public static long getMetaSeq(File file) {
        String filename = file.getName();
        return Long.parseLong(filename.substring(filename.lastIndexOf('.') + 1));
    }

}

