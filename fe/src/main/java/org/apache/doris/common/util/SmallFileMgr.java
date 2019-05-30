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

package org.apache.doris.common.util;

import org.apache.doris.analysis.CreateFileStmt;
import org.apache.doris.analysis.DropFileStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.SmallFileInfo;

import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/*
 * Author: Chenmingyu
 * Date: May 29, 2019
 */

/*
 * Manage some small files, such as certification file, public/private key used for some operations
 */
public class SmallFileMgr implements Writable {
    public static final Logger LOG = LogManager.getLogger(SmallFileMgr.class);

    public static class SmallFile {
        public String content;
        public long size;
        public String md5;

        public SmallFile(String content, long size, String md5) {
            this.content = content;
            this.size = size;
            this.md5 = md5.toLowerCase();
        }
    }

    public static class SmallFiles implements Writable {
        private String catalog;
        // file name -> file
        private Map<String, SmallFile> files = Maps.newHashMap();

        private SmallFiles() {

        }

        public SmallFiles(String catalog) {
            this.catalog = catalog;
        }

        public String getCatalogName() {
            return catalog;
        }

        public Map<String, SmallFile> getFiles() {
            return files;
        }

        public void addFile(String fileName, SmallFile file) throws DdlException {
            if (files.containsKey(fileName)) {
                throw new DdlException("File " + fileName + " already exist");
            }
            this.files.put(fileName, file);
        }

        public boolean removeFile(String fileName) {
            SmallFile smallFile = files.remove(fileName);
            if (smallFile == null) {
                return false;
            }
            return true;
        }

        public SmallFile getFile(String fileName) {
            return files.get(fileName);
        }

        public boolean containsFile(String fileName) {
            return files.containsKey(fileName);
        }

        public static SmallFiles read(DataInput in) throws IOException {
            SmallFiles smallFiles = new SmallFiles();
            smallFiles.readFields(in);
            return smallFiles;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, catalog);
            out.writeInt(files.size());
            for (Map.Entry<String, SmallFile> entry : files.entrySet()) {
                Text.writeString(out, entry.getKey());
                Text.writeString(out, entry.getValue().content);
                out.writeLong(entry.getValue().size);
                Text.writeString(out, entry.getValue().md5);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            catalog = Text.readString(in);
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String fileName = Text.readString(in);
                String content = Text.readString(in);
                long fileSize = in.readLong();
                String md5 = Text.readString(in);
                files.put(fileName, new SmallFile(content, fileSize, md5));
            }
        }
    }

    // db id -> catalog -> files
    private Table<Long, String, SmallFiles> files = HashBasedTable.create();
    private int fileNum = 0;

    public SmallFileMgr() {
    }

    public void createFile(CreateFileStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }
        downloadAndAddFile(db.getId(), stmt.getCatalogName(), stmt.getFileName(), stmt.getDownloadUrl(), stmt.getChecksum());
    }

    public void dropFile(DropFileStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }
        removeFile(db.getId(), stmt.getCatalogName(), stmt.getFileName(), false);
    }

    private void downloadAndAddFile(long dbId, String catalog, String fileName, String downloadUrl, String md5sum)
            throws DdlException {
        synchronized (files) {
            if (fileNum >= Config.max_small_file_number) {
                throw new DdlException("File number exceeds limit: " + Config.max_small_file_number);
            }
        }

        SmallFile smallFile = downloadAndCheck(downloadUrl, md5sum);

        synchronized (files) {
            if (fileNum >= Config.max_small_file_number) {
                throw new DdlException("File number exceeds limit: " + Config.max_small_file_number);
            }

            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                smallFiles = new SmallFiles(catalog);
                files.put(dbId, catalog, smallFiles);
            }

            smallFiles.addFile(fileName, smallFile);
            fileNum++;

            SmallFileInfo info = new SmallFileInfo(dbId, catalog, fileName, smallFile.content, smallFile.size, smallFile.md5);
            Catalog.getCurrentCatalog().getEditLog().logCreateSmallFile(info);

            LOG.info("finished to add file {} from url {}. current file number: {}", fileName, downloadUrl, fileNum);
        }
    }

    public void replayCreateFile(SmallFileInfo info) {
        synchronized (files) {
            SmallFiles smallFiles = files.get(info.dbId, info.catalogName);
            if (smallFiles == null) {
                smallFiles = new SmallFiles(info.catalogName);
                files.put(info.dbId, info.catalogName, smallFiles);
            }

            try {
                smallFiles.addFile(info.fileName, new SmallFile(info.content, info.fileSize, info.md5));
                fileNum++;
            } catch (DdlException e) {
                LOG.warn("should not happen", e);
            }
        }
    }

    public void removeFile(long dbId, String catalog, String fileName, boolean isReplay) {
        synchronized (files) {
            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                return;
            }
            if (smallFiles.removeFile(fileName)) {
                fileNum--;

                if (!isReplay) {
                    SmallFileInfo info = new SmallFileInfo(dbId, catalog, fileName);
                    Catalog.getCurrentCatalog().getEditLog().logDropSmallFile(info);
                }

                LOG.info("finished to remove file {}. current file number: {}. is replay: {}",
                        fileName, fileNum, isReplay);
            }
        }
    }

    public void replayRemoveFile(SmallFileInfo info) {
        removeFile(info.dbId, info.catalogName, info.fileName, true);
    }

    public boolean containsFile(long dbId, String catalog, String fileName) {
        synchronized (files) {
            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                return false;
            }
            return smallFiles.containsFile(fileName);
        }
    }

    public SmallFile getFile(long dbId, String catalog, String fileName) {
        synchronized (files) {
            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                return null;
            }
            return smallFiles.getFile(fileName);
        }
    }

    private SmallFile downloadAndCheck(String downloadUrl, String md5sum) throws DdlException {
        try {
            URL url = new URL(downloadUrl);

            URLConnection urlConnection = url.openConnection();
            urlConnection.setReadTimeout(10000); // 10s
            InputStream inputStream = urlConnection.getInputStream();

            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[Config.max_small_file_size_bytes];
            // read at most maxFileSizeBytes
            int bytesRead = inputStream.read(buf);
            if (bytesRead < 0) {
                throw new DdlException("No data read from url: " + downloadUrl);
            }

            // read again see if still has data
            byte[] nextBuf = new byte[1];
            int nextBytesRead = inputStream.read(nextBuf);
            if (nextBytesRead != -1) {
                throw new DdlException("Data from url: " + downloadUrl + " exceeds max file size: " + Config.max_small_file_size_bytes);
            }

            // check md5sum if necessary
            digest.update(buf, 0, bytesRead);
            String checksum = Hex.encodeHexString(digest.digest());
            if (!Strings.isNullOrEmpty(md5sum)) {
                if (!checksum.equals(md5sum)) {
                    throw new DdlException("Invalid md5sum of file in url: " + downloadUrl + ", read: " + checksum
                            + ", expected: " + checksum);
                }
            }

            String base64Content = Base64.getEncoder().encodeToString(buf);
            return new SmallFile(base64Content, bytesRead, checksum);
        } catch (IOException | NoSuchAlgorithmException e) {
            LOG.warn("failed to get file from url: {}", downloadUrl, e);
            String errorMsg = e.getMessage();
            if (e instanceof FileNotFoundException) {
                errorMsg = "File not found";
            }
            throw new DdlException("Failed to get file from url: " + downloadUrl + ". Error: " + errorMsg);
        }
    }

    // save the specified file to disk. if file already exist, check it.
    // return the absolute file path.
    public String saveToFile(long dbId, String catalog, String fileName) throws DdlException {
        SmallFile smallFile;
        synchronized (files) {
            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                throw new DdlException("File " + fileName + " does not exist");
            }

            smallFile = smallFiles.getFile(fileName);
            if (smallFile == null) {
                throw new DdlException("File " + fileName + " does not exist");
            }
        }

        // check file
        File file = getAbsoluteFile(dbId, catalog, fileName);
        if (file.exists()) {
            if (!file.isFile()) {
                throw new DdlException("File exist but not a file: " + fileName);
            }

            if (checkMd5(file, smallFile.md5)) {
                return file.getAbsolutePath();
            }

            // file is invalid, delete it and create a new one
            file.delete();
        }

        // write to file
        try {
            file.createNewFile();
            byte[] decoded = Base64.getDecoder().decode(smallFile.content);
            FileOutputStream outputStream = new FileOutputStream(file);
            outputStream.write(decoded);
            outputStream.flush();
            outputStream.close();

            if (!checkMd5(file, smallFile.md5)) {
                throw new DdlException("write file " + fileName +" failed. md5 is invalid. expected: " + smallFile.md5);
            }
        } catch (IOException e) {
            LOG.warn("failed to write file: {}", fileName, e);
            throw new DdlException("failed to write file: " + fileName);
        }

        return file.getAbsolutePath();
    }

    private boolean checkMd5(File file, String expectedMd5) throws DdlException {
        String md5sum = null;
        try {
            md5sum = DigestUtils.md5Hex(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new DdlException("File " + file.getName() + " does not exist");
        } catch (IOException e) {
            LOG.warn("failed to check md5 of file: {}", file.getName(), e);
            throw new DdlException("Failed to check md5 of file: " + file.getName());
        }

        return md5sum.equals(expectedMd5);
    }

    private File getAbsoluteFile(long dbId, String catalog, String fileName) {
        return Paths.get(Config.small_file_dir, String.valueOf(dbId), catalog, fileName).normalize().toAbsolutePath().toFile();
    }

    public List<List<String>> getInfo(String dbName) throws DdlException {
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }
        
        List<List<String>> infos = Lists.newArrayList();
        synchronized (files) {
            if (files.containsRow(db.getId())) {
                Map<String, SmallFiles> dbFiles = files.row(db.getId());
                for (Map.Entry<String, SmallFiles> entry : dbFiles.entrySet()) {
                    SmallFiles smallFiles = entry.getValue();
                    for (Map.Entry<String, SmallFile> entry2 : smallFiles.getFiles().entrySet()) {
                        List<String> info = Lists.newArrayList();
                        info.add(dbName);
                        info.add(entry.getKey()); // catalog
                        info.add(entry2.getKey()); // file name
                        info.add(String.valueOf(entry2.getValue().size)); // file size
                        info.add(entry2.getValue().md5);
                        infos.add(info);
                    }
                }
            }
        }
        return infos;
    }

    public static SmallFileMgr read(DataInput in) throws IOException {
        SmallFileMgr mgr = new SmallFileMgr();
        mgr.readFields(in);
        return mgr;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(files.rowMap().size());
        for (Map.Entry<Long, Map<String, SmallFiles>> entry : files.rowMap().entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Map.Entry<String, SmallFiles> entry2 : entry.getValue().entrySet()) {
                entry2.getValue().write(out);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long dbId = in.readLong();
            int size2 = in.readInt();
            for (int j = 0; j < size2; j++) {
                SmallFiles smallFiles = SmallFiles.read(in);
                files.put(dbId, smallFiles.getCatalogName(), smallFiles);
            }
        }
    }
}
