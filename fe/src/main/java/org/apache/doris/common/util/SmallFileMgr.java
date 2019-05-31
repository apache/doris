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

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
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

    public static class SmallFile implements Writable {
        public long dbId;
        public String catalog;
        public String name;
        public long id;
        public String content;
        public long size;
        public String md5;

        private SmallFile() {

        }

        public SmallFile(Long dbId, String catalogName, String fileName, Long id, String content, long size, String md5) {
            this.dbId = dbId;
            this.catalog = catalogName;
            this.name = fileName;
            this.id = id;
            this.content = content;
            this.size = size;
            this.md5 = md5.toLowerCase();
        }

        public static SmallFile read(DataInput in) throws IOException {
            SmallFile smallFile = new SmallFile();
            smallFile.readFields(in);
            return smallFile;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            Text.writeString(out, catalog);
            Text.writeString(out, name);
            out.writeLong(id);
            Text.writeString(out, content);
            out.writeLong(size);
            Text.writeString(out, md5);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            dbId = in.readLong();
            catalog = Text.readString(in);
            name = Text.readString(in);
            id = in.readLong();
            content = Text.readString(in);
            size = in.readLong();
            md5 = Text.readString(in);
        }
    }

    public static class SmallFiles {
        // file name -> file
        private Map<String, SmallFile> files = Maps.newHashMap();

        public SmallFiles() {

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

        public SmallFile removeFile(String fileName) {
            return files.remove(fileName);
        }

        public SmallFile getFile(String fileName) {
            return files.get(fileName);
        }

        public boolean containsFile(String fileName) {
            return files.containsKey(fileName);
        }
    }

    // db id -> catalog -> files
    private Table<Long, String, SmallFiles> files = HashBasedTable.create();
    private Map<Long, SmallFile> idToFiles = Maps.newHashMap();

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
            if (idToFiles.size() >= Config.max_small_file_number) {
                throw new DdlException("File number exceeds limit: " + Config.max_small_file_number);
            }
        }

        SmallFile smallFile = downloadAndCheck(dbId, catalog, fileName, downloadUrl, md5sum);

        synchronized (files) {
            if (idToFiles.size() >= Config.max_small_file_number) {
                throw new DdlException("File number exceeds limit: " + Config.max_small_file_number);
            }

            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                smallFiles = new SmallFiles();
                files.put(dbId, catalog, smallFiles);
            }

            smallFiles.addFile(fileName, smallFile);
            idToFiles.put(smallFile.id, smallFile);

            SmallFileInfo info = new SmallFileInfo(dbId, catalog, smallFile.id, fileName, smallFile.content,
                    smallFile.size, smallFile.md5);
            Catalog.getCurrentCatalog().getEditLog().logCreateSmallFile(info);

            LOG.info("finished to add file {} from url {}. current file number: {}", fileName, downloadUrl,
                    idToFiles.size());
        }
    }

    public void replayCreateFile(SmallFileInfo info) {
        synchronized (files) {
            SmallFiles smallFiles = files.get(info.dbId, info.catalogName);
            if (smallFiles == null) {
                smallFiles = new SmallFiles();
                files.put(info.dbId, info.catalogName, smallFiles);
            }

            try {
                SmallFile smallFile = new SmallFile(info.dbId, info.catalogName, info.fileName, info.fileId, info.content, info.fileSize, info.md5);
                smallFiles.addFile(info.fileName, smallFile);
                idToFiles.put(info.fileId, smallFile);
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
            SmallFile smallFile = smallFiles.removeFile(fileName);
            if (smallFile != null) {
                idToFiles.remove(smallFile.id);

                if (!isReplay) {
                    SmallFileInfo info = new SmallFileInfo(dbId, catalog, fileName);
                    Catalog.getCurrentCatalog().getEditLog().logDropSmallFile(info);
                }

                LOG.info("finished to remove file {}. current file number: {}. is replay: {}",
                        fileName, idToFiles.size(), isReplay);
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

    public SmallFile getSmallFile(long dbId, String catalog, String fileName) {
        synchronized (files) {
            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                return null;
            }
            return smallFiles.getFile(fileName);
        }
    }

    public SmallFile getSmallFile(long fileId) {
        synchronized (files) {
            return idToFiles.get(fileId);
        }
    }

    private SmallFile downloadAndCheck(long dbId, String catalog, String fileName,
            String downloadUrl, String md5sum) throws DdlException {
        try {
            URL url = new URL(downloadUrl);
            // get file length
            URLConnection urlConnection = url.openConnection();
            if (urlConnection instanceof HttpURLConnection) {
                ((HttpURLConnection) urlConnection).setRequestMethod("HEAD");
            }
            urlConnection.setReadTimeout(10000); // 10s
            urlConnection.getInputStream();
            
            int contentLength = urlConnection.getContentLength();
            if (contentLength == -1 || contentLength > Config.max_small_file_size_bytes) {
                throw new DdlException("Failed to download file from url: " + url + ", invalid content length: " + contentLength);
            }
            
            // download url
            MessageDigest digest = MessageDigest.getInstance("MD5");
            int bytesRead = 0;
            byte buf[] = new byte[contentLength];
            try (BufferedInputStream in = new BufferedInputStream(url.openStream())) {
                while (bytesRead < contentLength) {
                    bytesRead += in.read(buf, bytesRead, contentLength - bytesRead);
                }

                // check if there still has data(should not happen)
                if (in.read() != -1) {
                    throw new DdlException("Failed to download file from url: " + url
                            + ", content length does not equals to actual file length");
                }
            }

            if (bytesRead != contentLength) {
                throw new DdlException("Failed to download file from url: " + url
                        + ", invalid read bytes: " + bytesRead + ", expected: " + contentLength);
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

            // encode to base64
            String base64Content = Base64.getEncoder().encodeToString(buf);
            return new SmallFile(dbId, catalog, fileName, Catalog.getCurrentCatalog().getNextId(), base64Content,
                    bytesRead, checksum);
        } catch (IOException | NoSuchAlgorithmException e) {
            LOG.warn("failed to get file from url: {}", downloadUrl, e);
            String errorMsg = e.getMessage();
            if (e instanceof FileNotFoundException) {
                errorMsg = "File not found";
            }
            throw new DdlException("Failed to get file from url: " + downloadUrl + ". Error: " + errorMsg);
        }
    }

    public String saveToFile(long fileId) throws DdlException {
        SmallFile smallFile = getSmallFile(fileId);
        if (smallFile == null) {
            throw new DdlException("File does not exist: " + fileId);
        }
        return saveToFile(smallFile.dbId, smallFile.catalog, smallFile.name);
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
            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
                throw new IOException("failed to make dir for file: " + fileName);
            }
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
        out.writeInt(idToFiles.size());
        for (SmallFile smallFile : idToFiles.values()) {
            smallFile.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SmallFile smallFile = SmallFile.read(in);
            idToFiles.put(smallFile.id, smallFile);
            SmallFiles smallFiles = files.get(smallFile.dbId, smallFile.catalog);
            if (smallFiles == null) {
                smallFiles = new SmallFiles();
                files.put(smallFile.dbId, smallFile.catalog, smallFiles);
            }
            try {
                smallFiles.addFile(smallFile.name, smallFile);
            } catch (DdlException e) {
                // should not happen
                e.printStackTrace();
            }
        }
    }
}
