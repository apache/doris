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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

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
        public boolean isContent;

        private SmallFile() {

        }

        public SmallFile(Long dbId, String catalogName, String fileName, Long id, String content, long size,
                String md5, boolean isContent) {
            this.dbId = dbId;
            this.catalog = catalogName;
            this.name = fileName;
            this.id = id;
            this.content = content;
            this.size = size;
            this.md5 = md5.toLowerCase();
            this.isContent = isContent;
        }

        public static SmallFile read(DataInput in) throws IOException {
            SmallFile smallFile = new SmallFile();
            smallFile.readFields(in);
            return smallFile;
        }

        public byte[] getContentBytes() {
            if (!isContent) {
                return null;
            }
            return Base64.getDecoder().decode(content);
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
            out.writeBoolean(isContent);
        }

        public void readFields(DataInput in) throws IOException {
            dbId = in.readLong();
            catalog = Text.readString(in);
            name = Text.readString(in);
            id = in.readLong();
            content = Text.readString(in);
            size = in.readLong();
            md5 = Text.readString(in);
            isContent = in.readBoolean();
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
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        downloadAndAddFile(db.getId(), stmt.getCatalogName(), stmt.getFileName(),
                stmt.getDownloadUrl(), stmt.getChecksum(), stmt.isSaveContent());
    }

    public void dropFile(DropFileStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        removeFile(db.getId(), stmt.getCatalogName(), stmt.getFileName(), false);
    }

    private void downloadAndAddFile(long dbId, String catalog, String fileName, String downloadUrl, String md5sum,
            boolean saveContent) throws DdlException {
        synchronized (files) {
            if (idToFiles.size() >= Config.max_small_file_number) {
                throw new DdlException("File number exceeds limit: " + Config.max_small_file_number);
            }
        }

        SmallFile smallFile = downloadAndCheck(dbId, catalog, fileName, downloadUrl, md5sum, saveContent);

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

            Env.getCurrentEnv().getEditLog().logCreateSmallFile(smallFile);

            LOG.info("finished to add file {} from url {}. current file number: {}", fileName, downloadUrl,
                    idToFiles.size());
        }
    }

    public void replayCreateFile(SmallFile smallFile) {
        synchronized (files) {
            SmallFiles smallFiles = files.get(smallFile.dbId, smallFile.catalog);
            if (smallFiles == null) {
                smallFiles = new SmallFiles();
                files.put(smallFile.dbId, smallFile.catalog, smallFiles);
            }

            try {
                smallFiles.addFile(smallFile.name, smallFile);
                idToFiles.put(smallFile.id, smallFile);
            } catch (DdlException e) {
                LOG.warn("should not happen", e);
            }
        }
    }

    public void removeFile(long dbId, String catalog, String fileName, boolean isReplay) throws DdlException {
        synchronized (files) {
            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                throw new DdlException("No such file in catalog: " + catalog);
            }
            SmallFile smallFile = smallFiles.removeFile(fileName);
            if (smallFile != null) {
                idToFiles.remove(smallFile.id);

                if (!isReplay) {
                    Env.getCurrentEnv().getEditLog().logDropSmallFile(smallFile);
                }

                LOG.info("finished to remove file {}. current file number: {}. is replay: {}",
                        fileName, idToFiles.size(), isReplay);
            } else {
                throw new DdlException("No such file: " + fileName);
            }
        }
    }

    public void replayRemoveFile(SmallFile smallFile) {
        try {
            removeFile(smallFile.dbId, smallFile.catalog, smallFile.name, true);
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
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

    public SmallFile getSmallFile(long dbId, String catalog, String fileName, boolean needContent)
            throws DdlException {
        synchronized (files) {
            SmallFiles smallFiles = files.get(dbId, catalog);
            if (smallFiles == null) {
                throw new DdlException("file does not exist with db: " + dbId + " and catalog: " + catalog);
            }
            SmallFile smallFile = smallFiles.getFile(fileName);
            if (smallFile == null) {
                throw new DdlException("File does not exist");
            } else if (needContent && !smallFile.isContent) {
                throw new DdlException("File exists but not with content");
            }
            return smallFile;
        }
    }

    public SmallFile getSmallFile(long fileId) {
        synchronized (files) {
            return idToFiles.get(fileId);
        }
    }

    private SmallFile downloadAndCheck(long dbId, String catalog, String fileName,
            String downloadUrl, String md5sum, boolean saveContent) throws DdlException {
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
                throw new DdlException("Failed to download file from url: " + url
                        + ", invalid content length: " + contentLength);
            }

            int bytesRead = 0;
            String base64Content = null;
            MessageDigest digest = MessageDigest.getInstance("MD5");
            if (saveContent) {
                // download from url, and check file size
                bytesRead = 0;
                byte[] buf = new byte[contentLength];
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

                digest.update(buf, 0, bytesRead);
                // encoded to base64
                base64Content = Base64.getEncoder().encodeToString(buf);
            } else {
                byte[] buf = new byte[4096];
                int tmpSize = 0;
                try (BufferedInputStream in = new BufferedInputStream(url.openStream())) {
                    do {
                        tmpSize = in.read(buf);
                        if (tmpSize < 0) {
                            break;
                        }
                        digest.update(buf, 0, tmpSize);
                        bytesRead += tmpSize;
                    } while (true);
                }
            }

            // check md5sum if necessary
            String checksum = Hex.encodeHexString(digest.digest());
            if (!Strings.isNullOrEmpty(md5sum)) {
                if (!checksum.equalsIgnoreCase(md5sum)) {
                    throw new DdlException("Invalid md5sum of file in url: " + downloadUrl + ", read: " + checksum
                            + ", expected: " + checksum);
                }
            }

            SmallFile smallFile;
            long fileId = Env.getCurrentEnv().getNextId();
            if (saveContent) {
                smallFile = new SmallFile(dbId, catalog, fileName, fileId, base64Content, bytesRead,
                        checksum, true /* is content */);
            } else {
                // only save download url
                smallFile = new SmallFile(dbId, catalog, fileName, fileId, downloadUrl, bytesRead,
                        checksum, false /* not content */);
            }
            return smallFile;
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

            if (!smallFile.isContent) {
                throw new DdlException("File does not contain content: " + smallFile.id);
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
            try (FileOutputStream outputStream = new FileOutputStream(file)) {
                outputStream.write(decoded);
                outputStream.flush();
            }

            if (!checkMd5(file, smallFile.md5)) {
                throw new DdlException("write file " + fileName
                        + " failed. md5 is invalid. expected: " + smallFile.md5);
            }
        } catch (IOException e) {
            LOG.warn("failed to write file: {}", fileName, e);
            throw new DdlException("failed to write file: " + fileName);
        }

        return file.getAbsolutePath();
    }

    private boolean checkMd5(File file, String expectedMd5) throws DdlException {
        String md5sum;
        try (FileInputStream fis = new FileInputStream(file)) {
            md5sum = DigestUtils.md5Hex(fis);
        } catch (FileNotFoundException e) {
            throw new DdlException("File " + file.getName() + " does not exist");
        } catch (IOException e) {
            LOG.warn("failed to check md5 of file: {}", file.getName(), e);
            throw new DdlException("Failed to check md5 of file: " + file.getName());
        }

        return md5sum.equalsIgnoreCase(expectedMd5);
    }

    private File getAbsoluteFile(long dbId, String catalog, String fileName) {
        return Paths.get(Config.small_file_dir, String.valueOf(dbId), catalog, fileName)
                .normalize().toAbsolutePath().toFile();
    }

    public List<List<String>> getInfo(String dbName) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        List<List<String>> infos = Lists.newArrayList();
        synchronized (files) {
            if (files.containsRow(db.getId())) {
                Map<String, SmallFiles> dbFiles = files.row(db.getId());
                for (Map.Entry<String, SmallFiles> entry : dbFiles.entrySet()) {
                    SmallFiles smallFiles = entry.getValue();
                    for (Map.Entry<String, SmallFile> entry2 : smallFiles.getFiles().entrySet()) {
                        List<String> info = Lists.newArrayList();
                        info.add(String.valueOf(entry2.getValue().id));
                        info.add(dbName);
                        info.add(entry.getKey()); // catalog
                        info.add(entry2.getKey()); // file name
                        info.add(String.valueOf(entry2.getValue().size)); // file size
                        info.add(String.valueOf(entry2.getValue().isContent));
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
                LOG.warn("should not happen", e);
            }
        }
    }
}
