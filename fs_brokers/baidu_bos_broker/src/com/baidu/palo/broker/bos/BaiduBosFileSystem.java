// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.broker.bos;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;
import org.apache.http.ConnectionClosedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BaiduBosFileSystem extends FileSystem {

    private static Logger logger = LogManager
            .getLogger(FileSystemManager.class.getName());

    private static final String FOLDER_SUFFIX = "/";
    private static String BOS_BLOCK_SIZE = "fs.bos.block.size";
    private static final long DEFAULT_BOS_BLOCK_SIZE = 128 * 1024 * 1024L; // 128M
    private static final int BOS_MAX_LISTING_LENGTH = 1000;
    public static final String PATH_DELIMITER = Path.SEPARATOR;

    private class NativeBOSFsInputStream extends FSInputStream {

        private InputStream in;
        private final String key;
        private long pos = 0;
        private FileMetadata fileMetaData;

        public NativeBOSFsInputStream(InputStream in, String key,
                FileMetadata fileMetaData) {
            this.in = in;
            this.key = key;
            this.fileMetaData = fileMetaData;
        }

        public synchronized int read() throws IOException {
            if (this.pos >= this.fileMetaData.getLength()) {
                return -1;
            }

            int result = -1;
            try {
                result = in.read();
            } catch (ConnectionClosedException cce) {
                LOG.debug("ConnectionClosedException has been catched ...", cce);
                seek(getPos());
                result = in.read();
                LOG.debug("InputStream reset");
            }
            if (result != -1) {
                pos++;
            }
            if (statistics != null && result != -1) {
                statistics.incrementBytesRead(1);
            }
            return result;
        }

        public synchronized int read(byte[] b, int off, int len)
                throws IOException {
            if (this.pos >= this.fileMetaData.getLength()) {
                return -1;
            }

            int result = -1;
            try {
                result = in.read(b, off, len);
            } catch (ConnectionClosedException cce) {
                LOG.debug("ConnectionClosedException has been catched ...", cce);
                seek(getPos());
                result = in.read(b, off, len);
                LOG.debug("InputStream reset");
            }
            if (result > 0) {
                pos += result;
                if (statistics != null) {
                    statistics.incrementBytesRead(result);
                }
            }
            return result;
        }

        public void close() throws IOException {
            in.close();
        }

        public synchronized void seek(long pos) throws IOException {
            in.close();

            if (pos >= this.fileMetaData.getLength()) {
                this.pos = pos;
                return;
            }

            in = store.retrieve(key, pos);
            this.pos = pos;
        }

        public synchronized long getPos() throws IOException {
            return pos;
        }

        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }
    }

    private URI uri;
    private NativeFileSystemStore store;
    private Path workingDir;

    public BaiduBosFileSystem() {
        // set store in initialize()
    }

    public BaiduBosFileSystem(NativeFileSystemStore store) {
        this.store = store;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        if (store == null) {
            store = createDefaultStore(conf);
        }
        store.initialize(uri, conf);
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/user", System.getProperty("user.name"))
                .makeQualified(this);
    }

    private static NativeFileSystemStore createDefaultStore(Configuration conf) {
        NativeFileSystemStore store = new BosNativeFileSystemStore();

        RetryPolicy basePolicy = RetryPolicies
                .retryUpToMaximumCountWithFixedSleep(
                        conf.getInt("fs.bos.maxRetries", 4),
                        conf.getLong("fs.bos.sleepTimeSeconds", 10),
                        TimeUnit.SECONDS);
        Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = 
                new HashMap<Class<? extends Exception>, RetryPolicy>();
        exceptionToPolicyMap.put(IOException.class, basePolicy);

        RetryPolicy methodPolicy = RetryPolicies.retryByException(
                RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
        Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();
        methodNameToPolicyMap.put("storeFile", methodPolicy);

        return (NativeFileSystemStore) RetryProxy.create(
                NativeFileSystemStore.class, store, methodNameToPolicyMap);
    }

    private static String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Path must be absolute: " + path);
        }
        return path.toUri().getPath().substring(1); // remove initial slash
    }

    private static Path keyToPath(String key) {
        return new Path("/" + key);
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    /** This optional operation is not yet supported. */
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {

        if (exists(f)) {
            if (!overwrite) {
                throw new IOException("File already exists:" + f);
            } else {
                delete(f, false);
            }
        }

        Path parent = f.getParent();
        if (parent != null) {
            boolean parentExists = false;
            try {
                FileStatus stat = getFileStatus(parent);
                if (stat != null && !stat.isDir()) {
                    throw new IOException("parent " + parent
                            + " is not a directory");
                }
            } catch (FileNotFoundException e) {
                parentExists = false;
            }

            if (!parentExists) {
                mkdirs(parent);
            }
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        return new FSDataOutputStream(store.createFile(key, getConf(),
                bufferSize), statistics);
    }

    @Override
    @Deprecated
    public boolean delete(Path path) throws IOException {
        return delete(path, true);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        FileStatus status;
        try {
            status = getFileStatus(f);
        } catch (FileNotFoundException e) {
            return false;
        }
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        if (status.isDir()) {
            FileStatus[] contents = listStatus(f);
            if (!recursive && contents != null && contents.length > 0) {
                throw new IOException("Directory " + f.toString()
                        + " is not empty.");
            }

            if (contents != null) {
                for (FileStatus p : contents) {
                    if (!delete(p.getPath(), recursive)) {
                        return false;
                    }
                }
            }
            try {
                store.delete(key + FOLDER_SUFFIX);
            } catch (FileNotFoundException e) {
                // TODO add exception here
            }
        } else {
            store.delete(key);
        }
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        if (key.length() == 0) { // root always exists
            return newDirectory(null, absolutePath);
        }

        try {
            FileMetadata meta = store.retrieveMetadata(key);
            if (meta != null) {
                if (key.endsWith(FOLDER_SUFFIX)) {
                    return newDirectory(meta, absolutePath);
                } else {
                    return newFile(meta, absolutePath);
                }
            }
        } catch (FileNotFoundException e) {
            try {
                FileMetadata meta = store.retrieveMetadata(key + FOLDER_SUFFIX);
                if (meta != null) {
                    return newDirectory(meta, absolutePath);
                }
            } catch (FileNotFoundException ex) {
                try {
                    PartialListing listing = store.list(key,
                            BOS_MAX_LISTING_LENGTH, null);
                    if (listing != null) {
                        if (listing.getFiles().length > 0
                                || listing.getCommonPrefixes().length > 0) {
                            return newDirectory(null, absolutePath);
                        }
                    }
                } catch (FileNotFoundException exx) {
                    throw new FileNotFoundException(absolutePath
                            + ": No such file or directory.");
                }
            }
        }

        throw new FileNotFoundException(absolutePath
                + ": No such file or directory.");
    }

    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * <p>
     * If <code>f</code> is a file, this method will make a single call to S3.
     * If <code>f</code> is a directory, this method will make a maximum of
     * (<i>n</i> / 1000) + 2 calls to S3, where <i>n</i> is the total number of
     * files and directories contained directly in <code>f</code>.
     * </p>
     */
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        if (key.length() > 0) {
            FileStatus meta = getFileStatus(f);
            if (meta != null && !meta.isDir()) {
                return new FileStatus[] { meta };
            }
        }

        URI pathUri = absolutePath.toUri();
        Set<FileStatus> status = new TreeSet<FileStatus>();
        String priorLastKey = null;
        do {
            PartialListing listing = store.list(key, BOS_MAX_LISTING_LENGTH,
                    priorLastKey);
            if (listing != null) {
                for (FileMetadata fileMetadata : listing.getFiles()) {
                    if (fileMetadata.getKey() != null
                            && fileMetadata.getKey().length() > 0
                            && !fileMetadata.getKey().endsWith(FOLDER_SUFFIX)) {
                        Path subpath = keyToPath(fileMetadata.getKey());
                        String relativePath = pathUri.relativize(
                                subpath.toUri()).getPath();
                        // if (relativePath.endsWith(FOLDER_SUFFIX)) {
                            // String dirPath = relativePath.substring(0,
                            // relativePath.indexOf(FOLDER_SUFFIX));
                            // status.add(newDirectory(fileMetadata, new
                            // Path(absolutePath, dirPath)));
                        // } else {
                        //    status.add(newFile(fileMetadata, new Path(
                        //           absolutePath, relativePath)));
                        // }
                        if (!relativePath.endsWith(FOLDER_SUFFIX)) {
                            status.add(newFile(fileMetadata, new Path(
                                    absolutePath, relativePath)));
                        }
                    }
                }
                for (String commonPrefix : listing.getCommonPrefixes()) {
                    if (commonPrefix != null && commonPrefix.length() > 0) {
                        Path subpath = keyToPath(commonPrefix);
                        String relativePath = pathUri.relativize(
                                subpath.toUri()).getPath();
                        status.add(newDirectory(null, new Path(absolutePath,
                                relativePath)));
                    }
                }
                priorLastKey = listing.getPriorLastKey();
            }
        } while (priorLastKey != null && priorLastKey.length() > 0);

        return status.toArray(new FileStatus[0]);
    }

    private String getRelativePath(String path) {
        return path.substring(path.indexOf(PATH_DELIMITER) + 1);
    }

    private FileStatus newFile(FileMetadata meta, Path path) {
        return new FileStatus(meta.getLength(), false, 1, getConf().getLong(
                BOS_BLOCK_SIZE, DEFAULT_BOS_BLOCK_SIZE),
                meta.getLastModified(), path.makeQualified(this));
    }

    private FileStatus newDirectory(FileMetadata meta, Path path) {
        return new FileStatus(0, true, 1, getConf().getLong(BOS_BLOCK_SIZE,
                DEFAULT_BOS_BLOCK_SIZE), (meta == null ? 0 : 
                    meta.getLastModified()), path.makeQualified(this));
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        Path absolutePath = makeAbsolute(f);
        List<Path> paths = new ArrayList<Path>();
        do {
            paths.add(0, absolutePath);
            absolutePath = absolutePath.getParent();
        } while (absolutePath != null);

        boolean result = true;
        for (Path path : paths) {
            result &= mkdir(path);
        }
        return result;
    }

    private boolean mkdir(Path f) throws IOException {
        try {
            FileStatus fileStatus = getFileStatus(f);
            if (!fileStatus.isDir()) {
                throw new IOException(String.format(
                        "Can't make directory for path %s since it is a file.",
                        f));

            }
        } catch (FileNotFoundException e) {
            String key = pathToKey(f);
            if (!key.endsWith(FOLDER_SUFFIX)) {
                key = pathToKey(f) + FOLDER_SUFFIX;
            }
            store.storeEmptyFile(key);
        }
        return true;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        if (!exists(f)) {
            throw new FileNotFoundException(f.toString());
        }
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        FileMetadata fileMetaData = store.retrieveMetadata(key);
        return new FSDataInputStream(new BufferedFSInputStream(
                new NativeBOSFsInputStream(store.retrieve(key), key,
                        fileMetaData), bufferSize));
    }

    // rename() and delete() use this method to ensure that the parent directory
    // of the source does not vanish.
    private void createParent(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            String key = pathToKey(makeAbsolute(parent));
            if (key.length() > 0) {
                store.storeEmptyFile(key + FOLDER_SUFFIX);
            }
        }
    }

    private boolean existsAndIsFile(Path f) throws IOException {
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        if (key.length() == 0) {
            return false;
        }

        FileStatus stat = getFileStatus(absolutePath);

        if (stat != null && !stat.isDir()) {
            return true;
        }

        return false;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        String srcKey = pathToKey(makeAbsolute(src));

        if (srcKey.length() == 0) {
            // Cannot rename root of file system
            return false;
        }

        // Figure out the final destination
        String dstKey;
        try {
            boolean dstIsFile = existsAndIsFile(dst);
            if (dstIsFile) {
                // Attempting to overwrite a file using rename()
                return false;
            } else {
                // Move to within the existent directory
                dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
            }
        } catch (FileNotFoundException e) {
            // dst doesn't exist, so we can proceed
            dstKey = pathToKey(makeAbsolute(dst));
            try {
                if (!getFileStatus(dst.getParent()).isDir()) {
                    return false; // parent dst is a file
                }
            } catch (FileNotFoundException ex) {
                return false; // parent dst does not exist
            }
        }

        try {
            boolean srcIsFile = existsAndIsFile(src);
            createParent(src);
            if (srcIsFile) {
                rename(srcKey, dstKey);
            } else {
                // Move the folder object
                // srcKeys.add(srcKey + FOLDER_SUFFIX);
                // store.storeEmptyFile(dstKey + FOLDER_SUFFIX);

                // Move everything inside the folder
                String priorLastKey = null;
                do {
                    PartialListing listing = store.list(srcKey,
                            BOS_MAX_LISTING_LENGTH, priorLastKey, null);
                    for (FileMetadata file : listing.getFiles()) {
                        rename(file.getKey(),
                                dstKey
                                        + file.getKey().substring(
                                                srcKey.length()));
                    }
                    priorLastKey = listing.getPriorLastKey();
                } while (priorLastKey != null);
            }

            return true;
        } catch (FileNotFoundException e) {
            // Source file does not exist;
            return false;
        }
    }

    private void rename(String srcKey, String dstKey) throws IOException {
        int intervalSeconds = 5;
        int retry = 5;
        while (true) {
            try {
                store.rename(srcKey, dstKey);
                break;
            } catch (FileNotFoundException notFoundException) {
                throw new IOException(notFoundException);
            } catch (IOException ioException) {
                if (retry <= 0) {
                    throw new IOException(ioException);
                }

                try {
                    TimeUnit.SECONDS.sleep(intervalSeconds);
                } catch (InterruptedException e) {
                    // Just retry, so catch the exceptions thrown by sleep
                }
                intervalSeconds *= 2;
                retry--;
            }
        }
    }

    /**
     * Set the working directory to the given directory.
     */
    @Override
    public void setWorkingDirectory(Path newDir) {
        workingDir = newDir;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

}
