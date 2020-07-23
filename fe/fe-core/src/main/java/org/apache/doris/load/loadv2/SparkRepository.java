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

package org.apache.doris.load.loadv2;

import org.apache.doris.PaloFe;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.thrift.TBrokerFileStatus;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * SparkRepository represents the remote repository for spark archives uploaded by spark
 * The organization in repository is:
 *
 * * __spark_repository__/
 *   * __archive_1_0_0/
 *     * __lib_990325d2c0d1d5e45bf675e54e44fb16_spark-dpp.jar
 *     * __lib_7670c29daf535efe3c9b923f778f61fc_spark-2x.zip
 *   * __archive_2_2_0/
 *     * __lib_64d5696f99c379af2bee28c1c84271d5_spark-dpp.jar
 *     * __lib_1bbb74bb6b264a270bc7fca3e964160f_spark-2x.zip
 *   * __archive_3_2_0/
 *     * ...
 */
public class SparkRepository {
    private static final Logger LOG = LogManager.getLogger(SparkRepository.class);

    public static final String REPOSITORY_DIR = "__spark_repository__";
    public static final String PREFIX_ARCHIVE = "__archive_";
    public static final String PREFIX_LIB = "__lib_";
    public static final String SPARK_DPP = "spark-dpp";
    public static final String SPARK_2X = "spark-2x";
    public static final String SUFFIX = ".zip";

    private static final String PATH_DELIMITER = "/";
    private static final String FILE_NAME_SEPARATOR = "_";

    private String remoteRepositoryPath;
    private BrokerDesc brokerDesc;

    private String localDppPath;
    private String localSpark2xPath;

    // Version of the spark dpp program in this cluster
    private String currentDppVersion;
    // Archive that current dpp version pointed to
    private SparkArchive currentArchive;

    private ReentrantReadWriteLock rwLock;
    private boolean isInit;

    public SparkRepository(String remoteRepositoryPath, BrokerDesc brokerDesc) {
        this.remoteRepositoryPath = remoteRepositoryPath;
        this.brokerDesc = brokerDesc;
        this.currentDppVersion = FeConstants.spark_dpp_version;
        currentArchive = new SparkArchive(getRemoteArchivePath(currentDppVersion), currentDppVersion);
        this.rwLock = new ReentrantReadWriteLock();
        this.isInit = false;
        this.localDppPath = PaloFe.DORIS_HOME_DIR + "/spark-dpp/spark-dpp.jar";
        if (!Strings.isNullOrEmpty(Config.spark_resource_path)) {
            this.localSpark2xPath = Config.spark_resource_path;
        } else {
            this.localSpark2xPath = Config.spark_home_default_dir + "/jars/spark-2x.zip";
        }
    }

    public boolean prepare() throws LoadException {
        if (!isInit) {
            initRepository();
        }
        return isInit;
    }

    private void initRepository() throws LoadException {
        LOG.info("start to init remote repository");
        boolean needUpload = false;
        boolean needReplace = false;
        CHECK: {
            if (Strings.isNullOrEmpty(remoteRepositoryPath) || brokerDesc == null) {
                break CHECK;
            }

            if (!checkCurrentArchiveExists()) {
                needUpload = true;
                break CHECK;
            }

            // init current archive
            String remoteArchivePath = getRemoteArchivePath(currentDppVersion);
            List<SparkLibrary> libraries = Lists.newArrayList();
            getLibraries(remoteArchivePath, libraries);
            if (libraries.size() != 2) {
                needUpload = true;
                needReplace = true;
                break CHECK;
            }
            currentArchive.libraries.addAll(libraries);
            for (SparkLibrary library : currentArchive.libraries) {
                String localMd5sum = null;
                switch (library.libType) {
                    case DPP:
                        localMd5sum = getMd5String(localDppPath);
                        break;
                    case SPARK2X:
                        localMd5sum = getMd5String(localSpark2xPath);
                        break;
                    default:
                        Preconditions.checkState(false, "wrong library type: " + library.libType);
                        break;
                }
                if (!localMd5sum.equals(library.md5sum)) {
                    needUpload = true;
                    needReplace = true;
                    break;
                }
            }
        }

        if (needUpload) {
            uploadArchive(needReplace);
        }
        isInit = true;
        LOG.info("init spark repository success, current dppVersion={}, archive path={}, libraries size={}",
                currentDppVersion, currentArchive.remotePath, currentArchive.libraries.size());
    }

    public boolean checkCurrentArchiveExists() {
        boolean result = false;
        Preconditions.checkNotNull(remoteRepositoryPath);
        String remotePath = getRemoteArchivePath(currentDppVersion);
        readLock();
        try {
            result = BrokerUtil.checkPathExist(remotePath, brokerDesc);
            LOG.info("check archive exists in repository, {}", result);
        } catch (UserException e) {
            LOG.warn("Failed to check remote archive exist, path={}, version={}", remotePath, currentDppVersion);
        } finally {
            readUnlock();
        }
        return result;
    }

    private void uploadArchive(boolean isReplace) throws LoadException {
        writeLock();
        try {
            String remoteArchivePath = getRemoteArchivePath(currentDppVersion);
            if (isReplace) {
                BrokerUtil.deletePath(remoteArchivePath, brokerDesc);
                currentArchive.libraries.clear();
            }
            String srcFilePath = null;
            // upload dpp
            {
                srcFilePath = localDppPath;
                String md5sum = getMd5String(srcFilePath);
                long size = getFileSize(srcFilePath);
                String fileName = getFileName(PATH_DELIMITER, srcFilePath);
                String destFilePath = remoteArchivePath + PATH_DELIMITER +
                        assemblyFileName(PREFIX_LIB, md5sum, fileName, "");
                upload(srcFilePath, destFilePath);
                currentArchive.libraries.add(new SparkLibrary(destFilePath, md5sum, SparkLibrary.LibType.DPP, size));
            }
            // upload spark2x
            {
                srcFilePath = localSpark2xPath;
                String md5sum = getMd5String(srcFilePath);
                long size = getFileSize(srcFilePath);
                String fileName = getFileName(PATH_DELIMITER, srcFilePath);
                String destFilePath = remoteArchivePath + PATH_DELIMITER +
                        assemblyFileName(PREFIX_LIB, md5sum, fileName, "");
                upload(srcFilePath, destFilePath);
                currentArchive.libraries.add(new SparkLibrary(destFilePath, md5sum, SparkLibrary.LibType.SPARK2X, size));
            }
            LOG.info("finished to upload archive to repository, currentDppVersion={}, path={}",
                    currentDppVersion, remoteArchivePath);
        } catch (UserException e) {
            throw new LoadException(e.getMessage());
        } finally {
            writeUnlock();
        }
    }

    private void getLibraries(String remoteArchivePath, List<SparkLibrary> libraries) throws LoadException {
        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        readLock();
        try {
            LOG.info("input remote archive path, path={}", remoteArchivePath);
            BrokerUtil.parseFile(remoteArchivePath + "/*", brokerDesc, fileStatuses);
        } catch (UserException e) {
            throw new LoadException(e.getMessage());
        } finally {
            readUnlock();
        }

        for (TBrokerFileStatus fileStatus : fileStatuses) {
            String fileName = getFileName(PATH_DELIMITER, fileStatus.path);
            if (!fileName.startsWith(PREFIX_LIB)) {
                continue;
            }
            String[] lib_arg = unWrap(PREFIX_LIB, SUFFIX, fileName).split(FILE_NAME_SEPARATOR);
            if (lib_arg.length != 2) {
                continue;
            }
            String md5sum = lib_arg[0];
            String type = lib_arg[1];
            SparkLibrary.LibType libType = null;
            switch (type) {
                case SPARK_DPP:
                    libType = SparkLibrary.LibType.DPP;
                    break;
                case SPARK_2X:
                    libType = SparkLibrary.LibType.SPARK2X;
                    break;
                default:
                    Preconditions.checkState(false, "wrong library type: " + type);
                    break;
            }
            SparkLibrary remoteFile = new SparkLibrary(fileStatus.path, md5sum, libType, fileStatus.size);
            libraries.add(remoteFile);
            LOG.info("get Libraries from remote archive, archive path={}, library={}, md5sum={}, size={}",
                    remoteArchivePath, remoteFile.remotePath, remoteFile.md5sum, remoteFile.size);
        }
    }

    private String getMd5String(String filePath) throws LoadException {
        File file = new File(filePath);
        String md5sum = null;
        try {
            md5sum = DigestUtils.md5Hex(new FileInputStream(file));
            Preconditions.checkNotNull(md5sum);
            LOG.info("get md5sum from file {}, md5sum={}", filePath, md5sum);
            return md5sum;
        } catch (FileNotFoundException e) {
            throw new LoadException("file " + filePath + "dose not exist");
        } catch (IOException e) {
            throw new LoadException("failed to get md5sum from file " + filePath);
        }
    }

    private long getFileSize(String filePath) throws LoadException {
        File file = new File(filePath);
        long size = file.length();
        if (size <= 0) {
            throw new LoadException("failed to get size from file " + filePath);
        }
        return size;
    }

    private void upload(String srcFilePath, String destFilePath) throws LoadException {
        try {
            BrokerUtil.writeFile(srcFilePath, destFilePath , brokerDesc);
            LOG.info("finished to upload file, localPath={}, remotePath={}", srcFilePath, destFilePath);
        } catch (UserException e) {
            throw new LoadException("failed to upload lib to repository, srcPath=" +srcFilePath +
                    " destPath=" + destFilePath + " message=" + e.getMessage());
        }
    }

    public SparkArchive getCurrentArchive() {
        return currentArchive;
    }

    private static String getFileName(String delimiter, String path) {
        return path.substring(path.lastIndexOf(delimiter) + 1);
    }

    private static String unWrap(String prefix, String suffix, String fileName) {
        return fileName.substring(prefix.length(), fileName.length() - suffix.length());
    }

    private static String joinPrefix(String prefix, String fileName) {
        return prefix + fileName;
    }

    // eg:
    // __lib_979bfbcb9469888f1c5539e168d1925d_spark-2x.zip
    public static String assemblyFileName(String prefix, String md5sum, String fileName, String suffix) {
        return prefix + md5sum + FILE_NAME_SEPARATOR + fileName + suffix;
    }

    // eg:
    // .../__spark_repository__/__archive_1_0_0
    private String getRemoteArchivePath(String version) {
        return Joiner.on(PATH_DELIMITER).join(remoteRepositoryPath, joinPrefix(PREFIX_ARCHIVE, version));
    }

    public void readLock() {
        this.rwLock.readLock().lock();
    }

    public void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    public void writeLock() {
        this.rwLock.writeLock().lock();
    }

    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    // Represents a remote directory contains the uploaded libraries
    // an archive is named as __archive_{dppVersion}.
    // e.g. __archive_1_0_0/
    //        \ __lib_990325d2c0d1d5e45bf675e54e44fb16_spark-dpp.jar
    // *      \ __lib_7670c29daf535efe3c9b923f778f61fc_spark-2x.zip
    public static class SparkArchive {
        public String remotePath;
        public String version;
        public List<SparkLibrary> libraries;

        public SparkArchive(String remotePath, String version) {
            this.remotePath = remotePath;
            this.version = version;
            this.libraries = Lists.newArrayList();
        }
    }

    // Represents a uploaded remote file that save in the archive
    // a library refers to the dependency of DPP program or spark platform
    // named as __lib_{md5sum}_spark_{type}.{jar/zip}.
    // e.g. __lib_990325d2c0d1d5e45bf675e54e44fb16_spark-dpp.jar
    //      __lib_7670c29daf535efe3c9b923f778f61fc_spark-2x.zip
    public static class SparkLibrary {
        public String remotePath;
        public String md5sum;
        public long size;
        public LibType libType;

        public enum LibType {
            DPP, SPARK2X
        }

        public SparkLibrary(String remotePath, String md5sum, LibType libType, long size) {
            this.remotePath = remotePath;
            this.md5sum = md5sum;
            this.libType = libType;
            this.size = size;
        }
    }
}
