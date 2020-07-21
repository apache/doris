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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.thrift.TBrokerFileStatus;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/*
 * SparkRepository represents a remote repository for spark archives uploaded by spark
 * The organization in repository is:
 *
 * * __spark_repository__/
 *   * __archive_1_0_0/
 *     * __lib_990325d2c0d1d5e45bf675e54e44fb16_spark-dpp.zip
 *     * __lib_7670c29daf535efe3c9b923f778f61fc_spark-2x.zip
 *   * __archive_2_2_0/
 *     * __lib_64d5696f99c379af2bee28c1c84271d5_spark-dpp.zip
 *     * __lib_1bbb74bb6b264a270bc7fca3e964160f_spark-2x.zip
 *   * __archive_3_2_0/
 *     * ...
 */
public class SparkRepository {
    private static final Logger LOG = LogManager.getLogger(SparkRepository.class);

    public static final String PREFIX_ARCHIVE = "__archive_";
    public static final String PREFIX_LIB = "__lib_";
    public static final String SPARK_DPP = "spark-dpp";
    public static final String SPARK_2X = "spark-2x";
    public static final String SUFFIX_ZIP = ".zip";

    private static final String PATH_DELIMITER = "/";
    private static final String FILE_NAME_SEPARATOR = "_";

    private String remoteRepositoryPath;
    private BrokerDesc brokerDesc;
    private String currentDppVersion;

    private String localDppPath;
    private String localSpark2xPath;

    public SparkRepository(String remoteRepositoryPath, BrokerDesc brokerDesc, Map<String, String> properties) {
        this.remoteRepositoryPath = remoteRepositoryPath;
        this.brokerDesc = brokerDesc;
        currentDppVersion = FeConstants.spark_dpp_version;
        localDppPath = Config.spark_dpp_resource_local_path;
        localSpark2xPath = Config.spark_resource_path;
    }

    public void init() throws LoadException {
        boolean needUpload = false;
        boolean needReplace = false;
        CHECK: {
            if (!checkCurrentArchiveExists()) {
                needUpload = true;
                break CHECK;
            }

            List<SparkLibrary> libraries = Lists.newArrayList();
            getLibraries(getArchive(), libraries);
            if (libraries.size() != 2) {
                needUpload = true;
                needReplace = true;
                break CHECK;
            }

            for (SparkLibrary library : libraries) {
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
                }
            }
        }

        if (needUpload) {
            uploadArchive(needReplace);
        }
    }

    public boolean checkCurrentArchiveExists() {
        boolean result = false;
        String remotePath = getRemoteArchivePath(currentDppVersion);
        try {
            result = BrokerUtil.checkPathExist(remotePath, brokerDesc);
        } catch (UserException e) {
            LOG.warn("Failed to check remote archive exist, path={}, version={}", remotePath, currentDppVersion);
        }
        return result;
    }

//    public void listArchives(List<SparkArchive> archives) throws LoadException {
//        TPaloBrokerService.Client client = null;
//        TNetworkAddress address = null;
//        try {
//            Pair<TPaloBrokerService.Client, TNetworkAddress> pair = BrokerUtil.getBrokerAddressAndClient(brokerDesc);
//            client = pair.first;
//            address = pair.second;
//        } catch (UserException e) {
//            throw new LoadException(e.getMessage());
//        }
//        boolean failed = true;
//        try {
//            TBrokerListPathRequest request = new TBrokerListPathRequest(
//                    TBrokerVersion.VERSION_ONE, remoteRepositoryPath, false, brokerDesc.getProperties());
//            TBrokerListResponse response = client.listPath(request);
//            if (response.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
//                throw new LoadException("Broker list remote repository path failed. path=" + remoteRepositoryPath + " broker=" +
//                        BrokerUtil.printBroker(brokerDesc.getName(), address) + " message=" + response.getOpStatus().getMessage());
//            }
//            failed = false;
//            List<TBrokerFileStatus> fileStatus = response.getFiles();
//            for (TBrokerFileStatus tBrokerFileStatus : fileStatus) {
//                if (!tBrokerFileStatus.isDir) {
//                    continue;
//                }
//                String dirName = getFileName(PATH_DELIMITER, tBrokerFileStatus.path);
//                if (!dirName.startsWith(PREFIX_ARCHIVE)) {
//                    continue;
//                }
//                SparkArchive archive = new SparkArchive(tBrokerFileStatus.path);
//                archives.add(archive);
//            }
//        } catch (TException e) {
//            throw new LoadException("Broker list remote repository path failed. path=" + remoteRepositoryPath + ", broker=" +
//                    BrokerUtil.printBroker(brokerDesc.getName(), address));
//        } finally {
//            if (failed) {
//                if (failed) {
//                    ClientPool.brokerPool.invalidateObject(address, client);
//                } else {
//                    ClientPool.brokerPool.returnObject(address, client);
//                }
//            }
//        }
//    }

    public SparkArchive getArchive() {
        String remoteArchivePath = getRemoteArchivePath(currentDppVersion);
        SparkArchive archive = new SparkArchive(remoteArchivePath, currentDppVersion);
        return archive;
    }

    public void uploadArchive(boolean isReplace) throws LoadException {
        try {
            if (isReplace) {
                BrokerUtil.deletePath(getRemoteArchivePath(currentDppVersion), brokerDesc);
            }
            upload(localDppPath);
            upload(localSpark2xPath);
        } catch (UserException e) {
            throw new LoadException(e.getMessage());
        }
    }

    public void upload(String srcFilePath) throws LoadException {
        String md5sum = getMd5String(srcFilePath);
        String fileName = getFileName(PATH_DELIMITER, srcFilePath);
        String destFilePath = getRemoteArchivePath(currentDppVersion) +
                assemblyFileName(PREFIX_LIB, md5sum, fileName, null);
        try {
            BrokerUtil.writeFile(srcFilePath, destFilePath , brokerDesc);
        } catch (UserException e) {
            throw new LoadException("failed to upload lib to repository, srcPath=" +srcFilePath +
                    " destPath=" + destFilePath + " message=" + e.getMessage());
        }
    }

    public void getLibraries(SparkArchive archive, List<SparkLibrary> libraries) throws LoadException {
        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        try {
            BrokerUtil.parseFile(archive.remotePath, brokerDesc, fileStatuses);
        } catch (UserException e) {
            throw new LoadException(e.getMessage());
        }

        for (TBrokerFileStatus fileStatus : fileStatuses) {
            String fileName = getFileName(PATH_DELIMITER, fileStatus.path);
            if (!fileName.startsWith(PREFIX_LIB)) {
                continue;
            }
            String[] lib_arg = unWrap(PREFIX_LIB, SUFFIX_ZIP, fileName).split(FILE_NAME_SEPARATOR);
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
        }
    }

    public String getMd5String(String filePath) throws LoadException {
        File file = new File(filePath);
        String md5sum = null;
        try {
            md5sum = DigestUtils.md5Hex(new FileInputStream(file));
            Preconditions.checkNotNull(md5sum);
            return md5sum;
        } catch (FileNotFoundException e) {
            throw new LoadException("file " + filePath + "dose not exist");
        } catch (IOException e) {
            throw new LoadException("failed to get md5sum from file " + filePath);
        }
    }

    private static String getFileName(String delimiter, String path) {
        return path.substring(path.lastIndexOf(delimiter) + 1);
    }

    private static String disJoinPrefix(String prefix, String fileName) {
        return fileName.substring(prefix.length());
    }

    private static String disJoinSuffix(String suffix, String fileName) {
        return fileName.substring(fileName.length() - suffix.length());
    }

    private static String unWrap(String prefix, String suffix, String fileName) {
        return fileName.substring(prefix.length(), fileName.length() - suffix.length());
    }

    private static String joinPrefix(String prefix, String fileName) {
        return prefix + fileName;
    }

    private static String assemblyFileName(String prefix, String md5sum, String fileName, String suffix) {
        return prefix + md5sum + FILE_NAME_SEPARATOR + fileName + suffix;
    }

    private String getRemoteArchivePath(String version) {
        return Joiner.on(PATH_DELIMITER).join(remoteRepositoryPath, joinPrefix(PREFIX_ARCHIVE, version));
    }

    // Represents a remote directory contains the libraries which are in the same version
    public static class SparkArchive {
        private String remotePath;
        private String version;

        public SparkArchive(String remotePath, String version) {
            this.remotePath = remotePath;
            this.version = version;
        }
    }

    // Represents a remote library that Spark/Spark dpp rely on
    public static class SparkLibrary {
        private String remotePath;
        private String md5sum;
        private long size;
        private LibType libType;

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
