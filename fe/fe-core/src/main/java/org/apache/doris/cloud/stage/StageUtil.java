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

package org.apache.doris.cloud.stage;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud.FinishCopyRequest.Action;
import org.apache.doris.cloud.proto.Cloud.ObjectFilePB;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.storage.ListObjectsResult;
import org.apache.doris.cloud.storage.ObjectFile;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileCompressType;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.GlobExpander;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StageUtil {
    public static final Logger LOG = LogManager.getLogger(StageUtil.class);
    private static final String NO_FILES_ERROR_MSG = "No files can be copied";

    public static StagePB getStage(String stage, String user, boolean checkAuth)
            throws AnalysisException, DdlException {
        if (stage.equals("~")) {
            if (StringUtils.isEmpty(user)) {
                throw new AnalysisException("User name can not be empty");
            }
            String userId = Env.getCurrentEnv().getAuth().getUserId(user);
            List<StagePB> stagePBs = ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                                            .getStage(StageType.INTERNAL, user, null, userId);
            if (stagePBs == null || stagePBs.isEmpty()) {
                throw new AnalysisException("Failed to get internal stage for user: " + user + ", userId: " + userId);
            }
            return stagePBs.get(0);
        } else {
            // check stage permission
            if (checkAuth && !Env.getCurrentEnv().getAccessManager()
                    .checkCloudPriv(ConnectContext.get().getCurrentUserIdentity(), stage, PrivPredicate.USAGE,
                            ResourceTypeEnum.STAGE)) {
                throw new AnalysisException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                        + "'@'" + ConnectContext.get().getRemoteIP() + "' for cloud stage '" + stage + "'");
            }
            List<StagePB> stagePBs = ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                                            .getStage(StageType.EXTERNAL, null, stage, null);
            if (stagePBs.isEmpty()) {
                throw new AnalysisException("Failed to get external stage with name: " + stage);
            }
            return stagePBs.get(0);
        }
    }

    public static List<TBrokerFileStatus> beginCopy(String stageId, StageType stageType, ObjectInfo objectInfo,
            long tableId, String copyId, String filePattern, long sizeLimit, long timeout) throws Exception {
        List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
        Triple<Integer, Integer, String> triple = listAndFilterFilesV2(
                objectInfo, filePattern, copyId, stageId, tableId, false, sizeLimit,
                Config.max_file_num_per_copy_into_job, Config.max_meta_size_per_copy_into_job, fileStatus);
        // should return if file status is 0

        long matchedFileNum = triple.getLeft();
        long loadedFileNum = triple.getMiddle();
        String reachLimitStr = triple.getRight();
        List<ObjectFilePB> objectFiles = fileStatus.stream().map(p -> p.second).collect(Collectors.toList());
        long startTime = System.currentTimeMillis();
        long timeoutTime = startTime + timeout + 5000;
        List<ObjectFilePB> filteredObjectFiles = ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                .beginCopy(stageId, stageType, tableId, copyId, 0, startTime, timeoutTime, objectFiles, sizeLimit,
                Config.max_file_num_per_copy_into_job, Config.max_meta_size_per_copy_into_job);
        if (filteredObjectFiles.isEmpty()) {
            LOG.warn(NO_FILES_ERROR_MSG + ", matched {} files, filtered {} files "
                            + "because files may be loading or loaded" + reachLimitStr
                            + ", {} files left after beginCopy, queryId={}", matchedFileNum, loadedFileNum, 0,
                    copyId);
            throw new UserException(String.format(NO_FILES_ERROR_MSG + ", matched %d files, "
                    + "filtered %d files because files may be loading or loaded" + reachLimitStr
                    + ", %d files left after beginCopy", matchedFileNum, loadedFileNum, 0));
        }
        Set<String> set = filteredObjectFiles.stream().map(f -> getFileInfoUniqueId(f)).collect(Collectors.toSet());
        List<TBrokerFileStatus> brokerFiles = Lists.newArrayList();
        for (Pair<TBrokerFileStatus, ObjectFilePB> pair : fileStatus) {
            if (set.contains(getFileInfoUniqueId(pair.second))) {
                brokerFiles.add(pair.first);
            }
        }
        return brokerFiles;
    }

    public static void finishCopy(String stageId, StageType stageType, long tableId, String copyId, boolean success)
            throws Exception {
        ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                .finishCopy(stageId, stageType, tableId, copyId, 0, success ? Action.COMMIT : Action.ABORT);
    }

    /*
     * @return <matchedFileNum, loadedFileNum, reachLimitStr>.
     * matchedFileNum: how many files the file pattern matched
     * loadedFileNum: how many files are already loaded
     * reachLimitStr: explain the reason of stop list and select files
     */
    public static Triple<Integer, Integer, String> listAndFilterFilesV2(ObjectInfo objectInfo, String pattern,
            String copyId, String stageId, long tableId, boolean force, long sizeLimit, int fileNum, int metaSizeLimit,
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus) throws Exception {
        long startTimestamp = System.currentTimeMillis();
        long listFileNum = 0;
        int matchedFileNum = 0;
        int loadedFileNum = 0;
        String reachLimitStr = "";
        RemoteBase remote = RemoteBase.newInstance(objectInfo);

        List<Pair<String, Boolean>> globs = analyzeGlob(copyId, pattern);
        LOG.info("Input copy into glob={}, analyzed={}", pattern, globs);
        try {
            PathMatcher matcher = getPathMatcher(pattern);
            boolean finish = false;
            List<ObjectFile> matchPatternFiles = new ArrayList<>();
            for (int i = 0; i < globs.size() && !finish; i++) {
                Pair<String, Boolean> glob = globs.get(i);
                String continuationToken = null;
                while (!finish) {
                    ListObjectsResult listObjectsResult;
                    if (glob.second) {
                        // list objects with sub prefix
                        listObjectsResult = remote.listObjects(glob.first, continuationToken);
                    } else {
                        // head object
                        listObjectsResult = remote.headObject(glob.first);
                    }
                    listFileNum += listObjectsResult.getObjectInfoList().size();
                    long costSeconds = (System.currentTimeMillis() - startTimestamp) / 1000;
                    if (costSeconds >= 3600 || listFileNum >= 1000000) {
                        throw new DdlException("Abort list object for copyId=" + copyId
                                + ". We don't collect enough files to load, after listing " + listFileNum
                                + " objects for " + costSeconds + " seconds, please check if your pattern " + pattern
                                + " is correct.");
                    }
                    // 1. check if pattern is matched
                    for (ObjectFile objectFile : listObjectsResult.getObjectInfoList()) {
                        if (!matchPattern(objectFile.getRelativePath(), matcher)) {
                            LOG.info("not matchPattern path:{}", objectFile.getRelativePath());
                            continue;
                        }
                        matchPatternFiles.add(objectFile);
                        // 2. filter files which are not copying or copied by other copy jobs
                        if (matchPatternFiles.size() >= Config.cloud_filter_copy_file_num_limit) {
                            List<ObjectFilePB> filterCopyFiles = filterCopyFiles(stageId, tableId, force,
                                    matchPatternFiles);
                            fileStatus.addAll(
                                    generateFiles(filterCopyFiles, matchPatternFiles, objectInfo.getBucket()));
                            matchPatternFiles.clear();
                            // 3. check if reach any limit of fileNum/fileSize/fileMetaSize if select more than 1 file
                            if (reachLimit(fileStatus, sizeLimit, fileNum, metaSizeLimit, reachLimitStr)) {
                                finish = true;
                                break;
                            }
                        }
                    }
                    if (!listObjectsResult.isTruncated()) {
                        break;
                    }
                    continuationToken = listObjectsResult.getContinuationToken();
                }
            }
            if (!matchPatternFiles.isEmpty()) {
                List<ObjectFilePB> filterCopyFiles = filterCopyFiles(stageId, tableId, force, matchPatternFiles);
                fileStatus.addAll(generateFiles(filterCopyFiles, matchPatternFiles, objectInfo.getBucket()));
                matchPatternFiles.clear();
            }
        } finally {
            remote.close();
        }
        return Triple.of(matchedFileNum, loadedFileNum, reachLimitStr);
    }

    private static boolean reachLimit(List<Pair<TBrokerFileStatus, ObjectFilePB>> objectFiles, long sizeLimit,
            int fileNum, int metaSizeLimit, String reachLimitStr) {
        if (objectFiles.size() == 0) {
            return false;
        }
        if (fileNum > 0 && objectFiles.size() >= fileNum) {
            reachLimitStr = ", reach num limit: " + fileNum;
            LOG.info("reach file num limit fileNum:{} objectFiles.size:{}", fileNum, objectFiles.size());
            return true;
        }

        long objectFilesSize = objectFiles.stream().mapToLong(f -> f.first.getSize()).sum();
        if (sizeLimit > 0 && objectFilesSize >= sizeLimit) {
            reachLimitStr = ", reach size limit: " + sizeLimit;
            LOG.info("reach size limit sizeLimit:{}, objectFilesSize:{}", sizeLimit, objectFilesSize);
            return true;
        }

        long objectFilesSerializedSize = objectFiles.stream().mapToLong(f -> f.second.getSerializedSize()).sum();
        if (metaSizeLimit > 0 && objectFilesSerializedSize >= metaSizeLimit) {
            reachLimitStr = ", reach meta size limit: " + metaSizeLimit;
            LOG.info("reach meta size limit metaSizeLimit:{} objectFilesSerializedSize:{}",
                    metaSizeLimit, objectFilesSerializedSize);
            return true;
        }
        return false;
    }

    private static List<ObjectFilePB> filterCopyFiles(String stageId, long tableId, boolean force,
            List<ObjectFile> objectFiles) throws DdlException {
        if (force) {
            return objectFiles.stream()
                    .map(f -> ObjectFilePB.newBuilder().setRelativePath(f.getRelativePath()).setEtag(f.getEtag())
                            .setSize(f.getSize()).build()).collect(Collectors.toList());
        }
        return objectFiles.size() > 0
                ? ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                            .filterCopyFiles(stageId, tableId, objectFiles)
                : new ArrayList<>();
    }

    public static PathMatcher getPathMatcher(String pattern) {
        return pattern == null ? null : FileSystems.getDefault().getPathMatcher("glob:" + pattern);
    }

    public static boolean matchPattern(String key, PathMatcher matcher) {
        if (matcher == null) {
            return true;
        }
        Path path = Paths.get(key);
        return matcher.matches(path);
    }

    public static String getFileInfoUniqueId(ObjectFile objectFile) {
        return objectFile.getRelativePath() + "_" + objectFile.getEtag();
    }

    public static String getFileInfoUniqueId(ObjectFilePB objectFile) {
        return objectFile.getRelativePath() + "_" + objectFile.getEtag();
    }

    /*
     * @return the list of analyzed sub glob which is a pair prefix and containsWildcard
     */
    public static List<Pair<String, Boolean>> analyzeGlob(String queryId, String glob) throws DdlException {
        List<Pair<String, Boolean>> globs = new ArrayList<>();
        if (glob == null) {
            globs.add(Pair.of("", true));
            return globs;
        }
        try {
            List<String> flattenedPatterns = GlobExpander.expand(glob);
            for (String flattenedPattern : flattenedPatterns) {
                try {
                    globs.addAll(analyzeFlattenedPattern(flattenedPattern));
                } catch (Exception e) {
                    LOG.warn("Failed to analyze flattenedPattern: {}, glob: {}, queryId: {}", flattenedPattern, glob,
                            queryId, e);
                    throw e;
                }
            }
            return globs;
        } catch (Exception e) {
            LOG.warn("Failed to analyze glob: {}, queryId: {}", glob, queryId, e);
            throw new DdlException("Failed to analyze glob: " + glob + ", queryId: " + queryId + ", " + e.getMessage());
        }
    }

    private static List<Pair<String, Boolean>> analyzeFlattenedPattern(String flattenedPattern) throws IOException {
        List<String> components = getPathComponents(flattenedPattern);
        LOG.debug("flattenedPattern={}, components={}", flattenedPattern, components);
        String prefix = "";
        boolean sawWildcard = false;
        for (int componentIdx = 0; componentIdx < components.size(); componentIdx++) {
            String component = components.get(componentIdx);
            GlobFilter globFilter = new GlobFilter(component);
            if (globFilter.hasPattern()) {
                if (componentIdx == components.size() - 1) {
                    List<Pair<String, Boolean>> pairs = analyzeLastComponent(component);
                    if (pairs != null) {
                        List<Pair<String, Boolean>> results = new ArrayList<>();
                        for (Pair<String, Boolean> pair : pairs) {
                            results.add(Pair.of((prefix.isEmpty() ? "" : prefix + "/") + pair.first, pair.second));
                        }
                        return results;
                    }
                }
                sawWildcard = true;
                String componentPrefix = getComponentPrefix(component);
                prefix += (prefix.isEmpty() ? "" : "/") + componentPrefix;
                break;
            } else {
                String unescapedComponent = unescapePathComponent(component);
                prefix += (prefix.isEmpty() ? "" : "/") + unescapedComponent;
            }
        }
        return Lists.newArrayList(Pair.of(prefix, sawWildcard));
    }

    private static List<Pair<String, Boolean>> analyzeLastComponent(String component) throws IOException {
        if (component.startsWith("{") && component.endsWith("}")) {
            List<Pair<String, Boolean>> results = new ArrayList<>();
            String sub = component.substring(1, component.length() - 1);
            List<String> splits = splitByComma(sub);
            for (String split : splits) {
                GlobFilter globFilter = new GlobFilter(split);
                if (globFilter.hasPattern()) {
                    results.add(Pair.of(getComponentPrefix(split), true));
                } else {
                    results.add(Pair.of(unescapePathComponent(split), false));
                }
            }
            return results;
        }
        return null;
    }

    private static List<String> splitByComma(String str) {
        List<String> values = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ',') {
                if (isBackslash(str, i - 1)) {
                    continue;
                } else {
                    values.add(replaceBackslashComma(str.substring(start, i)));
                    start = i + 1;
                }
            }
        }
        if (start != str.length() - 1) {
            values.add(replaceBackslashComma(str.substring(start)));
        }
        return values;
    }

    private static String replaceBackslashComma(String value) {
        return value.replaceAll("\\\\,", ",");
    }

    private static boolean isBackslash(String str, int index) {
        if (index >= 0 && index < str.length()) {
            return str.charAt(index) == '\\';
        }
        return false;
    }

    private static String getComponentPrefix(String component) {
        int index = 0;
        // special characters: * ? [] {} ,(in {}) -(in [])
        for (int i = 0; i < component.length(); i++) {
            char ch = component.charAt(i);
            if (ch == '*' || ch == '?' || ch == '[' || ch == '{' || ch == '\\') {
                index = i;
                break;
            }
        }
        return component.substring(0, index);
    }

    /*
     * Glob process method are referenced from {@link org.apache.hadoop.fs.Globber}
     */
    private static String unescapePathComponent(String name) {
        return name.replaceAll("\\\\(.)", "$1");
    }

    private static List<String> getPathComponents(String path) {
        ArrayList<String> ret = new ArrayList<>();
        for (String component : path.split(org.apache.hadoop.fs.Path.SEPARATOR)) {
            if (!component.isEmpty()) {
                ret.add(component);
            }
        }
        return ret;
    }

    public static TFileCompressType parseCompressType(String compress) {
        if (StringUtils.isEmpty(compress)) {
            return TFileCompressType.PLAIN;
        } else if (compress.equalsIgnoreCase("gz")) {
            return TFileCompressType.GZ;
        } else if (compress.equalsIgnoreCase("bz2")) {
            return TFileCompressType.BZ2;
        } else if (compress.equalsIgnoreCase("lz4")) {
            return TFileCompressType.LZ4FRAME;
        } else if (compress.equalsIgnoreCase("lzo")) {
            return TFileCompressType.LZO;
        } else if (compress.equalsIgnoreCase("deflate")) {
            return TFileCompressType.DEFLATE;
        }
        return TFileCompressType.UNKNOWN;
    }

    private static List<Pair<TBrokerFileStatus, ObjectFilePB>> generateFiles(List<ObjectFilePB> filterFiles,
            List<ObjectFile> allFiles, String bucket) {
        List<Pair<TBrokerFileStatus, ObjectFilePB>> files = new ArrayList<>();
        Map<String, ObjectFilePB> fileMap = new HashMap<>();
        for (ObjectFilePB filterFile : filterFiles) {
            fileMap.put(getFileInfoUniqueId(filterFile), filterFile);
        }
        for (ObjectFile file : allFiles) {
            if (fileMap.containsKey(getFileInfoUniqueId(file))) {
                String objUrl = "s3://" + bucket + "/" + file.getKey();
                files.add(Pair.of(new TBrokerFileStatus(objUrl, false, file.getSize(), true),
                        fileMap.get(getFileInfoUniqueId(file))));
            }
        }
        return files;
    }

    public static List<String> parseLoadFiles(List<String> loadFiles, String bucket, String stagePrefix) {
        if (!Config.cloud_delete_loaded_internal_stage_files || loadFiles == null || !RemoteBase.checkStagePrefix(
                stagePrefix)) {
            return null;
        }
        String prefix = "s3://" + bucket + "/";
        List<String> parsedFiles = new ArrayList<>();
        for (String loadFile : loadFiles) {
            if (!loadFile.startsWith(prefix)) {
                LOG.warn("load file={} is not start with {}", loadFile, prefix);
                return null;
            }
            String key = loadFile.substring(prefix.length());
            if (!key.startsWith(stagePrefix)) {
                LOG.warn("load file={} is not start with {}", loadFile, stagePrefix);
                return null;
            }
            parsedFiles.add(key);
        }
        return parsedFiles;
    }
}
