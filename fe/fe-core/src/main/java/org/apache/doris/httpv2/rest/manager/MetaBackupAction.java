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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.persist.CloudMetaSyncPoint;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.bdbje.BDBJEJournal;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.Storage;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.util.DbBackup;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.io.FileUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/rest/v2/manager/backup")
public class MetaBackupAction extends RestBaseController {
    private static final String ALLOW_REDIRECT = "allow_redirect";

    @PostMapping("/sync_cloud_meta")
    public Object syncCloudMeta(HttpServletRequest request, HttpServletResponse response) {
        if (!Config.isCloudMode()) {
            return ResponseEntityBuilder.okWithCommonError("/sync_cloud_meta only works on the cloud mode");
        }
        try {
            if (needRedirect(request.getScheme())) {
                return redirectToHttps(request);
            }
            executeCheckPassword(request, response);
            checkGlobalAuth(org.apache.doris.qe.ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
            Object redirectOrError = checkMasterAndRedirectIfNeeded(request, response);
            if (redirectOrError != null) {
                return redirectOrError;
            }

            synchronized (Env.getCurrentEnv().getEditLog()) {
                MetaSyncPointVersion syncVersion = createMetaSyncPoint();
                CloudMetaSyncPoint syncPoint = new CloudMetaSyncPoint(syncVersion.committedVersion,
                        syncVersion.versionStamp,
                        System.currentTimeMillis());
                long journalId = Env.getCurrentEnv().getEditLog().logMetaSyncPoint(syncPoint);

                Map<String, Object> data = new HashMap<>();
                data.put("journal_id", journalId);
                data.put("committed_version", syncVersion.committedVersion);
                data.put("versionstamp", syncVersion.versionStamp);
                return ResponseEntityBuilder.ok(data);
            }
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    @PostMapping("/export_meta")
    public Object exportMeta(@RequestBody ExportMetaRequest req,
            HttpServletRequest request, HttpServletResponse response) {
        if (!Config.isCloudMode()) {
            return ResponseEntityBuilder.okWithCommonError("/export_meta only works on the cloud mode");
        }
        try {
            if (needRedirect(request.getScheme())) {
                return redirectToHttps(request);
            }
            executeCheckPassword(request, response);
            checkGlobalAuth(org.apache.doris.qe.ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
            Object redirectOrError = checkMasterAndRedirectIfNeeded(request, response);
            if (redirectOrError != null) {
                return redirectOrError;
            }

            if (req == null || Strings.isNullOrEmpty(req.getTargetDir())) {
                return ResponseEntityBuilder.badRequest("target_dir is required");
            }
            File targetDir = prepareTargetDir(req.getTargetDir());
            if (Env.getCurrentEnv().getCheckpointer() != null) {
                Env.getCurrentEnv().getCheckpointer().getLock().readLock().lock();
            }
            try {
                CopiedImage copiedImage = copyLatestImageIfExists(targetDir);
                copyImageMetaFiles(targetDir);
                BdbExportResult bdbResult = exportBdbJe(targetDir, copiedImage.version, copiedImage.exists);

                Map<String, Object> data = new HashMap<>();
                data.put("target_dir", targetDir.getAbsolutePath());
                data.put("bdb_dir", new File(targetDir, "bdb").getAbsolutePath());
                data.put("bdb_file_count", bdbResult.fileCount);
                data.put("image_file", copiedImage.exists ? copiedImage.file.getName() : null);
                data.put("image_version", copiedImage.version);
                data.put("image_exported", copiedImage.exists);
                data.put("journal_upper_bound", bdbResult.journalUpperBound);
                return ResponseEntityBuilder.ok(data);
            } finally {
                if (Env.getCurrentEnv().getCheckpointer() != null) {
                    Env.getCurrentEnv().getCheckpointer().getLock().readLock().unlock();
                }
            }
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    private Object checkMasterAndRedirectIfNeeded(HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        if (Env.getCurrentEnv().isMaster()) {
            return null;
        }
        if (Boolean.parseBoolean(request.getParameter(ALLOW_REDIRECT))) {
            return redirectToMasterOrException(request, response);
        }
        return ResponseEntityBuilder.okWithCommonError(
                "current fe is not master, master is "
                        + Env.getCurrentEnv().getMasterHost() + ":" + Env.getCurrentEnv().getMasterHttpPort());
    }

    private MetaSyncPointVersion createMetaSyncPoint() throws DdlException {
        Cloud.CreateMetaSyncPointRequest req = Cloud.CreateMetaSyncPointRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .build();
        try {
            Cloud.CreateMetaSyncPointResponse resp = MetaServiceProxy.getInstance().createMetaSyncPoint(req);
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                throw new DdlException("create_meta_sync_point failed: " + resp.getStatus().getMsg());
            }
            if (!resp.hasCommittedVersion()) {
                throw new DdlException("meta service response missing committed_version");
            }
            if (!resp.hasVersionstamp() || Strings.isNullOrEmpty(resp.getVersionstamp())) {
                throw new DdlException("meta service response missing versionstamp");
            }
            return new MetaSyncPointVersion(resp.getCommittedVersion(), resp.getVersionstamp());
        } catch (RpcException e) {
            throw new DdlException("create_meta_sync_point rpc failed: " + e.getMessage());
        }
    }

    private static class MetaSyncPointVersion {
        private final long committedVersion;
        private final String versionStamp;

        MetaSyncPointVersion(long committedVersion, String versionStamp) {
            this.committedVersion = committedVersion;
            this.versionStamp = versionStamp;
        }
    }

    private static File prepareTargetDir(String targetDir) throws IOException {
        File dir = new File(targetDir).getCanonicalFile();
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new IOException("target_dir exists but is not a directory: " + dir.getAbsolutePath());
            }
            FileUtils.cleanDirectory(dir);
        } else {
            FileUtils.forceMkdir(dir);
        }
        return dir;
    }

    private BdbExportResult exportBdbJe(File targetDir, long imageVersion, boolean hasImage) throws Exception {
        if (!"bdb".equalsIgnoreCase(Config.edit_log_type)) {
            throw new DdlException("only bdb edit_log_type supports bdbje export");
        }
        Journal journal = Env.getCurrentEnv().getEditLog().getJournal();
        if (!(journal instanceof BDBJEJournal)) {
            throw new DdlException("current edit log is not BDBJEJournal");
        }

        BDBJEJournal bdbjeJournal = (BDBJEJournal) journal;
        if (bdbjeJournal.getBDBEnvironment() == null) {
            throw new DdlException("bdb environment is not initialized");
        }
        ReplicatedEnvironment replicatedEnvironment = bdbjeJournal.getBDBEnvironment().getReplicatedEnvironment();
        if (replicatedEnvironment == null) {
            throw new DdlException("bdb replicated environment is not ready");
        }

        File bdbTargetDir = new File(targetDir, "bdb");
        FileUtils.forceMkdir(bdbTargetDir);
        File bdbSourceDir = new File(Env.getCurrentEnv().getBdbDir());

        DbBackup backup = new DbBackup(replicatedEnvironment);
        backup.startBackup();
        try {
            long journalUpperBound = bdbjeJournal.getMaxJournalId();
            if (hasImage) {
                long journalMinId = bdbjeJournal.getMinJournalId();
                if (journalMinId > 0 && journalMinId > imageVersion + 1) {
                    throw new DdlException("export failed: bdb min journal id " + journalMinId
                            + " is greater than image_version + 1 (" + (imageVersion + 1) + ")");
                }
                if (journalUpperBound < imageVersion) {
                    throw new DdlException("export failed: bdb journal upper bound " + journalUpperBound
                            + " is smaller than image_version " + imageVersion);
                }
            }
            String[] files = backup.getLogFilesInBackupSet();
            for (String fileName : files) {
                FileUtils.copyFile(new File(bdbSourceDir, fileName), new File(bdbTargetDir, fileName));
            }
            return new BdbExportResult(files.length, journalUpperBound);
        } finally {
            backup.endBackup();
        }
    }

    private CopiedImage copyLatestImageIfExists(File targetDir) throws IOException {
        File imageTargetDir = new File(targetDir, "image");
        Storage storage = new Storage(Env.getServingEnv().getImageDir());
        long imageVersion = storage.getLatestImageSeq();
        File image = storage.getImageFile(imageVersion);
        if (!image.exists()) {
            return CopiedImage.notFound(imageVersion);
        }
        File targetImage = new File(imageTargetDir, image.getName());
        linkOrCopyFile(image, targetImage);
        return CopiedImage.found(targetImage, imageVersion);
    }

    private void copyImageMetaFiles(File targetDir) throws IOException {
        File imageTargetDir = new File(targetDir, "image");
        Storage storage = new Storage(Env.getServingEnv().getImageDir());
        File[] metaFiles = new File[] {
                storage.getModeFile(),
                storage.getRoleFile(),
                storage.getVersionFile()
        };
        for (File source : metaFiles) {
            if (!source.exists()) {
                continue;
            }
            linkOrCopyFile(source, new File(imageTargetDir, source.getName()));
        }
    }

    private void linkOrCopyFile(File source, File target) throws IOException {
        try {
            Files.createLink(target.toPath(), source.toPath());
        } catch (UnsupportedOperationException | SecurityException | FileAlreadyExistsException e) {
            FileUtils.copyFile(source, target);
        } catch (IOException e) {
            FileUtils.copyFile(source, target);
        }
    }

    private static class CopiedImage {
        private final File file;
        private final long version;
        private final boolean exists;

        CopiedImage(File file, long version, boolean exists) {
            this.file = file;
            this.version = version;
            this.exists = exists;
        }

        static CopiedImage found(File file, long version) {
            return new CopiedImage(file, version, true);
        }

        static CopiedImage notFound(long version) {
            return new CopiedImage(null, version, false);
        }
    }

    private static class BdbExportResult {
        private final int fileCount;
        private final long journalUpperBound;

        BdbExportResult(int fileCount, long journalUpperBound) {
            this.fileCount = fileCount;
            this.journalUpperBound = journalUpperBound;
        }
    }

    public static class ExportMetaRequest {
        @JsonAlias({"targetDir"})
        @JsonProperty("target_dir")
        private String targetDir;

        public String getTargetDir() {
            return targetDir;
        }

        public void setTargetDir(String targetDir) {
            this.targetDir = targetDir;
        }
    }
}
