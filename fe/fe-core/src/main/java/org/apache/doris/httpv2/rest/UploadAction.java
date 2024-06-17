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

package org.apache.doris.httpv2.rest;

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.util.LoadSubmitter;
import org.apache.doris.httpv2.util.TmpFileMgr;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Upload file
 */
@RestController
public class UploadAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(UploadAction.class);
    private static TmpFileMgr fileMgr = new TmpFileMgr(Config.tmp_dir);
    private static LoadSubmitter loadSubmitter = new LoadSubmitter();

    private static final String PARAM_COLUMN_SEPARATOR = "column_separator";
    private static final String PARAM_PREVIEW = "preview";
    private static final String PARAM_FILE_ID = "file_id";
    private static final String PARAM_FILE_UUID = "file_uuid";

    /**
     * Upload the file
     * @param ns
     * @param dbName
     * @param tblName
     * @param file
     * @param request
     * @param response
     * @return
     */
    @RequestMapping(path = "/api/{" + NS_KEY + "}/{" + DB_KEY + "}/{" + TABLE_KEY + "}/upload",
            method = {RequestMethod.POST})
    public Object upload(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tblName,
            @RequestParam("file") MultipartFile file,
            HttpServletRequest request, HttpServletResponse response) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        String fullDbName = getFullDbName(dbName);
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.LOAD);

        String columnSeparator = request.getParameter(PARAM_COLUMN_SEPARATOR);
        if (Strings.isNullOrEmpty(columnSeparator)) {
            columnSeparator = "\t";
        }

        String preview = request.getParameter(PARAM_PREVIEW);
        if (Strings.isNullOrEmpty(preview)) {
            preview = "false"; // default is false
        }

        if (file.isEmpty()) {
            return ResponseEntityBuilder.badRequest("Empty file");
        }

        try {
            TmpFileMgr.TmpFile tmpFile = fileMgr.upload(new TmpFileMgr.UploadFile(file, columnSeparator));
            TmpFileMgr.TmpFile copiedFile = tmpFile.copy();
            if (preview.equalsIgnoreCase("true")) {
                copiedFile.setPreview();
            }
            return ResponseEntityBuilder.ok(copiedFile);
        } catch (TmpFileMgr.TmpFileException | IOException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    /**
     * Load the uploaded file
     * @param ns
     * @param dbName
     * @param tblName
     * @param request
     * @param response
     * @return
     */
    @RequestMapping(path = "/api/{" + NS_KEY + "}/{" + DB_KEY + "}/{" + TABLE_KEY + "}/upload",
            method = {RequestMethod.PUT})
    public Object submit(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tblName,
            HttpServletRequest request, HttpServletResponse response) {

        // This is a strict restriction
        if (!Strings.isNullOrEmpty(Config.security_checker_class_name)) {
            return ResponseEntityBuilder.badRequest("Not support upload data api in security env");
        }

        ActionAuthorizationInfo authInfo = checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        String fullDbName = getFullDbName(dbName);
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.LOAD);

        String fileIdStr = request.getParameter(PARAM_FILE_ID);
        if (Strings.isNullOrEmpty(fileIdStr)) {
            return ResponseEntityBuilder.badRequest("Missing file id parameter");
        }
        String fileUUIDStr = request.getParameter(PARAM_FILE_UUID);
        if (Strings.isNullOrEmpty(fileUUIDStr)) {
            return ResponseEntityBuilder.badRequest("Missing file id parameter");
        }

        TmpFileMgr.TmpFile tmpFile = null;
        try {
            tmpFile = fileMgr.getFile(Long.valueOf(fileIdStr), fileUUIDStr);
        } catch (TmpFileMgr.TmpFileException e) {
            return ResponseEntityBuilder.okWithCommonError("file not found");
        }
        Preconditions.checkNotNull(tmpFile, fileIdStr);

        LoadContext loadContext = new LoadContext(request, dbName, tblName,
                authInfo.fullUserName, authInfo.password, tmpFile);
        Future<LoadSubmitter.SubmitResult> future = loadSubmitter.submit(loadContext);

        try {
            LoadSubmitter.SubmitResult res = future.get();
            return ResponseEntityBuilder.ok(res);
        } catch (InterruptedException | ExecutionException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    /**
     * Get all uploaded file or specified file
     * If preview is true, also return the preview of the file
     * @param ns
     * @param dbName
     * @param tblName
     * @param request
     * @param response
     * @return
     */
    @RequestMapping(path = "/api/{" + NS_KEY + "}/{" + DB_KEY + "}/{" + TABLE_KEY + "}/upload",
            method = {RequestMethod.GET})
    public Object list(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tblName,
            HttpServletRequest request, HttpServletResponse response) {

        checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        String fullDbName = getFullDbName(dbName);
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.LOAD);

        String fileIdStr = request.getParameter(PARAM_FILE_ID);
        String fileUUIDStr = request.getParameter(PARAM_FILE_UUID);

        if (Strings.isNullOrEmpty(fileIdStr) || Strings.isNullOrEmpty(fileUUIDStr)) {
            // not specified file id, return all files list
            List<TmpFileMgr.TmpFileBrief> files = fileMgr.listFiles();
            return ResponseEntityBuilder.ok(files);
        }

        // return specified file
        String preview = request.getParameter(PARAM_PREVIEW);
        if (Strings.isNullOrEmpty(preview)) {
            preview = "true"; // default is true
        }

        try {
            TmpFileMgr.TmpFile tmpFile = fileMgr.getFile(Long.valueOf(fileIdStr), fileUUIDStr);
            TmpFileMgr.TmpFile copiedFile = tmpFile.copy();
            if (preview.equalsIgnoreCase("true")) {
                copiedFile.setPreview();
            }
            return ResponseEntityBuilder.ok(copiedFile);
        } catch (TmpFileMgr.TmpFileException | IOException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/{" + NS_KEY + "}/{" + DB_KEY + "}/{" + TABLE_KEY + "}/upload",
            method = {RequestMethod.DELETE})
    public Object delete(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tblName,
            HttpServletRequest request, HttpServletResponse response) {

        checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        String fullDbName = getFullDbName(dbName);
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.LOAD);

        String fileIdStr = request.getParameter(PARAM_FILE_ID);
        if (Strings.isNullOrEmpty(fileIdStr)) {
            return ResponseEntityBuilder.badRequest("Missing file id parameter");
        }
        String fileUUIDStr = request.getParameter(PARAM_FILE_UUID);
        if (Strings.isNullOrEmpty(fileUUIDStr)) {
            return ResponseEntityBuilder.badRequest("Missing file id parameter");
        }

        fileMgr.deleteFile(Long.valueOf(fileIdStr), fileUUIDStr);
        return ResponseEntityBuilder.ok();
    }

    /**
     * A context to save infos of stream load
     */
    public static class LoadContext {
        public String user;
        public String passwd;
        public String db;
        public String tbl;
        public TmpFileMgr.TmpFile file;

        public String label;
        public String columnSeparator;
        public String columns;
        public String where;
        public String maxFilterRatio;
        public String partitions;
        public String timeout;
        public String strictMode;
        public String timezone;
        public String execMemLimit;
        public String format;
        public String jsonPaths;
        public String stripOuterArray;
        public String jsonRoot;
        public String numAsString;
        public String fuzzyParse;


        public LoadContext(HttpServletRequest request, String db,
                String tbl, String user, String passwd, TmpFileMgr.TmpFile file) {
            this.db = db;
            this.tbl = tbl;
            this.user = user;
            this.passwd = passwd;
            this.file = file;

            parseHeader(request);
        }

        private void parseHeader(HttpServletRequest request) {
            this.label = request.getHeader("label");
            this.columnSeparator = file.columnSeparator;
            if (!Strings.isNullOrEmpty(request.getHeader("column_separator"))) {
                this.columnSeparator = request.getHeader("column_separator");
            }
            this.columns = request.getHeader("columns");
            this.where = request.getHeader("where");
            this.maxFilterRatio = request.getHeader("max_filter_ratio");
            this.partitions = request.getHeader("partitions");
            this.timeout = request.getHeader("timeout");
            this.strictMode = request.getHeader("strict_mode");
            this.timezone = request.getHeader("timezone");
            this.execMemLimit = request.getHeader("exec_mem_limit");
            this.format = request.getHeader("format");
            this.jsonPaths = request.getHeader("jsonpaths");
            this.stripOuterArray = request.getHeader("strip_outer_array");
            this.numAsString = request.getHeader("num_as_string");
            this.jsonRoot = request.getHeader("json_root");
            this.fuzzyParse = request.getHeader("fuzzy_parse");
        }
    }
}
