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

package org.apache.doris.stack.connector;

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.palo.HdfsFilePreview;
import org.apache.doris.stack.model.palo.HdfsFilePreviewReq;
import org.apache.doris.stack.model.palo.LocalFileInfo;
import org.apache.doris.stack.model.palo.LocalFileSubmitResult;
import org.apache.doris.stack.model.palo.PaloResponseEntity;
import org.apache.doris.stack.model.request.construct.FileImportReq;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.exception.HdfsUnknownHostException;
import org.apache.doris.stack.exception.HdfsUrlException;
import org.apache.doris.stack.exception.PaloRequestException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@Component
@Slf4j
public class PaloFileUploadClient extends PaloClient {

    protected HttpClientPoolManager poolManager;

    @Autowired
    public PaloFileUploadClient(HttpClientPoolManager poolManager) {
        this.poolManager = poolManager;
    }

    public LocalFileInfo uploadLocalFile(String ns, String db, String table,
                                         MultipartFile file, Map<String, String> otherParams,
                                         ClusterInfoEntity entity, String contentType) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/api/");
        buffer.append(ns);
        buffer.append("/");
        buffer.append(db);
        buffer.append("/");
        buffer.append(table);
        buffer.append("/upload");
        url = buffer.toString();

        log.debug("Send upload local file request, url is {}.", url);
        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        headers.put("Content-Type", contentType);
        String[] array = contentType.split(";");
        String[] boundary = array[1].split("=");
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());

        PaloResponseEntity response = poolManager.uploadFile(url, file, headers, otherParams, boundary[1]);

        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            log.error("file upload error:" + response.getData());
            throw new PaloRequestException("file upload error:" + response.getData());
        }

        return JSON.parseObject(response.getData(), LocalFileInfo.class);
    }

    public LocalFileSubmitResult submitFileImport(String ns, String db, String table, FileImportReq importReq,
                                                  ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/api/");
        buffer.append(ns);
        buffer.append("/");
        buffer.append(db);
        buffer.append("/");
        buffer.append(table);
        buffer.append("/upload");
        buffer.append("?file_id=");
        buffer.append(importReq.getFileId());
        buffer.append("&file_uuid=");
        buffer.append(importReq.getFileUuid());
        url = buffer.toString();
        log.debug("Send submit import local file request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setPostHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());
        headers.put("label", importReq.getName());

        StringBuffer columnNameBuffer = new StringBuffer();
        for (String columnName : importReq.getColumnNames()) {
            columnNameBuffer.append(columnName);
            columnNameBuffer.append(",");
        }
        columnNameBuffer.deleteCharAt(columnNameBuffer.length() - 1);
        headers.put("columns", columnNameBuffer.toString());
        headers.put("column_separator", ",");

        PaloResponseEntity response = poolManager.doPut(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            log.error("import file error:" + response.getData());
            throw new PaloRequestException("import file error:" + response.getData());
        }

        LocalFileSubmitResult submitResult = JSON.parseObject(response.getData(), LocalFileSubmitResult.class);
        return submitResult;
    }

    public void deleteLocalFile(String ns, String db, String table, int fileId, String fileUuid,
                                ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/api/");
        buffer.append(ns);
        buffer.append("/");
        buffer.append(db);
        buffer.append("/");
        buffer.append(table);
        buffer.append("/upload");
        buffer.append("?file_id=");
        buffer.append(fileId);
        buffer.append("&file_uuid=");
        buffer.append(fileUuid);
        url = buffer.toString();
        log.debug("Send delete local file request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());
        PaloResponseEntity response = poolManager.doDelete(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            log.error("delete file error:" + response.getData());
            throw new PaloRequestException("delete file error:" + response.getData());
        }
    }

    public HdfsFilePreview getHdfsPreview(HdfsFilePreviewReq req, ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/rest/v2/api/import/file_review");
        url = buffer.toString();
        log.debug("get hdfs preview request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setPostHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());

        PaloResponseEntity response = poolManager.doPost(url, headers, req);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            if (response.getData().indexOf("java.net.UnknownHostException") > 0
                    || response.getData().indexOf("java.net.ConnectException") > 0) {
                log.error("Hdfs host or port is error.");
                throw new HdfsUnknownHostException();
            } else if (response.getData().indexOf("Wrong FS") > 0) {
                log.error("Hdfs URL is error.");
                throw new HdfsUrlException();
            } else {
                log.error("get hdfs preview error: {}.", response.getData());
                throw new PaloRequestException("get hdfs preview error:" + response.getData());
            }
        }

        return JSON.parseObject(response.getData(), HdfsFilePreview.class);
    }
}
