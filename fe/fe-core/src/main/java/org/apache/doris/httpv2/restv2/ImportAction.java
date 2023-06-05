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

package org.apache.doris.httpv2.restv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.parquet.ParquetReader;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/rest/v2")
public class ImportAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(ImportAction.class);

    private static final long MAX_READ_LEN_BYTES = 1024 * 1024; // 1MB

    private static final String FORMAT_CSV = "CSV";
    private static final String FORMAT_PARQUET = "PARQUET";
    private static final String FORMAT_ORC = "ORC";

    private static final int MAX_SAMPLE_LINE = 50;

    /**
     * Request body:
     * {
     *  "fileInfo": {
     *      "columnSeparator": ",",
     *      "fileUrl": "hdfs://127.0.0.1:50070/file/test/text*",
     *      "format": "TXT" // TXT or PARQUET
     *  },
     *  "connectInfo": {  // Optional
     *      "brokerName" : "my_broker",
     *      "brokerProps" : {
     *          "username" : "yyy",
     *          "password" : "xxx"
     *      }
     *  }
     * }
     */
    @RequestMapping(path = "/api/import/file_review", method = RequestMethod.POST)
    public Object fileReview(@RequestBody FileReviewRequestVo body,
            HttpServletRequest request, HttpServletResponse response) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
        }

        FileInfo fileInfo = body.getFileInfo();
        ConnectInfo connectInfo = body.getConnectInfo();
        BrokerDesc brokerDesc = new BrokerDesc(connectInfo.getBrokerName(), connectInfo.getBrokerProps());

        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        try {
            // get file status
            BrokerUtil.parseFile(fileInfo.getFileUrl(), brokerDesc, fileStatuses);
            // create response
            FileReviewResponseVo reviewResponseVo = createFileReviewResponse(brokerDesc, fileInfo, fileStatuses);
            return ResponseEntityBuilder.ok(reviewResponseVo);
        } catch (UserException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    private FileReviewResponseVo createFileReviewResponse(BrokerDesc brokerDesc, FileInfo fileInfo,
                                                          List<TBrokerFileStatus> fileStatuses) throws UserException {
        FileReviewResponseVo responseVo = new FileReviewResponseVo();
        // set file review statistic
        FileReviewStatistic statistic = new FileReviewStatistic();
        statistic.setFileNumber(fileStatuses.size());
        long totalFileSize = 0;
        for (TBrokerFileStatus fStatus : fileStatuses) {
            if (fStatus.isDir) {
                throw new UserException("Not all matched paths are files: " + fStatus.path);
            }
            totalFileSize += fStatus.size;
        }
        statistic.setFileSize(totalFileSize);
        responseVo.setReviewStatistic(statistic);

        if (fileStatuses.isEmpty()) {
            return responseVo;
        }

        // Begin to preview first file.
        TBrokerFileStatus sampleFile = fileStatuses.get(0);
        FileSample fileSample = new FileSample();
        fileSample.setSampleFileName(sampleFile.path);

        if (fileInfo.format.equalsIgnoreCase(FORMAT_CSV)) {
            byte[] fileContentBytes = BrokerUtil.readFile(sampleFile.path, brokerDesc, MAX_READ_LEN_BYTES);
            parseContent(fileInfo.columnSeparator, "\n", fileContentBytes, fileSample);
        } else if (fileInfo.format.equalsIgnoreCase(FORMAT_PARQUET)) {
            try {
                ParquetReader parquetReader = ParquetReader.create(sampleFile.path, brokerDesc);
                parseParquet(parquetReader, fileSample);
            } catch (IOException e) {
                LOG.warn("failed to get sample data of parquet file: {}", sampleFile.path, e);
                throw new UserException("failed to get sample data of parquet file. " + e.getMessage());
            }
        } else {
            throw new UserException("Only support CSV or PARQUET file format");
        }

        responseVo.setFileSample(fileSample);
        return responseVo;
    }

    private void parseContent(String columnSeparator, String lineDelimiter, byte[] fileContentBytes,
                                            FileSample fileSample) {
        List<List<String>> sampleLines = Lists.newArrayList();
        int maxColSize = 0;
        String content = new String(fileContentBytes);
        String[] lines = content.split(lineDelimiter);
        for (String line : lines) {
            if (sampleLines.size() >= MAX_SAMPLE_LINE) {
                break;
            }
            String[] cols = line.split(columnSeparator);
            List<String> row = Lists.newArrayList(cols);
            sampleLines.add(row);
            maxColSize = Math.max(maxColSize, row.size());
        }

        fileSample.setFileLineNumber(sampleLines.size());
        fileSample.setMaxColumnSize(maxColSize);
        fileSample.setSampleFileLines(sampleLines);
        return;
    }

    private void parseParquet(ParquetReader reader, FileSample fileSample) throws IOException {
        fileSample.setColNames(reader.getSchema(false));
        fileSample.setMaxColumnSize(fileSample.colNames.size());
        fileSample.setSampleFileLines(reader.getLines(MAX_SAMPLE_LINE));
        fileSample.setFileLineNumber(fileSample.sampleFileLines.size());
    }

    @Getter
    @Setter
    public static class FileReviewRequestVo {
        private FileInfo fileInfo;
        private ConnectInfo connectInfo;
    }

    @Getter
    @Setter
    public static class FileInfo {
        private String columnSeparator;
        private String fileUrl;
        private String format;
    }

    @Getter
    @Setter
    public static class ConnectInfo {
        private String brokerName;
        private Map<String, String> brokerProps;
    }

    @Getter
    @Setter
    public static class FileReviewResponseVo {
        private FileReviewStatistic reviewStatistic;
        private FileSample fileSample;
    }

    @Getter
    @Setter
    public static class FileReviewStatistic {
        private int fileNumber;
        private long fileSize;
    }

    @Getter
    @Setter
    public static class FileSample {
        private String sampleFileName;
        private int fileLineNumber;
        private int maxColumnSize;
        private List<String> colNames;
        private List<List<String>> sampleFileLines;
    }
}
