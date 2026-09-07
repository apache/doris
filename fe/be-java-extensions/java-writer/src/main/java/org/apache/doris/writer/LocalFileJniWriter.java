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

package org.apache.doris.writer;

import org.apache.doris.jni.spi.JniWriter;
import org.apache.doris.jni.spi.vec.VectorTable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Writes the blocks BE hands over to a local delimited file.
 *
 * <p>Reached as the {@code local-file} writer of the {@code java-writer} plugin; the class name
 * is private to the plugin, which is why the factory exists.
 */
public class LocalFileJniWriter extends JniWriter {
    private static final Logger LOG = Logger.getLogger(LocalFileJniWriter.class.getName());

    private final String filePath;
    private final String columnSeparator;
    private final String lineDelimiter;
    private BufferedWriter fileWriter;
    private long writtenRows = 0;
    private long writtenBytes = 0;

    public LocalFileJniWriter(int batchSize, Map<String, String> params) {
        super(batchSize, params);
        this.filePath = params.get("file_path");
        this.columnSeparator = params.getOrDefault("column_separator", ",");
        this.lineDelimiter = params.getOrDefault("line_delimiter", "\n");
    }

    @Override
    protected void openInternal() throws IOException {
        fileWriter = new BufferedWriter(new FileWriter(filePath));
        LOG.info(() -> "opened " + filePath);
    }

    @Override
    protected void writeInternal(VectorTable inputTable) throws IOException {
        int numRows = inputTable.getNumRows();
        int numCols = inputTable.getNumColumns();
        if (numRows == 0) {
            return;
        }

        Object[][] data = inputTable.getMaterializedData();
        StringBuilder sb = new StringBuilder();

        for (int row = 0; row < numRows; row++) {
            for (int col = 0; col < numCols; col++) {
                if (col > 0) {
                    sb.append(columnSeparator);
                }
                Object val = data[col][row];
                if (val != null) {
                    sb.append(val);
                } else {
                    sb.append("\\N");
                }
            }
            sb.append(lineDelimiter);
        }

        String output = sb.toString();
        fileWriter.write(output);
        writtenRows += numRows;
        writtenBytes += output.getBytes().length;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (fileWriter != null) {
            fileWriter.flush();
            fileWriter.close();
            fileWriter = null;
        }
        LOG.info(() -> "closed " + filePath + " after " + writtenRows + " rows, " + writtenBytes + " bytes");
    }

    @Override
    protected Map<String, String> collectStatistics() {
        Map<String, String> stats = new HashMap<>();
        stats.put("counter:WrittenRows", String.valueOf(writtenRows));
        stats.put("bytes:WrittenBytes", String.valueOf(writtenBytes));
        stats.put("timer:WriteTime", String.valueOf(writeTime));
        stats.put("timer:ReadTableTime", String.valueOf(readTableTime));
        return stats;
    }
}
