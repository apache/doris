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

package org.apache.doris.common.parquet;

import org.apache.doris.analysis.BrokerDesc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ParquetReader {

    private ParquetFileReader fileReader;

    private ParquetReader(InputFile inputFile) throws IOException {
        ParquetReadOptions readOptions = ParquetReadOptions.builder().build();
        this.fileReader = new ParquetFileReader(inputFile, readOptions);
    }

    public static ParquetReader create(String filePath, BrokerDesc brokerDesc) throws IOException {
        BrokerInputFile inputFile = BrokerInputFile.create(filePath, brokerDesc);
        return new ParquetReader(inputFile);
    }

    public static ParquetReader create(String filePath) throws IOException {
        LocalInputFile inputFile = new LocalInputFile(new File(filePath));
        return new ParquetReader(inputFile);
    }

    // For test only. ip port is broker ip port
    public static ParquetReader create(String filePath, BrokerDesc brokerDesc, String ip, int port) throws IOException {
        BrokerInputFile inputFile = BrokerInputFile.create(filePath, brokerDesc, ip, port);
        return new ParquetReader(inputFile);
    }

    // Get file schema as a list of column name
    public List<String> getSchema(boolean debug) {
        List<String> colNames = Lists.newArrayList();
        FileMetaData metaData = fileReader.getFileMetaData();
        MessageType messageType = metaData.getSchema();
        List<ColumnDescriptor> columnDescriptors = messageType.getColumns();
        for (ColumnDescriptor column : columnDescriptors) {
            if (debug) {
                colNames.add(column.toString());
            } else {
                String colName = column.getPath()[0];
                if (column.getMaxDefinitionLevel() > 1) {
                    // this is a nested column, print then definition level
                    colName += " (" + column.getMaxDefinitionLevel() + ")";
                }
                colNames.add(colName);
            }
        }
        return colNames;
    }

    // get limit number of file content as 2-dimension array
    public List<List<String>> getLines(int limit) throws IOException {
        List<List<String>> lines = Lists.newArrayList();
        FileMetaData metaData = fileReader.getFileMetaData();
        MessageType schema = metaData.getSchema();
        final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        int readLines = 0;
        PageReadStore pages = null;
        while (null != (pages = fileReader.readNextRowGroup()) && readLines < limit) {
            final long rows = pages.getRowCount();
            final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
            for (int i = 0; i < rows && readLines < limit; i++) {
                List<String> line = Lists.newArrayList();
                final Group g = recordReader.read();
                parseGroup(g, line);
                lines.add(line);
                readLines++;
            }
        }
        return lines;
    }

    private void parseGroup(Group g, List<String> line) {
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);
            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();
            if (valueCount == 1) {
                line.add(g.getValueToString(field, 0));
            } else if (valueCount > 1) {
                List<String> array = Lists.newArrayList();
                for (int index = 0; index < valueCount; index++) {
                    if (fieldType.isPrimitive()) {
                        array.add(g.getValueToString(field, index));
                    }
                }
                line.add("[" + Joiner.on(",").join(array) + "]");
            } else {
                line.add("");
            }
        }
    }

    public void close() throws IOException {
        fileReader.close();
    }
}
