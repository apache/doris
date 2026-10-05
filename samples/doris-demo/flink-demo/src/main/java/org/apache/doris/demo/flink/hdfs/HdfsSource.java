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
package org.apache.doris.demo.flink.hdfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Custom hdfs source
 */
public class HdfsSource extends RichSourceFunction<String> {

    // Flink HDFS FileSystem
    private Configuration configuration;
    // hdfs path
    private String path;

    public HdfsSource(Configuration configuration, String path) {
        this.configuration = configuration;
        this.path = path;
    }


    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        FileSystem.initialize(configuration, null);
        FileSystem fileSystem = FileSystem.get(FileSystem.getDefaultFsUri());
        readHdfsFile(sourceContext, fileSystem);

    }

    @Override
    public void cancel() {
    }

    public void readHdfsFile(SourceContext<String> sourceContext, FileSystem fs) {
        FSDataInputStream dataInputStream = null;
        BufferedReader bufferedReader = null;
        try {
            dataInputStream = fs.open(new Path(path));
            bufferedReader = new BufferedReader(new InputStreamReader(dataInputStream));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sourceContext.collect(line);
            }
        } catch (IOException e) {
            throw new RuntimeException("read hdfs file fail");
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (dataInputStream != null) {
                try {
                    dataInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
