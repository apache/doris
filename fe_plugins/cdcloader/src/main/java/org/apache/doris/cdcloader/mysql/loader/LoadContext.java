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

package org.apache.doris.cdcloader.mysql.loader;

import org.apache.doris.cdcloader.mysql.config.LoaderOptions;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LoadContext {
    private static volatile LoadContext INSTANCE;
    private LoaderOptions loaderOptions;
    private final BlockingQueue<MySqlSplit> splits;
    private final BlockingQueue<SplitRecords> elementsQueue;
    private SplitReader splitReader;
    private SplitAssigner splitAssigner;

    private LoadContext() {
        this.splits = new LinkedBlockingQueue<>(1);
        this.elementsQueue = new LinkedBlockingQueue<>(2);
    }

    public static LoadContext getInstance() {
        if (INSTANCE == null) {
            synchronized (LoadContext.class) {
                if (INSTANCE == null) {
                    INSTANCE = new LoadContext();
                }
            }
        }
        return INSTANCE;
    }

    public BlockingQueue<MySqlSplit> getSplits() {
        return splits;
    }

    public BlockingQueue<SplitRecords> getElementsQueue() {
        return elementsQueue;
    }

    public void setSplitReader(SplitReader splitReader) {
        this.splitReader = splitReader;
    }

    public SplitReader getSplitReader() {
        return splitReader;
    }

    public LoaderOptions getLoaderOptions() {
        return loaderOptions;
    }

    public void setLoaderOptions(LoaderOptions loaderOptions) {
        this.loaderOptions = loaderOptions;
    }

    public void close() {
        splitReader.close();
        splitAssigner.close();
    }

    public void setSplitAssigner(SplitAssigner splitAssigner) {
        this.splitAssigner = splitAssigner;
    }

    public SplitAssigner getSplitAssigner(){
        return splitAssigner;
    }
}
