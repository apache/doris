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

package org.apache.doris.cdcloader.mysql.config;

import java.util.Map;

import org.apache.doris.cdcloader.mysql.reader.SourceReader;

public class LoadContext {
    private static volatile LoadContext INSTANCE;
    private SourceReader sourceReader;
    private Map<String, String> options;

    private LoadContext() {
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


    public SourceReader getSourceReader() {
        return sourceReader;
    }

    public void setSourceReader(SourceReader sourceReader) {
        this.sourceReader = sourceReader;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

}
