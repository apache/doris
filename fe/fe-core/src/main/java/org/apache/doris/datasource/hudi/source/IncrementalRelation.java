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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.spi.Split;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.exception.HoodieException;

import java.util.List;
import java.util.Map;

public interface IncrementalRelation {
    public static String EARLIEST_TIME = "earliest";

    List<FileSlice> collectFileSlices() throws HoodieException;

    List<Split> collectSplits() throws HoodieException;

    Map<String, String> getHoodieParams();

    boolean fallbackFullTableScan();

    boolean isIncludeStartTime();

    String getStartTs();

    String getEndTs();
}
