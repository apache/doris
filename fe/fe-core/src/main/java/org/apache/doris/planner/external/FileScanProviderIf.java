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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TScanRangeLocations;

import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface FileScanProviderIf {
    // Return parquet/orc/text, etc.
    TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException;

    // Return S3/HDSF, etc.
    TFileType getLocationType() throws DdlException, MetaNotFoundException;

    // Return file list
    List<InputSplit> getSplits(List<Expr> exprs) throws IOException, UserException;

    // return properties for S3/HDFS, etc.
    Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException;

    List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException;

    ParamCreateContext createContext(Analyzer analyzer) throws UserException;

    void createScanRangeLocations(ParamCreateContext context, BackendPolicy backendPolicy,
            List<TScanRangeLocations> scanRangeLocations) throws UserException;

    int getInputSplitNum();

    long getInputFileSize();

    TableIf getTargetTable();
}
