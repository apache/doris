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

package org.apache.doris.datasource.property.fileformat;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;

/**
 * A wrapper of FileFormatProperties, which defers the initialization of the actual FileFormatProperties.
 * Currently, this is only used in broker load.
 * In broker load, user may not specify the file format properties, and we can not even infer the file format
 * from path at the beginning, because the path may be a directory with wildcard.
 * So we can only get the file suffix after listing files, and then initialize the actual.
 *
 * When using this class, you must call {@link #deferInit(TFileFormatType)} after getting the actual file format type
 * before using other methods.
 * And all methods must override and delegate to the actual FileFormatProperties.
 */
public class DeferredFileFormatProperties extends FileFormatProperties {

    private FileFormatProperties delegate;
    private Map<String, String> origProperties;

    public DeferredFileFormatProperties() {
        super(null, null);
    }

    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException {
        this.origProperties = formatProperties;
    }

    @Override
    public void fullTResultFileSinkOptions(TResultFileSinkOptions sinkOptions) {
        Preconditions.checkNotNull(delegate);
        delegate.fullTResultFileSinkOptions(sinkOptions);
    }

    @Override
    public TFileAttributes toTFileAttributes() {
        Preconditions.checkNotNull(delegate);
        return delegate.toTFileAttributes();
    }

    @Override
    protected String getOrDefault(Map<String, String> props, String key, String defaultValue,
            boolean isRemove) {
        Preconditions.checkNotNull(delegate);
        return delegate.getOrDefault(props, key, defaultValue, isRemove);
    }

    public TFileFormatType getFileFormatType() {
        Preconditions.checkNotNull(delegate);
        return delegate.getFileFormatType();
    }

    public TFileCompressType getCompressionType() {
        Preconditions.checkNotNull(delegate);
        return delegate.getCompressionType();
    }

    public String getFormatName() {
        Preconditions.checkNotNull(delegate);
        return delegate.getFormatName();
    }

    public FileFormatProperties getDelegate() {
        Preconditions.checkNotNull(delegate);
        return delegate;
    }

    public void deferInit(TFileFormatType formatType) {
        switch (formatType) {
            case FORMAT_PARQUET: {
                this.formatName = FORMAT_PARQUET;
                break;
            }
            case FORMAT_ORC: {
                this.formatName = FORMAT_ORC;
                break;
            }
            case FORMAT_JSON: {
                this.formatName = FORMAT_JSON;
                break;
            }
            default: {
                this.formatName = FORMAT_CSV;
                break;
            }
        }
        delegate = FileFormatProperties.createFileFormatProperties(this.formatName);
        delegate.analyzeFileFormatProperties(origProperties, false);
    }
}
