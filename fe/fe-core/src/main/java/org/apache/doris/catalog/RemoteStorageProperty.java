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

package org.apache.doris.catalog;


import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class RemoteStorageProperty implements Writable {

    public enum RemoteStorageType {
        S3
    }

    public RemoteStorageType getStorageType() {
        return null;
    }

    public void modifyRemoteStorage(Map<String, String> properties) throws DdlException {
        throw new NotImplementedException();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException("Not implemented serializable.");
    }

    public static void writeTo(RemoteStorageProperty property, DataOutput output) throws IOException {
        Text.writeString(output, property.getStorageType().name());
        property.write(output);
    }

    public static RemoteStorageProperty readIn(DataInput input) throws IOException {
        String storageTypeName = Text.readString(input);
        RemoteStorageType storageType = RemoteStorageType.valueOf(storageTypeName);
        if (storageType == null) {
            throw new IOException("Unknown remote storage type: " + storageTypeName);
        }
        switch (storageType) {
            case S3:
                return S3Property.read(input);
            default:
                throw new IOException("Unknown remote storage type: " + storageTypeName);
        }
    }

    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }
}