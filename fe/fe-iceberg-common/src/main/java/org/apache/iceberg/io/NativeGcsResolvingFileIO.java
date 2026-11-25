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

package org.apache.iceberg.io;

/**
 * A {@link ResolvingFileIO} that keeps {@code gs://} locations on {@code HadoopFileIO}.
 *
 * <p>Adding iceberg-gcp makes the SDK route these locations to GCSFileIO, which does not read the
 * {@code fs.s3a.*} HMAC settings Doris supplies. Both FE and the BE metadata scanner must load this
 * implementation because its class name is carried inside serialized Iceberg scan tasks.
 *
 * <p>The SDK package is required because {@code implFromLocation} is package-private. It also keeps
 * this class and iceberg-core in the same class loader on both sides of a metadata scan.
 */
public class NativeGcsResolvingFileIO extends ResolvingFileIO {
    private static final long serialVersionUID = 1L;

    private static final String GCS_FILE_IO_IMPL = "org.apache.iceberg.gcp.gcs.GCSFileIO";
    private static final String HADOOP_FILE_IO_IMPL = "org.apache.iceberg.hadoop.HadoopFileIO";

    @Override
    String implFromLocation(String location) {
        String impl = super.implFromLocation(location);
        return GCS_FILE_IO_IMPL.equals(impl) ? HADOOP_FILE_IO_IMPL : impl;
    }
}
