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
 * <p>Doris configures GCS access as Hadoop properties: native credentials (credential provider type,
 * service-account impersonation) as {@code fs.gs.*}, and HMAC interoperability keys or anonymous access as
 * {@code fs.s3a.*}. Only the Hadoop filesystems read them. Upstream ResolvingFileIO
 * routes {@code gs} to {@code GCSFileIO} whenever iceberg-gcp is on the classpath and falls back to
 * HadoopFileIO only when that class is missing. The plugins bundle iceberg-gcp for the REST catalog's
 * {@code GoogleAuthManager} and an explicitly chosen {@code GCSFileIO}, so this class pins the default
 * routing instead of relying on that class being absent. Every other scheme resolves exactly as upstream.
 *
 * <p>It lives in {@code org.apache.iceberg.io} because {@code implFromLocation} is package-private; the
 * override only takes effect when this class and iceberg-core share a class loader, which holds for the
 * FE connector plugin and the BE metadata scanner, both of which bundle iceberg-core themselves. Its class
 * name is part of serialized Iceberg scan tasks, so it must stay loadable on both sides.
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
