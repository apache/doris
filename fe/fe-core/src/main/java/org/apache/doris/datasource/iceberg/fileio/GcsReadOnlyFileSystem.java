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

package org.apache.doris.datasource.iceberg.fileio;

import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.obj.S3ObjStorage;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only Hadoop FileSystem used by Iceberg HadoopCatalog for GCS bucket names containing
 * underscores. Hadoop S3A reads the bucket from {@link URI#getHost()}, which is {@code null} for
 * such names. This adapter reads it from {@link URI#getAuthority()} instead.
 *
 * <p>The adapter is catalog-local and intentionally implements only the operations required to
 * discover and read existing Iceberg tables. Mutating operations are not supported.
 */
public class GcsReadOnlyFileSystem extends FileSystem {
    private static final long DEFAULT_BLOCK_SIZE = 32 * 1024 * 1024L;

    private URI uri;
    private Path workingDirectory;
    private S3ObjStorage objectStorage;
    private S3FileIO fileIO;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        if (StringUtils.isBlank(name.getScheme()) || StringUtils.isBlank(name.getAuthority())) {
            throw new IOException("GCS S3-compatible URI must contain a scheme and bucket: " + name);
        }

        try {
            this.uri = new URI(name.getScheme(), name.getAuthority(), null, null, null);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid GCS S3-compatible URI: " + name, e);
        }
        this.workingDirectory = new Path(uri.toString() + "/");

        Map<String, String> properties = new HashMap<>();
        properties.put(StorageProperties.FS_GCS_SUPPORT, "true");
        copyIfNotBlank(conf, "fs.s3a.endpoint", properties, "gs.endpoint");
        copyIfNotBlank(conf, "fs.s3a.endpoint.region", properties, "s3.region");
        copyIfNotBlank(conf, "fs.s3a.access.key", properties, "gs.access_key");
        copyIfNotBlank(conf, "fs.s3a.secret.key", properties, "gs.secret_key");
        copyIfNotBlank(conf, "fs.s3a.session.token", properties, "gs.session_token");
        copyIfNotBlank(conf, "fs.s3a.connection.maximum", properties, "gs.connection.maximum");
        copyIfNotBlank(conf, "fs.s3a.connection.request.timeout", properties,
                "gs.connection.request.timeout");
        copyIfNotBlank(conf, "fs.s3a.connection.timeout", properties, "gs.connection.timeout");
        properties.put("gs.use_path_style", "true");
        properties.put("gs.force_parsing_by_standard_uri", "false");

        StorageProperties storageProperties = StorageProperties.createPrimary(properties);
        if (!(storageProperties instanceof GCSProperties)) {
            throw new IOException("Failed to create GCS storage properties for " + name);
        }
        this.objectStorage = new S3ObjStorage((GCSProperties) storageProperties);
        this.fileIO = new S3FileIO(() -> objectStorage.getClient());
        this.fileIO.initialize(Collections.singletonMap(S3FileIOProperties.PATH_STYLE_ACCESS, "true"));
    }

    private static void copyIfNotBlank(Configuration conf, String sourceKey,
            Map<String, String> target, String targetKey) {
        String value = conf.get(sourceKey);
        if (StringUtils.isNotBlank(value)) {
            target.put(targetKey, value);
        }
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        String location = toLocation(path);
        InputFile inputFile = fileIO.newInputFile(location);
        if (!inputFile.exists()) {
            throw new FileNotFoundException(location);
        }
        return new FSDataInputStream(new IcebergInputStream(inputFile.newStream()));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        FileStatus pathStatus = getFileStatus(path);
        if (pathStatus.isFile()) {
            return new FileStatus[] {pathStatus};
        }

        S3Location location = parse(path);
        String prefix = location.key.isEmpty() ? "" : withTrailingSlash(location.key);
        List<FileStatus> result = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                    .bucket(location.bucket)
                    .prefix(prefix)
                    .delimiter("/");
            if (continuationToken != null) {
                builder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = objectStorage.getClient().listObjectsV2(builder.build());
            for (S3Object object : response.contents()) {
                if (!object.key().equals(prefix)) {
                    result.add(fileStatus(object.size(), false,
                            object.lastModified() == null ? 0 : object.lastModified().toEpochMilli(),
                            pathForKey(object.key())));
                }
            }
            for (CommonPrefix commonPrefix : response.commonPrefixes()) {
                result.add(fileStatus(0, true, 0, pathForKey(commonPrefix.prefix())));
            }
            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);
        return result.toArray(new FileStatus[0]);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        S3Location location = parse(path);
        if (location.key.isEmpty()) {
            return fileStatus(0, true, 0, makeQualified(path));
        }

        if (!location.key.endsWith("/")) {
            try {
                HeadObjectResponse response = objectStorage.getClient().headObject(HeadObjectRequest.builder()
                        .bucket(location.bucket).key(location.key).build());
                return fileStatus(response.contentLength(), false,
                        response.lastModified() == null ? 0 : response.lastModified().toEpochMilli(),
                        makeQualified(path));
            } catch (S3Exception e) {
                if (!isNotFound(e)) {
                    throw new IOException("Failed to get file status for " + path, e);
                }
            }
        }

        String prefix = withTrailingSlash(location.key);
        try {
            objectStorage.getClient().headObject(HeadObjectRequest.builder()
                    .bucket(location.bucket).key(prefix).build());
            return fileStatus(0, true, 0, makeQualified(path));
        } catch (S3Exception e) {
            if (!isNotFound(e)) {
                throw new IOException("Failed to get directory status for " + path, e);
            }
        }

        try {
            ListObjectsV2Response response = objectStorage.getClient().listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(location.bucket).prefix(prefix).maxKeys(1).build());
            if (!response.contents().isEmpty() || !response.commonPrefixes().isEmpty()) {
                return fileStatus(0, true, 0, makeQualified(path));
            }
        } catch (S3Exception e) {
            throw new IOException("Failed to list directory " + path, e);
        }
        throw new FileNotFoundException(path.toString());
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
            int bufferSize, short replication, long blockSize, Progressable progress) {
        throw readOnly("create");
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) {
        throw readOnly("append");
    }

    @Override
    public boolean rename(Path source, Path destination) {
        throw readOnly("rename");
    }

    @Override
    public boolean delete(Path path, boolean recursive) {
        throw readOnly("delete");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) {
        throw readOnly("mkdirs");
    }

    @Override
    public void setWorkingDirectory(Path newDirectory) {
        this.workingDirectory = newDirectory.makeQualified(uri, workingDirectory);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    private FileStatus fileStatus(long length, boolean directory, long modificationTime, Path path) {
        return new FileStatus(length, directory, 1, DEFAULT_BLOCK_SIZE, modificationTime, path);
    }

    private Path pathForKey(String key) {
        return new Path(uri.toString() + "/" + StringUtils.removeStart(key, "/"));
    }

    private String toLocation(Path path) {
        S3Location location = parse(path);
        return uri.getScheme() + "://" + location.bucket
                + (location.key.isEmpty() ? "" : "/" + location.key);
    }

    private S3Location parse(Path path) {
        Path qualified = path.makeQualified(uri, workingDirectory);
        checkPath(qualified);
        URI pathUri = qualified.toUri();
        return new S3Location(pathUri.getAuthority(), StringUtils.removeStart(pathUri.getPath(), "/"));
    }

    private static String withTrailingSlash(String value) {
        return value.endsWith("/") ? value : value + "/";
    }

    private static boolean isNotFound(S3Exception exception) {
        return exception.statusCode() == 404;
    }

    private static UnsupportedOperationException readOnly(String operation) {
        return new UnsupportedOperationException(operation + " is not supported by the read-only GCS file system");
    }

    @Override
    public void close() throws IOException {
        try {
            if (fileIO != null) {
                fileIO.close();
            }
            if (objectStorage != null) {
                objectStorage.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close read-only GCS file system", e);
        } finally {
            super.close();
        }
    }

    private static class S3Location {
        private final String bucket;
        private final String key;

        private S3Location(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }
    }

    private static class IcebergInputStream extends FSInputStream {
        private final SeekableInputStream delegate;

        private IcebergInputStream(SeekableInputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void seek(long position) throws IOException {
            delegate.seek(position);
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public boolean seekToNewSource(long targetPosition) throws IOException {
            delegate.seek(targetPosition);
            return false;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            return delegate.read(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
