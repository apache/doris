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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata

suite("test_list_files", "p0,external") {
    boolean usePathStyle = getS3Provider().equalsIgnoreCase("S3")
    def client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getS3Endpoint(), getS3Region()))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(getS3AK(), getS3SK())))
            .withPathStyleAccessEnabled(usePathStyle).build()
    String bucket = getS3BucketName()
    String prefix = "list-files/${UUID.randomUUID()}/"
    String root = "s3://${bucket}/${prefix}"
    def keys = []
    def put = { String key, String contents ->
        byte[] data = contents.getBytes("UTF-8")
        def metadata = new ObjectMetadata()
        metadata.setContentLength(data.length)
        client.putObject(bucket, prefix + key, new ByteArrayInputStream(data), metadata)
        keys.add(prefix + key)
    }
    try {
        ["": "", "empty.txt": "", "a.json": "{}", "sub/": "", "sub/c.png": "abc",
         "sub/deep/d.txt": "abcd", "empty/": "", "empty/deep/": "", "marker/": "marker"]
                .each { key, contents -> put(key, contents) }
        sql "DROP RESOURCE IF EXISTS list_files_s3"
        sql """CREATE RESOURCE list_files_s3 PROPERTIES(
            "type"="s3", "s3.endpoint"="${getS3Endpoint()}", "s3.region"="${getS3Region()}",
            "s3.access_key"="${getS3AK()}", "s3.secret_key"="${getS3SK()}",
            "s3.bucket"="${bucket}", "s3.root.path"="unused", "use_path_style"="${usePathStyle}")"""
        def listing = { String path, String recursive ->
            """list_files("resource"="list_files_s3", "uri"="${path}"${recursive})"""
        }
        String direct = listing(root, ', "recursive"="false"')
        String recursive = listing(root, ', "recursive"="true"')
        String defaults = listing(root, '')
        qt_schema "DESC FUNCTION ${direct}"
        for (def entry : [direct: direct, recursive: recursive, defaults: defaults]) {
            "qt_${entry.key}" """SELECT substring(path, ${root.length() + 1}), size,
                file IS NULL, ELEMENT_AT(file, 'size'), ELEMENT_AT(file, 'content_type')
                FROM ${entry.value} ORDER BY path"""
            "qt_${entry.key}_counts" "SELECT count(*), count(file), sum(size) FROM ${entry.value}"
        }
        qt_size_only "SELECT size FROM ${direct} ORDER BY path"
        qt_path_only "SELECT substring(path, ${root.length() + 1}) FROM ${direct} ORDER BY path"
        qt_file_only "SELECT file IS NULL FROM ${direct} ORDER BY path"
        qt_limit "SELECT count(*) FROM (SELECT * FROM ${direct} LIMIT 2) t"
        qt_roundtrip """SELECT substring(path, ${root.length() + 1}),
            ELEMENT_AT(TO_FILE('list_files_s3', path), 'size') = size,
            ELEMENT_AT(file, 'checksum') = ELEMENT_AT(TO_FILE('list_files_s3', path), 'checksum')
            FROM ${recursive} ORDER BY path"""
        qt_no_trailing_slash """SELECT count(*), count(file), sum(size)
            FROM ${listing(root.substring(0, root.length() - 1), ', "recursive"="false"')}"""
        qt_empty_direct """SELECT substring(path, ${root.length() + 1}), size, file IS NULL
            FROM ${listing(root + 'empty/', ', "recursive"="false"')} ORDER BY path"""
        qt_empty_recursive "SELECT count(*) FROM ${listing(root + 'empty/', '')}"
        qt_missing "SELECT count(*) FROM ${listing(root + 'missing/', '')}"
        for (int i = 0; i < 1005; ++i) {
            put(String.format("pages/d%04d/", i), "")
        }
        qt_directory_pages """SELECT count(*), count(file), sum(size),
            count(distinct path) FROM ${listing(root + 'pages/', ', "recursive"="false"')}"""
        qt_directory_pages_recursive "SELECT count(*) FROM ${listing(root + 'pages/', '')}"
        for (int i = 0; i < 1005; ++i) {
            put(String.format("files/f%04d.txt", i), "x")
        }
        qt_file_pages """SELECT count(*), count(file), sum(size), count(distinct path)
            FROM ${listing(root + 'files/', '')}"""
        qt_file_pages_direct """SELECT count(*), count(file), sum(size)
            FROM ${listing(root + 'files/', ', "recursive"="false"')}"""
        test {
            sql """SELECT * FROM list_file("resource"="list_files_s3", "uri"="${root}")"""
            exception "Can not found function"
        }
        test {
            sql """SELECT * FROM list_files("resource"="list_files_s3", "uri"="${root}", "recursive"="yes")"""
            exception "recursive must be true or false"
        }
    } finally {
        keys.each { key -> client.deleteObject(bucket, key) }
        client.shutdown()
    }
}
