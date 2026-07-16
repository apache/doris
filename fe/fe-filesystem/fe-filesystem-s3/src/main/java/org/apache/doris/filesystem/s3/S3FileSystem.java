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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.ObjectStorageUri;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * S3 filesystem backed by the AWS S3 SDK.
 */
public class S3FileSystem extends S3CompatibleFileSystem {

    private final S3FileSystemProperties properties;
    private final S3ObjStorage s3Storage;

    public S3FileSystem(S3FileSystemProperties properties) {
        this(properties, new S3ObjStorage(properties));
    }

    S3FileSystem(S3FileSystemProperties properties, S3ObjStorage objStorage) {
        super(objStorage, objStorage.isUsePathStyle());
        this.properties = properties;
        this.s3Storage = objStorage;
    }

    public S3FileSystem(S3ObjStorage objStorage) {
        super(objStorage, objStorage.isUsePathStyle());
        this.properties = null;
        this.s3Storage = objStorage;
    }

    public Optional<S3FileSystemProperties> properties() {
        return Optional.ofNullable(properties);
    }

    private static String slashTerminatedNonGlobPrefix(String globPattern) {
        String prefix = longestNonGlobPrefix(globPattern);
        if (prefix.isEmpty() || prefix.endsWith("/")) {
            return prefix;
        }
        int slash = prefix.lastIndexOf('/');
        return slash < 0 ? "" : prefix.substring(0, slash + 1);
    }

    @Override
    protected boolean restartListingAfterDelete(String bucket) {
        return s3Storage.isDirectoryBucket(bucket);
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        String uri = path.uri();
        ObjectStorageUri parsed = parseUri(uri);
        if (!s3Storage.isDirectoryBucket(parsed.bucket())) {
            return super.globListWithLimit(path, startAfter, maxBytes, maxFiles);
        }

        String keyPattern = expandNumericRanges(parsed.key());
        Pattern matcher = Pattern.compile(globToRegex(keyPattern));
        String listPrefix = slashTerminatedNonGlobPrefix(keyPattern);
        String base = uriBase(uri, parsed);
        String listUri = base + listPrefix;

        List<RemoteObject> matches = new ArrayList<>();
        String continuationToken = null;
        do {
            RemoteObjects response = s3Storage.listObjects(listUri, continuationToken);
            for (RemoteObject object : response.getObjectList()) {
                if (!object.getKey().endsWith("/")
                        && matcher.matcher(object.getKey()).matches()
                        && (startAfter == null
                                || compareUtf8Binary(object.getKey(), startAfter) > 0)) {
                    matches.add(object);
                }
            }
            continuationToken = response.isTruncated()
                    ? response.getContinuationToken() : null;
        } while (continuationToken != null);

        matches.sort(Comparator.comparing(RemoteObject::getKey, S3FileSystem::compareUtf8Binary));
        List<FileEntry> files = new ArrayList<>();
        long totalSize = 0L;
        int nextIndex = matches.size();
        for (int i = 0; i < matches.size(); i++) {
            RemoteObject object = matches.get(i);
            files.add(new FileEntry(Location.of(base + object.getKey()), object.getSize(), false,
                    object.getModificationTime(), List.of()));
            totalSize += object.getSize();
            if ((maxFiles > 0 && files.size() >= maxFiles)
                    || (maxBytes > 0 && totalSize >= maxBytes)) {
                nextIndex = i + 1;
                break;
            }
        }
        String maxFile = matches.isEmpty() ? ""
                : nextIndex < matches.size() ? matches.get(nextIndex).getKey()
                : matches.get(Math.min(files.size(), matches.size()) - 1).getKey();
        return new GlobListing(files, parsed.bucket(), listPrefix, maxFile);
    }

    protected static boolean isSingleLevelGlob(String pathStr) {
        return S3CompatibleFileSystem.isSingleLevelGlob(pathStr);
    }

    protected static String longestNonGlobPrefix(String globPattern) {
        return S3CompatibleFileSystem.longestNonGlobPrefix(globPattern);
    }

    protected static List<String> expandedGlobListPrefixes(String globPattern) {
        return S3CompatibleFileSystem.expandedGlobListPrefixes(globPattern);
    }

    protected static String globToRegex(String glob) {
        return S3CompatibleFileSystem.globToRegex(glob);
    }
}
