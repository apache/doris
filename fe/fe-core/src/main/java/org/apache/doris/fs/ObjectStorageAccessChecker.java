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

package org.apache.doris.fs;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.property.storage.ObjectStorageProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.capability.ReadAccessCheckCapability;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/** Shared endpoint and credential checks for object-storage import paths. */
public final class ObjectStorageAccessChecker {

    private ObjectStorageAccessChecker() {
    }

    /**
     * Keeps the existing endpoint connectivity/SSRF check for paths that use a configured
     * endpoint. S3 Express paths use SDK-derived zonal endpoints and therefore skip that probe.
     */
    public static void checkEndpoint(StorageProperties properties, Collection<String> locations)
            throws UserException {
        if (!(properties instanceof ObjectStorageProperties)) {
            return;
        }
        boolean onlyS3Express = properties instanceof S3Properties
                && !locations.isEmpty()
                && locations.stream().allMatch(((S3Properties) properties)::isS3Express);
        if (!onlyS3Express) {
            S3Util.validateAndTestEndpoint(((ObjectStorageProperties) properties).getEndpoint());
        }
    }

    /** Performs all synchronous checks required before a Broker Load job is created. */
    public static void checkBrokerLoad(BrokerDesc brokerDesc, Collection<String> locations)
            throws UserException {
        StorageProperties properties = brokerDesc.getStorageProperties();
        checkEndpoint(properties, locations);
        if (!(properties instanceof S3Properties)) {
            return;
        }

        S3Properties s3Properties = (S3Properties) properties;
        Map<String, String> expressBuckets = new LinkedHashMap<>();
        for (String location : locations) {
            if (s3Properties.isS3Express(location)) {
                S3URI uri = S3URI.create(location);
                expressBuckets.putIfAbsent(uri.getBucket(), location);
            }
        }
        if (expressBuckets.isEmpty()) {
            return;
        }

        try (FileSystem fileSystem = FileSystemFactory.getFileSystem(brokerDesc)) {
            ReadAccessCheckCapability accessCheck =
                    fileSystem.requireCapability(ReadAccessCheckCapability.class);
            for (String location : expressBuckets.values()) {
                accessCheck.checkReadAccess(Location.of(location));
            }
        } catch (IOException | UnsupportedOperationException e) {
            throw new UserException("Failed to verify S3 Express read access before creating load job: "
                    + e.getMessage(), e);
        }
    }
}
