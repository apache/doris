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

package org.apache.doris.common.util;

import org.apache.doris.common.FeConstants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class S3Util {
    private static final Logger LOG = LogManager.getLogger(S3Util.class);

    public static boolean isObjStorage(String location) {
        return isS3CompatibleObjStorage(location) || location.startsWith(FeConstants.FS_PREFIX_OBS);
    }

    private static boolean isS3CompatibleObjStorage(String location) {
        return location.startsWith(FeConstants.FS_PREFIX_S3)
                || location.startsWith(FeConstants.FS_PREFIX_S3A)
                || location.startsWith(FeConstants.FS_PREFIX_S3N)
                || location.startsWith(FeConstants.FS_PREFIX_GCS)
                || location.startsWith(FeConstants.FS_PREFIX_BOS)
                || location.startsWith(FeConstants.FS_PREFIX_COS)
                || location.startsWith(FeConstants.FS_PREFIX_OSS);
    }

    public static  String convertToS3IfNecessary(String location) {
        LOG.debug("try convert location to s3 prefix: " + location);
        if (isS3CompatibleObjStorage(location)) {
            int pos = location.indexOf("://");
            if (pos == -1) {
                throw new RuntimeException("No '://' found in location: " + location);
            }
            return "s3" + location.substring(pos);
        }
        return location;
    }

}
