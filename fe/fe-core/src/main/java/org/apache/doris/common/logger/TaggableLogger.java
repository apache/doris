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

package org.apache.doris.common.logger;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.proto.Types;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

/**
 * Wrap a log4j Logger and tag on the log. usage:
 *   LOG.tag("query_id", queryId).info("here is an info for a query");
 *
 * TaggableLogger extends log4j Logger, so it's fully compatible with the usual
 * one. You can use method tag(key, value) to add tags and log all the tags and
 * message when log method is called, like info(fmt, ...). Usually the tag key is
 * determined, like "query_id", so we use specified tag methods more often, like
 * query_id(id). You can add a new tag method to TaggableLogger if needed.
 *
 * You can custom your tagged logging format in LoggerProvider.getTaggedLogFormat,
 * the default is like "#message#|k1=v1|k2=v2". You can also custom all the tag
 * names in TagKey. For example, if you wish to use camelCase style, just set tag
 * name constants like QUERY_ID to "queryId".
 *
 * The transfer from the variable of tag method to string is immediate. If a tagged
 * logging has time-consuming to-string procedure and is at debug level which may
 * not be processed, check isDebugEnabled() first.
 */
public interface TaggableLogger extends Logger {

    TaggableLogger tag(String key, Object value);

    // add tag method here

    default TaggableLogger queryId(final TUniqueId queryId) {
        return tag(TagKey.QUERY_ID, DebugUtil.printId(queryId));
    }

    default TaggableLogger queryId(final UUID queryId) {
        return tag(TagKey.QUERY_ID, DebugUtil.printId(queryId));
    }

    default TaggableLogger queryId(final Types.PUniqueId queryId) {
        return tag(TagKey.QUERY_ID, DebugUtil.printId(queryId));
    }

}
