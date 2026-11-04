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

package org.apache.doris.common.jni.vec;

/**
 * A Doris FILE value. Every field is nullable; a null FileValue represents a SQL NULL FILE.
 *
 * <p>The inline bytes are not copied. A connector must keep them valid until appendFile returns.
 */
public final class FileValue {
    // Canonical child order shared with C++ DataTypeFile and ColumnFile.
    public static final int URI_FIELD_INDEX = 0;
    public static final int OFFSET_FIELD_INDEX = 1;
    public static final int SIZE_FIELD_INDEX = 2;
    public static final int CONTENT_TYPE_FIELD_INDEX = 3;
    public static final int CHECKSUM_FIELD_INDEX = 4;
    public static final int INLINE_FIELD_INDEX = 5;
    public static final int FIELD_COUNT = 6;

    private final String uri;
    private final Long offset;
    private final Long size;
    private final String contentType;
    private final String checksum;
    private final byte[] inline;

    public FileValue(String uri, Long offset, Long size, String contentType, String checksum, byte[] inline) {
        this.uri = uri;
        this.offset = offset;
        this.size = size;
        this.contentType = contentType;
        this.checksum = checksum;
        this.inline = inline;
    }

    public String getUri() {
        return uri;
    }

    public Long getOffset() {
        return offset;
    }

    public Long getSize() {
        return size;
    }

    public String getContentType() {
        return contentType;
    }

    public String getChecksum() {
        return checksum;
    }

    public byte[] getInline() {
        return inline;
    }
}
