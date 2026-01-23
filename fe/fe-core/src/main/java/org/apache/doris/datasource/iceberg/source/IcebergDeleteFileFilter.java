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

package org.apache.doris.datasource.iceberg.source;

import lombok.Data;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.types.Conversions;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

@Data
public class IcebergDeleteFileFilter {
    private String deleteFilePath;
    private long filesize;
    private FileFormat fileformat;


    public static int type() {
        return 0;
    }

    public IcebergDeleteFileFilter(String deleteFilePath, long filesize, FileFormat fileformat) {
        this.deleteFilePath = deleteFilePath;
        this.filesize = filesize;
        this.fileformat = fileformat;
    }

    public static PositionDelete createPositionDelete(DeleteFile deleteFile) {
        Optional<Long> positionLowerBound = Optional.ofNullable(deleteFile.lowerBounds())
                .map(m -> m.get(MetadataColumns.DELETE_FILE_POS.fieldId()))
                .map(bytes -> Conversions.fromByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), bytes));
        Optional<Long> positionUpperBound = Optional.ofNullable(deleteFile.upperBounds())
                .map(m -> m.get(MetadataColumns.DELETE_FILE_POS.fieldId()))
                .map(bytes -> Conversions.fromByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), bytes));
        String deleteFilePath = deleteFile.path().toString();

        if (deleteFile.format() == FileFormat.PUFFIN) {
            // The content_offset and content_size_in_bytes fields are used to reference
            // a specific blob for direct access to a deletion vector.
            return new DeletionVector(deleteFilePath, positionLowerBound.orElse(-1L), positionUpperBound.orElse(-1L),
                    deleteFile.fileSizeInBytes(), deleteFile.contentOffset(), deleteFile.contentSizeInBytes());
        } else {
            return new PositionDelete(deleteFilePath, positionLowerBound.orElse(-1L), positionUpperBound.orElse(-1L),
                    deleteFile.fileSizeInBytes(), deleteFile.format());
        }
    }

    public static EqualityDelete createEqualityDelete(String deleteFilePath, List<Integer> fieldIds,
            long fileSize, FileFormat fileformat) {
        // todo:
        // Schema deleteSchema = TypeUtil.select(scan.schema(), new HashSet<>(fieldIds));
        // StructLikeSet deleteSet = StructLikeSet.create(deleteSchema.asStruct());
        // pass deleteSet to BE
        // compare two StructLike value, if equals, filtered
        return new EqualityDelete(deleteFilePath, fieldIds, fileSize, fileformat);
    }

    static class PositionDelete extends IcebergDeleteFileFilter {
        private final Long positionLowerBound;
        private final Long positionUpperBound;

        public PositionDelete(String deleteFilePath, Long positionLowerBound,
                              Long positionUpperBound, long fileSize, FileFormat fileformat) {
            super(deleteFilePath, fileSize, fileformat);
            this.positionLowerBound = positionLowerBound;
            this.positionUpperBound = positionUpperBound;
        }

        public OptionalLong getPositionLowerBound() {
            return positionLowerBound == -1L ? OptionalLong.empty() : OptionalLong.of(positionLowerBound);
        }

        public OptionalLong getPositionUpperBound() {
            return positionUpperBound == -1L ? OptionalLong.empty() : OptionalLong.of(positionUpperBound);
        }

        public static int type() {
            return 1;
        }
    }

    static class DeletionVector extends PositionDelete {
        private final long contentOffset;
        private final long contentLength;

        public DeletionVector(String deleteFilePath, Long positionLowerBound,
                Long positionUpperBound, long fileSize, long contentOffset, long contentLength) {
            super(deleteFilePath, positionLowerBound, positionUpperBound, fileSize, FileFormat.PUFFIN);
            this.contentOffset = contentOffset;
            this.contentLength = contentLength;
        }

        public long getContentOffset() {
            return contentOffset;
        }

        public long getContentLength() {
            return contentLength;
        }

        public static int type() {
            return 3;
        }
    }

    static class EqualityDelete extends IcebergDeleteFileFilter {
        private List<Integer> fieldIds;

        public EqualityDelete(String deleteFilePath, List<Integer> fieldIds, long fileSize, FileFormat fileFormat) {
            super(deleteFilePath, fileSize, fileFormat);
            this.fieldIds = fieldIds;
        }

        public List<Integer> getFieldIds() {
            return fieldIds;
        }

        public void setFieldIds(List<Integer> fieldIds) {
            this.fieldIds = fieldIds;
        }


        public static int type() {
            return 2;
        }
    }
}
