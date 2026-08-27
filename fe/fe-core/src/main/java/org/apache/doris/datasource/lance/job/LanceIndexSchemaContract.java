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

package org.apache.doris.datasource.lance.job;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Persisted representation of schema contract v1: the ordered list of indexed
 * fields captured at admission. Before native invocation the worker reopens
 * the admitted dataset version, independently recomputes contract v1, and
 * compares the ordered representation; a mismatch or unavailable version is a
 * complete pre-invocation NOT_COMMITTED result. Equality is order-sensitive.
 *
 * <p>Building a contract from an Arrow schema belongs to admission (a later
 * delivery slice); this class is only the durable, comparable representation.
 */
public class LanceIndexSchemaContract {
    /** The only schema contract version defined. */
    public static final int SCHEMA_CONTRACT_VERSION_V1 = 1;

    @SerializedName(value = "scv")
    private int schemaContractVersion = SCHEMA_CONTRACT_VERSION_V1;

    @SerializedName(value = "flds")
    private List<IndexedField> fields = new ArrayList<>();

    /**
     * No-arg constructor for Gson replay only.
     */
    public LanceIndexSchemaContract() {
    }

    public LanceIndexSchemaContract(List<IndexedField> fields) {
        this.schemaContractVersion = SCHEMA_CONTRACT_VERSION_V1;
        this.fields = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(fields, "fields")));
    }

    public int getSchemaContractVersion() {
        return schemaContractVersion;
    }

    public List<IndexedField> getFields() {
        return fields == null ? Collections.emptyList() : Collections.unmodifiableList(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceIndexSchemaContract)) {
            return false;
        }
        LanceIndexSchemaContract that = (LanceIndexSchemaContract) o;
        return schemaContractVersion == that.schemaContractVersion && getFields().equals(that.getFields());
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaContractVersion, getFields());
    }

    @Override
    public String toString() {
        return "LanceIndexSchemaContract{version=" + schemaContractVersion + ", fields=" + getFields() + '}';
    }

    /**
     * One indexed field of the ordered contract. {@code normalizedType}
     * carries the relevant parameters such as decimal precision/scale and
     * timestamp unit/time-zone semantics; unindexed fields are excluded from
     * the contract.
     */
    public static final class IndexedField {
        @SerializedName(value = "fid")
        private long fieldId;

        @SerializedName(value = "nn")
        private String normalizedName;

        @SerializedName(value = "nt")
        private String normalizedType;

        @SerializedName(value = "nul")
        private boolean nullable;

        @SerializedName(value = "fsd")
        private Integer fixedSizeListDimension;

        @SerializedName(value = "vet")
        private String vectorElementType;

        @SerializedName(value = "ven")
        private Boolean vectorElementNullable;

        /**
         * No-arg constructor for Gson replay only.
         */
        public IndexedField() {
        }

        public IndexedField(long fieldId, String normalizedName, String normalizedType, boolean nullable,
                Integer fixedSizeListDimension, String vectorElementType, Boolean vectorElementNullable) {
            this.fieldId = fieldId;
            this.normalizedName = Objects.requireNonNull(normalizedName, "normalizedName");
            this.normalizedType = Objects.requireNonNull(normalizedType, "normalizedType");
            this.nullable = nullable;
            this.fixedSizeListDimension = fixedSizeListDimension;
            this.vectorElementType = vectorElementType;
            this.vectorElementNullable = vectorElementNullable;
        }

        public long getFieldId() {
            return fieldId;
        }

        public String getNormalizedName() {
            return normalizedName;
        }

        public String getNormalizedType() {
            return normalizedType;
        }

        public boolean isNullable() {
            return nullable;
        }

        public Integer getFixedSizeListDimension() {
            return fixedSizeListDimension;
        }

        public String getVectorElementType() {
            return vectorElementType;
        }

        public Boolean getVectorElementNullable() {
            return vectorElementNullable;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IndexedField)) {
                return false;
            }
            IndexedField that = (IndexedField) o;
            return fieldId == that.fieldId
                    && nullable == that.nullable
                    && Objects.equals(normalizedName, that.normalizedName)
                    && Objects.equals(normalizedType, that.normalizedType)
                    && Objects.equals(fixedSizeListDimension, that.fixedSizeListDimension)
                    && Objects.equals(vectorElementType, that.vectorElementType)
                    && Objects.equals(vectorElementNullable, that.vectorElementNullable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldId, normalizedName, normalizedType, nullable, fixedSizeListDimension,
                    vectorElementType, vectorElementNullable);
        }

        @Override
        public String toString() {
            return "IndexedField{fieldId=" + fieldId + ", normalizedName=" + normalizedName
                    + ", normalizedType=" + normalizedType + ", nullable=" + nullable
                    + ", fixedSizeListDimension=" + fixedSizeListDimension
                    + ", vectorElementType=" + vectorElementType
                    + ", vectorElementNullable=" + vectorElementNullable + '}';
        }
    }
}
