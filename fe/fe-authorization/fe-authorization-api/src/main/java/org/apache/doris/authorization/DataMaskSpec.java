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

package org.apache.doris.authorization;

import java.util.Objects;

/**
 * How one column must be rewritten before a subject may read it.
 *
 * <p>The payload is a <b>scalar SQL expression in Doris dialect</b> that replaces the column in the projection
 * ({@code CONCAT('XXXX', SUBSTR(phone, -4))}, {@code NULL}). Sources hold this as text natively - Ranger, for
 * one, stores a transformer template in its service definition and substitutes the column name into it - so
 * text is the lossless form; see {@link RowFilterSpec} for the full rationale.</p>
 *
 * <p>Like {@link RowFilterSpec} this is an immutable value with real equality, because the SQL result cache
 * uses spec equality to decide whether a column's masking changed.</p>
 */
public final class DataMaskSpec {

    private final String policyIdent;
    private final String maskSql;

    /**
     * @param policyIdent identifies the policy that produced this mask, for auditing and for change
     *         detection. The spec as a whole has to compare unequal once the policy changes; carry the
     *         version here when nothing else moves (e.g. {@code "<policyId>:<version>"} for a source whose
     *         policies are edited in place)
     * @param maskSql a scalar SQL expression in Doris dialect yielding the value the subject may see
     */
    public DataMaskSpec(String policyIdent, String maskSql) {
        this.policyIdent = requireNonBlank(policyIdent, "policyIdent");
        this.maskSql = requireNonBlank(maskSql, "maskSql");
    }

    public String getPolicyIdent() {
        return policyIdent;
    }

    public String getMaskSql() {
        return maskSql;
    }

    private static String requireNonBlank(String value, String field) {
        Objects.requireNonNull(value, field + " is required");
        if (value.trim().isEmpty()) {
            // A blank mask would parse to nothing and leave the raw column exposed. Fail at the source.
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataMaskSpec)) {
            return false;
        }
        DataMaskSpec that = (DataMaskSpec) o;
        return policyIdent.equals(that.policyIdent) && maskSql.equals(that.maskSql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policyIdent, maskSql);
    }

    @Override
    public String toString() {
        return "DataMaskSpec{policyIdent='" + policyIdent + "', maskSql='" + maskSql + "'}";
    }
}
