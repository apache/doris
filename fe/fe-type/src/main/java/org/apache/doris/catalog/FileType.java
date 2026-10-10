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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonPreProcessable;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Independent FILE identity with six canonical nullable physical children.
 * Scan selection is separate metadata and never changes this schema.
 */
public final class FileType extends Type implements GsonPostProcessable, GsonPreProcessable {
    public static final int FIELD_COUNT = 6;
    // Worst-case JSON escaping expands each public string byte to six ASCII bytes.
    public static final int JSON_DISPLAY_LENGTH = 6 * (65533 + 1024 + 1024) + 128;

    @SerializedName("fields")
    private List<StructField> fields;

    public FileType(List<StructField> fields) {
        validateFields(fields);
        this.fields = canonicalFields();
    }

    public static FileType create() {
        return new FileType(canonicalFields());
    }

    private static List<StructField> canonicalFields() {
        return ImmutableList.of(
                new StructField("uri", ScalarType.createVarcharType(65533)),
                new StructField("offset", Type.BIGINT),
                new StructField("size", Type.BIGINT),
                new StructField("content_type", ScalarType.createVarcharType(1024)),
                new StructField("checksum", ScalarType.createVarcharType(1024)),
                new StructField("inline", ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH)));
    }

    /** Return independent field descriptors so callers cannot mutate the fixed schema. */
    public List<StructField> getFields() {
        return canonicalFields();
    }

    public StructField getField(String name) {
        String normalized = name.toLowerCase(Locale.ROOT);
        for (StructField field : getFields()) {
            if (field.getName().equals(normalized)) {
                return field;
            }
        }
        return null;
    }

    public static void validateFields(List<StructField> fields) {
        Preconditions.checkArgument(fields != null && fields.size() == FIELD_COUNT,
                "FILE requires six canonical nullable children");
        List<StructField> expected = canonicalFields();
        for (int i = 0; i < FIELD_COUNT; i++) {
            StructField field = fields.get(i);
            StructField canonical = expected.get(i);
            Preconditions.checkArgument(field != null
                            && canonical.getName().equals(field.getName())
                            && canonical.getName().equals(field.getOriginalName())
                            && canonical.getType().equals(field.getType())
                            && canonical.getType().getLength() == field.getType().getLength()
                            && field.getContainsNull(),
                    "Invalid FILE child at position %s; expected %s", i, canonical.getName());
        }
    }

    public static void checkExecutionVersion() {
        Preconditions.checkState(Config.be_exec_version >= Config.FILE_MIN_BE_EXEC_VERSION,
                "FILE requires execution version %s or newer; current be_exec_version is %s",
                Config.FILE_MIN_BE_EXEC_VERSION, Config.be_exec_version);
    }

    /** Check creation/execution only; metadata inspection and replay do not require a new BE. */
    public static void checkExecutionVersion(Type type) {
        if (type.typeContainsFile()) {
            checkExecutionVersion();
        }
    }

    @Override
    public void gsonPostProcess() {
        validateFields(fields);
        fields = canonicalFields();
    }

    @Override
    public void gsonPreProcess() {
        validateFields(fields);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.FILE;
    }

    @Override
    protected String toSql(int depth) {
        return "FILE";
    }

    @Override
    protected String prettyPrint(int lpad) {
        return Strings.repeat(" ", lpad) + "FILE";
    }

    @Override
    public String toString() {
        return "FILE";
    }

    @Override
    public boolean matchesType(Type other) {
        return equals(other) || (other.isAnyType() && other.matchesType(this));
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FileType;
    }

    @Override
    public int hashCode() {
        return FileType.class.hashCode();
    }

    @Override
    public Integer getColumnSize() {
        return JSON_DISPLAY_LENGTH;
    }

    @Override
    public int getColumnStringRepSize() {
        return JSON_DISPLAY_LENGTH;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.FILE.getSlotSize();
    }

    @Override
    public void toThrift(TTypeDesc container) {
        checkExecutionVersion();
        validateFields(fields);
        TTypeNode node = new TTypeNode(TTypeNodeType.FILE);
        node.setStructFields(new ArrayList<>());
        container.addToTypes(node);
        for (StructField field : fields) {
            field.toThrift(container, node);
        }
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        return new TColumnType(PrimitiveType.FILE.toThrift());
    }
}
