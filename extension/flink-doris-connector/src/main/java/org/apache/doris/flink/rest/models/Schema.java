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

package org.apache.doris.flink.rest.models;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Schema {
    private int status = 0;
    private String keysType;
    private List<Field> properties;

    public Schema() {
        properties = new ArrayList<>();
    }

    public Schema(int fieldCount) {
        properties = new ArrayList<>(fieldCount);
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getKeysType() {
        return keysType;
    }

    public void setKeysType(String keysType) {
        this.keysType = keysType;
    }

    public List<Field> getProperties() {
        return properties;
    }

    public void setProperties(List<Field> properties) {
        this.properties = properties;
    }

    public void put(String name, String type, String comment, int scale, int precision) {
        properties.add(new Field(name, type, comment, scale, precision));
    }

    public void put(Field f) {
        properties.add(f);
    }

    public Field get(int index) {
        if (index >= properties.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Fields size：" + properties.size());
        }
        return properties.get(index);
    }

    public int size() {
        return properties.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schema schema = (Schema) o;
        return status == schema.status &&
                Objects.equals(properties, schema.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, properties);
    }

    @Override
    public String toString() {
        return "Schema{" +
                "status=" + status +
                ", properties=" + properties +
                '}';
    }
}
