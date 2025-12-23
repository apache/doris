/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.orc.DataMask;
import org.apache.orc.DataMaskDescription;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MaskDescriptionImpl implements DataMaskDescription,
                                            Comparable<MaskDescriptionImpl> {
  private int id;
  private final String name;
  private final String[] parameters;
  private final List<TypeDescription> columns = new ArrayList<>();

  public MaskDescriptionImpl(String name,
                             String... parameters) {
    this.name = name;
    this.parameters = parameters == null ? new String[0] : parameters;
  }

  public MaskDescriptionImpl(int id,
                             OrcProto.DataMask mask) {
    this.id = id;
    this.name = mask.getName();
    this.parameters = new String[mask.getMaskParametersCount()];
    for(int p=0; p < parameters.length; ++p) {
      parameters[p] = mask.getMaskParameters(p);
    }

  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    } else {
      return compareTo((MaskDescriptionImpl) other) == 0;
    }
  }

  public void addColumn(TypeDescription column) {
    columns.add(column);
  }

  public void setId(int id) {
    this.id = id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String[] getParameters() {
    return parameters;
  }

  @Override
  public TypeDescription[] getColumns() {
    TypeDescription[] result = columns.toArray(new TypeDescription[0]);
    // sort the columns by their ids
    Arrays.sort(result, Comparator.comparingInt(TypeDescription::getId));
    return result;
  }

  public int getId() {
    return id;
  }

  public DataMask create(TypeDescription schema,
                         DataMask.MaskOverrides overrides) {
    return DataMask.Factory.build(this, schema, overrides);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("mask ");
    buffer.append(getName());
    buffer.append('(');
    String[] parameters = getParameters();
    if (parameters != null) {
      for(int p=0; p < parameters.length; ++p) {
        if (p != 0) {
          buffer.append(", ");
        }
        buffer.append(parameters[p]);
      }
    }
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + Arrays.hashCode(parameters);
    return result;
  }

  @Override
  public int compareTo(@NotNull MaskDescriptionImpl other) {
    if (other == this) {
      return 0;
    }
    int result = name.compareTo(other.name);
    int p = 0;
    while (result == 0 &&
               p < parameters.length && p < other.parameters.length) {
      result = parameters[p].compareTo(other.parameters[p]);
      p += 1;
    }
    if (result == 0) {
      result = Integer.compare(parameters.length, other.parameters.length);
    }
    return result;
  }
}

