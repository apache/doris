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

package org.apache.orc.tools.json;

import org.apache.orc.TypeDescription;

import java.io.PrintStream;
import java.util.Objects;

public class MapType extends HiveType {

  private final HiveType keyType;

  private final HiveType valueType;

  MapType(HiveType keyType, HiveType valueType) {
    super(Kind.MAP);
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public String toString() {
    return "map<" + keyType + "," + valueType + ">";
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o) && keyType.equals(((MapType)o).keyType) &&
        valueType.equals(((MapType)o).valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), keyType, valueType);
  }

  @Override
  public boolean subsumes(HiveType other) {
    return (other.kind == Kind.MAP &&
        keyType.subsumes(((MapType)other).keyType) &&
        valueType.subsumes(((MapType)other).valueType)) ||
        other.kind == Kind.NULL;
  }

  @Override
  public void merge(HiveType other) {
    if (other.getClass() == MapType.class) {
      MapType otherMap = (MapType) other;
      keyType.merge(otherMap.keyType);
      valueType.merge(otherMap.valueType);
    }
  }

  @Override
  public void printFlat(PrintStream out, String prefix) {
    prefix = prefix + ".";
    keyType.printFlat(out, prefix + "key");
    keyType.printFlat(out, prefix + "value");
  }

  @Override
  public TypeDescription getSchema() {
    return TypeDescription.createMap(keyType.getSchema(), valueType.getSchema());
  }

}
