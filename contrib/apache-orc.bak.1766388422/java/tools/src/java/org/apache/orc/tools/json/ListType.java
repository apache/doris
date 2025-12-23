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

/**
 * A model for types that are lists.
 */
class ListType extends HiveType {
  HiveType elementType;

  ListType() {
    super(Kind.LIST);
  }

  ListType(HiveType child) {
    super(Kind.LIST);
    this.elementType = child;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder("list<");
    buf.append(elementType.toString());
    buf.append(">");
    return buf.toString();
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other) &&
        elementType.equals(((ListType) other).elementType);
  }

  @Override
  public int hashCode() {
    return super.hashCode() * 3 + elementType.hashCode();
  }

  @Override
  public boolean subsumes(HiveType other) {
    return other.kind == Kind.NULL || other.kind == Kind.LIST;
  }

  @Override
  public void merge(HiveType other) {
    if (other instanceof ListType) {
      ListType otherList = (ListType) other;
      if (elementType.subsumes(otherList.elementType)) {
        elementType.merge(otherList.elementType);
      } else if (otherList.elementType.subsumes(elementType)) {
        otherList.elementType.merge(elementType);
        elementType = otherList.elementType;
      } else {
        elementType = new UnionType(elementType, otherList.elementType);
      }
    }
  }

  @Override
  public void printFlat(PrintStream out, String prefix) {
    elementType.printFlat(out, prefix + "._list");
  }

  @Override
  public TypeDescription getSchema() {
    return TypeDescription.createList(elementType.getSchema());
  }
}
