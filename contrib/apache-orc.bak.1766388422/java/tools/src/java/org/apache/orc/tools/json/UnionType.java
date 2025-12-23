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
import java.util.ArrayList;
import java.util.List;

/**
 * A union type to represent types that don't fit together.
 */
class UnionType extends HiveType {
  final List<HiveType> children = new ArrayList<HiveType>();

  UnionType() {
    super(Kind.UNION);
  }

  UnionType(HiveType left, HiveType right) {
    super(Kind.UNION);
    children.add(left);
    children.add(right);
  }

  UnionType addType(HiveType type) {
    children.add(type);
    return this;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder("uniontype<");
    boolean first = true;
    for (HiveType child : children) {
      if (!first) {
        buf.append(',');
      } else {
        first = false;
      }
      buf.append(child.toString());
    }
    buf.append(">");
    return buf.toString();
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other) &&
        children.equals(((UnionType) other).children);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    for (HiveType child : children) {
      result += child.hashCode() * 17;
    }
    return result;
  }

  @Override
  public boolean subsumes(HiveType other) {
    return true;
  }

  @Override
  public void merge(HiveType other) {
    if (other instanceof UnionType) {
      for (HiveType otherChild : ((UnionType) other).children) {
        merge(otherChild);
      }
    } else {
      for (int i = 0; i < children.size(); ++i) {
        HiveType child = children.get(i);
        if (child.subsumes(other)) {
          child.merge(other);
          return;
        } else if (other.subsumes(child)) {
          other.merge(child);
          children.set(i, other);
          return;
        }
      }
      addType(other);
    }
  }

  @Override
  public void printFlat(PrintStream out, String prefix) {
    prefix = prefix + ".";
    int id = 0;
    for (HiveType child : children) {
      child.printFlat(out, prefix + (id++));
    }
  }

  @Override
  public TypeDescription getSchema() {
    TypeDescription result = TypeDescription.createUnion();
    for (HiveType child: children) {
      result.addUnionChild(child.getSchema());
    }
    return result;
  }
}
