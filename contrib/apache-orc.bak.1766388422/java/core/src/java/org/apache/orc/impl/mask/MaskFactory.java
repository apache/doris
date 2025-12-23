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
package org.apache.orc.impl.mask;

import org.apache.orc.DataMask;
import org.apache.orc.DataMaskDescription;
import org.apache.orc.TypeDescription;

import java.util.List;

/**
 * A mask factory framework that automatically builds a recursive mask.
 * The subclass defines how to mask the primitive types and the factory
 * builds a recursive tree of data masks that matches the schema tree.
 */
public abstract class MaskFactory {

  protected abstract DataMask buildBooleanMask(TypeDescription schema);
  protected abstract DataMask buildLongMask(TypeDescription schema);
  protected abstract DataMask buildDecimalMask(TypeDescription schema);
  protected abstract DataMask buildDoubleMask(TypeDescription schema);
  protected abstract DataMask buildStringMask(TypeDescription schema);
  protected abstract DataMask buildDateMask(TypeDescription schema);
  protected abstract DataMask buildTimestampMask(TypeDescription schema);
  protected abstract DataMask buildBinaryMask(TypeDescription schema);

  public DataMask build(TypeDescription schema,
                        DataMask.MaskOverrides overrides) {
    switch(schema.getCategory()) {
      case BOOLEAN:
        return buildBooleanMask(schema);
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return buildLongMask(schema);
      case FLOAT:
      case DOUBLE:
        return buildDoubleMask(schema);
      case DECIMAL:
        return buildDecimalMask(schema);
      case STRING:
      case CHAR:
      case VARCHAR:
        return buildStringMask(schema);
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return buildTimestampMask(schema);
      case DATE:
        return buildDateMask(schema);
      case BINARY:
        return buildBinaryMask(schema);
      case UNION:
        return buildUnionMask(schema, overrides);
      case STRUCT:
        return buildStructMask(schema, overrides);
      case LIST:
        return buildListMask(schema, overrides);
      case MAP:
        return buildMapMask(schema, overrides);
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }

  protected DataMask[] buildChildren(List<TypeDescription> children,
                                     DataMask.MaskOverrides overrides) {
    DataMask[] result = new DataMask[children.size()];
    for(int i = 0; i < result.length; ++i) {
      TypeDescription child = children.get(i);
      DataMaskDescription over = overrides.hasOverride(child);
      if (over != null) {
        result[i] = DataMask.Factory.build(over, child, overrides);
      } else {
        result[i] = build(child, overrides);
      }
    }
    return result;
  }

  protected DataMask buildStructMask(TypeDescription schema,
                                     DataMask.MaskOverrides overrides) {
    return new StructIdentity(buildChildren(schema.getChildren(), overrides));
  }

  DataMask buildListMask(TypeDescription schema,
                         DataMask.MaskOverrides overrides) {
    return new ListIdentity(buildChildren(schema.getChildren(), overrides));
  }

  DataMask buildMapMask(TypeDescription schema,
                        DataMask.MaskOverrides overrides) {
    return new MapIdentity(buildChildren(schema.getChildren(), overrides));
  }

  DataMask buildUnionMask(TypeDescription schema,
                          DataMask.MaskOverrides overrides) {
    return new UnionIdentity(buildChildren(schema.getChildren(), overrides));
  }
}
