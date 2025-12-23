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
package org.apache.orc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TestCorruptTypes {

  @Test
  public void testIllType() {
    testCorruptHelper(OrcProto.Type.Kind.LIST, 0,
            "LIST type should contain exactly one subtype but has 0");
    testCorruptHelper(OrcProto.Type.Kind.LIST, 2,
            "LIST type should contain exactly one subtype but has 2");
    testCorruptHelper(OrcProto.Type.Kind.MAP, 1,
            "MAP type should contain exactly two subtypes but has 1");
    testCorruptHelper(OrcProto.Type.Kind.MAP, 3,
            "MAP type should contain exactly two subtypes but has 3");
    testCorruptHelper(OrcProto.Type.Kind.UNION, 0,
            "UNION type should contain at least one subtype but has none");
  }

  private void testCorruptHelper(OrcProto.Type.Kind type,
                                 int subTypesCnt,
                                 String errMsg) {

    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    OrcProto.Type.Builder builder = OrcProto.Type.newBuilder().setKind(type);
    for (int i = 0; i < subTypesCnt; ++i) {
      builder.addSubtypes(i + 2);
    }
    types.add(builder.build());
    try {
      OrcUtils.convertTypeFromProtobuf(types, 0);
      fail("Should throw FileFormatException for ill types");
    } catch (FileFormatException e) {
      assertEquals(errMsg, e.getMessage());
    } catch (Throwable e) {
      fail("Should only trow FileFormatException for ill types");
    }
  }
}
