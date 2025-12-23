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

/**
 * The Provider for all of the built-in data masks.
 */
public class MaskProvider implements DataMask.Provider {

  @Override
  public DataMask build(DataMaskDescription description,
                        TypeDescription schema,
                        DataMask.MaskOverrides overrides) {
    String name = description.getName();
    if (name.equals(DataMask.Standard.NULLIFY.getName())) {
      return new NullifyMask();
    } else if (name.equals(DataMask.Standard.REDACT.getName())) {
      return new RedactMaskFactory(description.getParameters())
                 .build(schema, overrides);
    } else if(name.equals(DataMask.Standard.SHA256.getName())) {
      return new SHA256MaskFactory().build(schema, overrides);
    }
    return null;
  }
}
