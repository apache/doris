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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.impl.MaskDescriptionImpl;

import java.util.ServiceLoader;

/**
 * The API for masking data during column encryption for ORC.
 * <p>
 * They apply to an individual column (via ColumnVector) instead of a
 * VectorRowBatch.
 *
 */
public interface DataMask {

  /**
   * The standard DataMasks can be created using this short cut.
   *
   * For example, DataMask.Standard.NULLIFY.build(schema) will build a
   * nullify DataMask.
   */
  enum Standard {
    NULLIFY("nullify"),
    REDACT("redact"),
    SHA256("sha256");

    Standard(String name) {
      this.name = name;
    }

    private final String name;

    /**
     * Get the name of the predefined data mask.
     * @return the standard name
     */
    public String getName() {
      return name;
    }

    /**
     * Build a DataMaskDescription given the name and a set of parameters.
     * @param params the parameters
     * @return a MaskDescription with the given parameters
     */
    public DataMaskDescription getDescription(String... params) {
      return new MaskDescriptionImpl(name, params);
    }
  }

  /**
   * Mask the given range of values
   * @param original the original input data
   * @param masked the masked output data
   * @param start the first data element to mask
   * @param length the number of data elements to mask
   */
  void maskData(ColumnVector original, ColumnVector masked,
                int start, int length);


  /**
   * An interface to provide override data masks for sub-columns.
   */
  interface MaskOverrides {
    /**
     * Should the current mask be overridden on a sub-column?
     * @param type the subfield
     * @return the new mask description or null to continue using the same one
     */
    DataMaskDescription hasOverride(TypeDescription type);
  }

  /**
   * Providers can provide one or more kinds of data masks.
   * Because they are discovered using a service loader, they may be added
   * by third party jars.
   */
  interface Provider {
    /**
     * Build a mask with the given parameters.
     * @param description the description of the data mask
     * @param schema the type of the field
     * @param overrides a function to override this mask on a sub-column
     * @return the new data mask or null if this name is unknown
     */
    DataMask build(DataMaskDescription description,
                   TypeDescription schema,
                   MaskOverrides overrides);
  }

  /**
   * To create a DataMask, the users should come through this API.
   *
   * It supports extension via additional DataMask.Provider implementations
   * that are accessed through Java's ServiceLoader API.
   */
  class Factory {

    /**
     * Build a new DataMask instance.
     * @param mask the description of the data mask
     * @param schema the type of the field
     * @param overrides sub-columns where the mask is overridden
     * @return a new DataMask
     * @throws IllegalArgumentException if no such kind of data mask was found
     *
     * @see org.apache.orc.impl.mask.MaskProvider for the standard provider
     */
    public static DataMask build(DataMaskDescription mask,
                                 TypeDescription schema,
                                 MaskOverrides overrides) {
      for(Provider provider: ServiceLoader.load(Provider.class)) {
        DataMask result = provider.build(mask, schema, overrides);
        if (result != null) {
          return result;
        }
      }
      throw new IllegalArgumentException("Can't find data mask - " + mask);
    }
  }
}
