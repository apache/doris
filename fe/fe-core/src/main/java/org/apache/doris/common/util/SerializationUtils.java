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

package org.apache.doris.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A utility class for serializing and deep cloning Serializable objects.
 * <p>
 * This class provides a method to clone an object by serializing it to a byte array
 * and then deserializing it back to an object. This approach ensures that a deep copy
 * is created, meaning that all objects referenced by the original object are also cloned.
 * <p>
 * Note: The object to be cloned must implement the {@link Serializable} interface.
 */
public class SerializationUtils {

    /**
     * Clones a Serializable object by serializing and deserializing it.
     *
     * @param object the object to be cloned. Must be Serializable.
     * @param <T>    the type of the object to clone. Must extend Serializable.
     * @return a deep copy of the provided object, or null if the input object is null.
     * @throws RuntimeException if cloning fails due to I/O or class not found exceptions.
     */
    public static <T> T clone(final T object) {
        // Check if the object is null; if true, return null
        if (object == null) {
            return null; // Return null for null input
        }

        try {
            // Serialize the object to a byte array
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); // Output stream to hold the serialized data
                    ObjectOutputStream oos = new ObjectOutputStream(baos)) { // ObjectOutputStream for serialization
                oos.writeObject(object); // Write the object to the output stream
                oos.flush(); // Ensure all data is written

                // Deserialize the byte array back to an object
                // Input stream from byte array
                try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                        ObjectInputStream ois = new ObjectInputStream(bais)) { // ObjectInputStream for deserialization
                    return (T) ois.readObject(); // Read and return the cloned object
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            // Wrap any exceptions thrown during serialization/deserialization
            throw new RuntimeException("Cloning failed", e);
        }
    }
}
