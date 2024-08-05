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

package org.apache.doris.common.io;

import org.apache.doris.meta.MetaContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;

/*
 * This class is for deep copying a writable instance.
 */
public class DeepCopy {
    private static final Logger LOG = LoggerFactory.getLogger(DeepCopy.class);

    public static final String READ_FIELDS_METHOD_NAME = "readFields";
    public static final String READ_METHOD_NAME = "read";

    // deep copy orig to dest.
    // the param "c" is the implementation class of "dest".
    // And the "dest" class must has method "readFields(DataInput)"
    @Deprecated
    public static boolean copy(Writable orig, Writable dest, Class c, int metaVersion) {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(metaVersion);
        metaContext.setThreadLocalInfo();

        try (FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            orig.write(out);
            out.flush();

            try (DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream())) {
                Method readMethod = c.getDeclaredMethod(READ_FIELDS_METHOD_NAME, DataInput.class);
                readMethod.invoke(dest, in);
            }
        } catch (Exception e) {
            LOG.warn("failed to copy object.", e);
            return false;
        } finally {
            MetaContext.remove();
        }
        return true;
    }

    // Deep copy orig to result
    // The param "c" is the implementation class of "orig"
    // And the "orig" class must has method "read(DataInput)"
    // The result is another object which totally exactly same as the orig
    public static <T> T copy(Writable orig, Class<T> c, int metaVersion) {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(metaVersion);
        metaContext.setThreadLocalInfo();

        try (FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            orig.write(out);
            out.flush();

            try (DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream())) {
                Method readMethod = c.getDeclaredMethod(READ_METHOD_NAME, DataInput.class);
                T result = (T) readMethod.invoke(orig, in);
                return result;
            }
        } catch (Exception e) {
            LOG.warn("failed to copy object.", e);
            return null;
        } finally {
            MetaContext.remove();
        }
    }
}
