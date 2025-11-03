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
// This file is copied from
// https://github.com/apache/impala/blob/branch-4.0.0/fe/src/main/java/org/apache/impala/util/JMXJsonUtil.java
// and modified by Doris

package org.apache.doris.common.jni.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Set;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;

/**
 * Utility class that returns a JSON representation of the JMX beans.
 * This is based on hadoop-common's implementation of JMXJsonServlet.
 * <p>
 * Output format:
 * {
 * "beans" : [
 * {
 * "name":"bean-name"
 * ...
 * }
 * ]
 * }
 * Each bean's attributes will be converted to a JSON object member.
 * If the attribute is a boolean, a number, a string, or an array
 * it will be converted to the JSON equivalent.
 * <p>
 * If the value is a {@link CompositeData} then it will be converted
 * to a JSON object with the keys as the name of the JSON member and
 * the value is converted following these same rules.
 * If the value is a {@link TabularData} then it will be converted
 * to an array of the {@link CompositeData} elements that it contains.
 * All other objects will be converted to a string and output as such.
 * The bean's name and modelerType will be returned for all beans.
 */
public class JMXJsonUtil {
    // MBean server instance
    protected static transient MBeanServer mBeanServer =
            ManagementFactory.getPlatformMBeanServer();

    private static final Logger LOG = Logger.getLogger(JMXJsonUtil.class);

    // Returns the JMX beans as a JSON string.
    public static String getJMXJson() {
        StringWriter writer = new StringWriter();
        try {
            JsonGenerator jg = null;
            try {
                JsonFactory jsonFactory = new JsonFactory();
                jg = jsonFactory.createJsonGenerator(writer);
                jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                jg.writeStartObject();
                if (mBeanServer == null) {
                    jg.writeStringField("result", "ERROR");
                    jg.writeStringField("message", "No MBeanServer could be found");
                    jg.close();
                    LOG.error("No MBeanServer could be found.");
                    return writer.toString();
                }
                listBeans(jg);
            } finally {
                if (jg != null) {
                    jg.close();
                }
                if (writer != null) {
                    writer.close();
                }
            }
        } catch (IOException e) {
            LOG.error("Caught an exception while processing JMX request", e);
        }
        return writer.toString();
    }

    // Utility method that lists all the mbeans and write them using the supplied
    // JsonGenerator.
    private static void listBeans(JsonGenerator jg) throws IOException {
        Set<ObjectName> names;
        names = mBeanServer.queryNames(null, null);
        jg.writeArrayFieldStart("beans");
        Iterator<ObjectName> it = names.iterator();
        while (it.hasNext()) {
            ObjectName oname = it.next();
            MBeanInfo minfo;
            String code = "";
            try {
                minfo = mBeanServer.getMBeanInfo(oname);
                code = minfo.getClassName();
                String prs = "";
                try {
                    if ("org.apache.commons.modeler.BaseModelMBean".equals(code)) {
                        prs = "modelerType";
                        code = (String) mBeanServer.getAttribute(oname, prs);
                    }
                } catch (AttributeNotFoundException e) {
                    // If the modelerType attribute was not found, the class name is used
                    // instead.
                    LOG.error("getting attribute " + prs + " of " + oname
                            + " threw an exception", e);
                } catch (MBeanException e) {
                    // The code inside the attribute getter threw an exception so log it,
                    // and fall back on the class name
                    LOG.error("getting attribute " + prs + " of " + oname
                            + " threw an exception", e);
                } catch (RuntimeException e) {
                    // For some reason even with an MBeanException available to them
                    // Runtime exceptionscan still find their way through, so treat them
                    // the same as MBeanException
                    LOG.error("getting attribute " + prs + " of " + oname
                            + " threw an exception", e);
                } catch (ReflectionException e) {
                    // This happens when the code inside the JMX bean (setter?? from the
                    // java docs) threw an exception, so log it and fall back on the
                    // class name
                    LOG.error("getting attribute " + prs + " of " + oname
                            + " threw an exception", e);
                }
            } catch (InstanceNotFoundException e) {
                //Ignored for some reason the bean was not found so don't output it
                continue;
            } catch (IntrospectionException | ReflectionException e) {
                // This is an internal error, something odd happened with reflection so
                // log it and don't output the bean.
                LOG.error("Problem while trying to process JMX query with MBean " + oname, e);
                continue;
            }
            jg.writeStartObject();
            jg.writeStringField("name", oname.toString());
            jg.writeStringField("modelerType", code);
            MBeanAttributeInfo[] attrs = minfo.getAttributes();
            for (int i = 0; i < attrs.length; i++) {
                writeAttribute(jg, oname, attrs[i]);
            }
            jg.writeEndObject();
        }
        jg.writeEndArray();
    }

    // Utility method to write mBean attributes.
    private static void writeAttribute(JsonGenerator jg, ObjectName oname,
                                       MBeanAttributeInfo attr) throws IOException {
        if (!attr.isReadable()) {
            return;
        }
        String attName = attr.getName();
        if ("modelerType".equals(attName)) {
            return;
        }
        if (attName.indexOf("=") >= 0 || attName.indexOf(":") >= 0
                || attName.indexOf(" ") >= 0) {
            return;
        }
        Object value = null;
        try {
            value = mBeanServer.getAttribute(oname, attName);
        } catch (RuntimeMBeanException e) {
            // UnsupportedOperationExceptions happen in the normal course of business,
            // so no need to log them as errors all the time.
            if (e.getCause() instanceof UnsupportedOperationException) {
                LOG.trace("getting attribute " + attName + " of " + oname + " threw an exception", e);
            } else {
                LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
            }
            return;
        } catch (RuntimeErrorException e) {
            // RuntimeErrorException happens when an unexpected failure occurs in getAttribute
            // for example https://issues.apache.org/jira/browse/DAEMON-120
            if (LOG.isDebugEnabled()) {
                LOG.debug("getting attribute " + attName + " of " + oname + " threw an exception", e);
            }
            return;
        } catch (AttributeNotFoundException e) {
            //Ignored the attribute was not found, which should never happen because the bean
            //just told us that it has this attribute, but if this happens just don't output
            //the attribute.
            return;
        } catch (MBeanException e) {
            //The code inside the attribute getter threw an exception so log it, and
            // skip outputting the attribute
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
            return;
        } catch (RuntimeException e) {
            //For some reason even with an MBeanException available to them Runtime exceptions
            //can still find their way through, so treat them the same as MBeanException
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
            return;
        } catch (ReflectionException e) {
            //This happens when the code inside the JMX bean (setter?? from the java docs)
            //threw an exception, so log it and skip outputting the attribute
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
            return;
        } catch (InstanceNotFoundException e) {
            //Ignored the mbean itself was not found, which should never happen because we
            //just accessed it (perhaps something unregistered in-between) but if this
            //happens just don't output the attribute.
            return;
        }
        writeAttribute(jg, attName, value);
    }

    private static void writeAttribute(JsonGenerator jg, String attName, Object value)
            throws IOException {
        jg.writeFieldName(attName);
        writeObject(jg, value);
    }

    private static void writeObject(JsonGenerator jg, Object value) throws IOException {
        if (value == null) {
            jg.writeNull();
        } else {
            Class<?> c = value.getClass();
            if (c.isArray()) {
                jg.writeStartArray();
                int len = Array.getLength(value);
                for (int j = 0; j < len; j++) {
                    Object item = Array.get(value, j);
                    writeObject(jg, item);
                }
                jg.writeEndArray();
            } else if (value instanceof Number) {
                Number n = (Number) value;
                jg.writeNumber(n.toString());
            } else if (value instanceof Boolean) {
                Boolean b = (Boolean) value;
                jg.writeBoolean(b);
            } else if (value instanceof CompositeData) {
                CompositeData cds = (CompositeData) value;
                CompositeType comp = cds.getCompositeType();
                Set<String> keys = comp.keySet();
                jg.writeStartObject();
                for (String key : keys) {
                    writeAttribute(jg, key, cds.get(key));
                }
                jg.writeEndObject();
            } else if (value instanceof TabularData) {
                TabularData tds = (TabularData) value;
                jg.writeStartArray();
                for (Object entry : tds.values()) {
                    writeObject(jg, entry);
                }
                jg.writeEndArray();
            } else {
                jg.writeString(value.toString());
            }
        }
    }
}
