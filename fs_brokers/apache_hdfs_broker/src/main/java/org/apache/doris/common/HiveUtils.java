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

package org.apache.doris.common;

import org.apache.doris.broker.hdfs.BrokerException;
import org.apache.doris.thrift.TBrokerOperationStatusCode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class HiveUtils {
    private static final Logger logger = Logger.getLogger(HiveUtils.class.getName());

    public static boolean isSplittable(String path, String inputFormatName,
                                       Map<String, String> properties) throws BrokerException {
        JobConf jobConf = getJobConf(properties);
        InputFormat inputFormat = getInputFormat(jobConf, inputFormatName);
        return isSplittableInternal(inputFormat, new Path(path), jobConf);
    }

    private static JobConf getJobConf(Map<String, String> properties) {
        Configuration configuration = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        return new JobConf(configuration);
    }

    private static InputFormat<?, ?> getInputFormat(JobConf jobConf, String inputFormatName) throws BrokerException {
        try {
            Class<? extends InputFormat<?, ?>> inputFormatClass = getInputFormatClass(jobConf, inputFormatName);
            if (inputFormatClass == SymlinkTextInputFormat.class) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }

            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        } catch (ClassNotFoundException | RuntimeException e) {
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
                "Unable to create input format " + inputFormatName, e);
        }
    }

    @SuppressWarnings({"unchecked", "RedundantCast"})
    private static Class<? extends InputFormat<?, ?>> getInputFormatClass(JobConf conf, String inputFormatName)
        throws ClassNotFoundException {
        // CDH uses different names for Parquet
        if ("parquet.hive.DeprecatedParquetInputFormat".equals(inputFormatName)
            || "parquet.hive.MapredParquetInputFormat".equals(inputFormatName)) {
            return MapredParquetInputFormat.class;
        }

        Class<?> clazz = conf.getClassByName(inputFormatName);
        return (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
    }

    private static boolean isSplittableInternal(InputFormat<?, ?> inputFormat, Path path, JobConf jobConf) {
        // ORC uses a custom InputFormat but is always splittable
        if (inputFormat.getClass().getSimpleName().equals("OrcInputFormat")) {
            return true;
        }

        // use reflection to get isSplittable method on FileInputFormat
        Method method = null;
        for (Class<?> clazz = inputFormat.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            try {
                method = clazz.getDeclaredMethod("isSplitable", FileSystem.class, Path.class);
                break;
            } catch (NoSuchMethodException ignored) {
                logger.warn(LoggerMessageFormat.format("Class {} doesn't contain isSplitable method", clazz));
            }
        }

        if (method == null) {
            return false;
        }
        try {
            method.setAccessible(true);
            return (boolean) method.invoke(inputFormat, path.getFileSystem(jobConf), path);
        } catch (InvocationTargetException | IllegalAccessException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
