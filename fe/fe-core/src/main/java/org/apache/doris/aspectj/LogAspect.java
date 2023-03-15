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

package org.apache.doris.aspectj;

import org.apache.doris.common.annotation.ExceptionLog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;

@Aspect
public class LogAspect {

    private static final Logger LOG = LogManager.getLogger(LogAspect.class);

    @Pointcut("@annotation(org.apache.doris.common.annotation.ExceptionLog)")
    public void throwingLogPointCut() {
    }

    @AfterThrowing(value = "throwingLogPointCut()", throwing = "e")
    public void exceptionLog(JoinPoint point, Exception e)  {
        MethodSignature joinPointObject = (MethodSignature) point.getSignature();
        Method method = joinPointObject.getMethod();
        ExceptionLog exceptionLog = method.getAnnotation(ExceptionLog.class);
        LOG.error(e.getMessage());
        if (exceptionLog.isGetStackTrace()) {
            LOG.error(getStackTrace(e));
        }
    }

    public static String getStackTrace(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        throwable.printStackTrace(printWriter);
        return stringWriter.toString();
    }
}
