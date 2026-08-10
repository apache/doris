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

package org.apache.doris.auth.certificate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.ServiceLoader;

public final class CertificateRuntimeAuthFactory {
    private static final Logger LOG = LogManager.getLogger(CertificateRuntimeAuthFactory.class);
    private static volatile CertificateRuntimeAuthService service;

    private CertificateRuntimeAuthFactory() {
    }

    public static CertificateRuntimeAuthService getInstance() {
        if (service != null) {
            return service;
        }
        synchronized (CertificateRuntimeAuthFactory.class) {
            if (service != null) {
                return service;
            }
            ServiceLoader<CertificateRuntimeAuthService> loader =
                    ServiceLoader.load(CertificateRuntimeAuthService.class);
            Iterator<CertificateRuntimeAuthService> iterator = loader.iterator();
            if (iterator.hasNext()) {
                service = iterator.next();
                LOG.info("Using CertificateRuntimeAuthService implementation: {}", service.getClass().getName());
            } else {
                LOG.info("No customized CertificateRuntimeAuthService found, fallback to no-op implementation");
                service = new NoOpCertificateRuntimeAuthService();
            }
            return service;
        }
    }

    @Deprecated
    public static CertificateRuntimeAuthService create() {
        return getInstance();
    }
}
