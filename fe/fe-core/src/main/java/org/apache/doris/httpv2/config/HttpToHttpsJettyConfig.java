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

package org.apache.doris.httpv2.config;

import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.webapp.AbstractConfiguration;
import org.eclipse.jetty.webapp.WebAppContext;

public class HttpToHttpsJettyConfig extends AbstractConfiguration {
    @Override
    public void configure(WebAppContext context) throws Exception {
        Constraint constraint = new Constraint();
        constraint.setDataConstraint(Constraint.DC_CONFIDENTIAL);

        ConstraintSecurityHandler handler = new ConstraintSecurityHandler();

        ConstraintMapping mappingGetRest = new ConstraintMapping();
        mappingGetRest.setConstraint(constraint);
        mappingGetRest.setPathSpec("/rest/*");
        mappingGetRest.setMethod("GET");
        handler.addConstraintMapping(mappingGetRest);

        ConstraintMapping mappingGetAPI = new ConstraintMapping();
        mappingGetAPI.setConstraint(constraint);
        mappingGetAPI.setPathSpec("/api/*");
        mappingGetAPI.setMethod("GET");
        handler.addConstraintMapping(mappingGetAPI);

        ConstraintMapping mappingGetDump = new ConstraintMapping();
        mappingGetDump.setConstraint(constraint);
        mappingGetDump.setPathSpec("/dump");
        mappingGetDump.setMethod("GET");
        handler.addConstraintMapping(mappingGetDump);

        ConstraintMapping mappingGetMetrics = new ConstraintMapping();
        mappingGetMetrics.setConstraint(constraint);
        mappingGetMetrics.setPathSpec("/metrics");
        mappingGetMetrics.setMethod("GET");
        handler.addConstraintMapping(mappingGetMetrics);

        ConstraintMapping mappingDel = new ConstraintMapping();
        mappingDel.setConstraint(constraint);
        mappingDel.setPathSpec("/*");
        mappingDel.setMethod("DELETE");
        handler.addConstraintMapping(mappingDel);

        ConstraintMapping mappingRest = new ConstraintMapping();
        mappingRest.setConstraint(constraint);
        mappingRest.setPathSpec("/rest/*");
        mappingRest.setMethod("POST");
        handler.addConstraintMapping(mappingRest);

        ConstraintMapping mappingCopy = new ConstraintMapping();
        mappingCopy.setConstraint(constraint);
        mappingCopy.setPathSpec("/copy/*");
        mappingCopy.setMethod("GET");
        handler.addConstraintMapping(mappingCopy);

        context.setSecurityHandler(handler);
    }
}
