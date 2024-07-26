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

package org.apache.doris.regression.action

import groovy.json.JsonSlurper
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import groovy.util.logging.Slf4j
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils

@Slf4j
class ProfileAction implements SuiteAction {
    private String tag
    private Runnable runCallback
    private Closure<String> check
    private SuiteContext context

    ProfileAction(SuiteContext context, String tag) {
        this.context = context
        this.tag = Objects.requireNonNull(tag, "tag can not be null")
    }

    void run(@ClosureParams(value = FromString, options = []) Runnable run) {
        runCallback = run
    }

    void check(
        @ClosureParams(value = FromString, options = ["String, Throwable"]) Closure check) {
        this.check = check
    }

    @Override
    void run() {
        if (runCallback.is(null)) {
            throw new IllegalStateException("Missing tag")
        }
        if (check.is(null)) {
            throw new IllegalStateException("Missing check")
        }
        def conn = context.getConnection()
        try {
            JdbcUtils.executeToList(conn, "set enable_profile=true")

            Throwable exception = null
            try {
                this.runCallback.run()
            } catch (Throwable t) {
                exception = t
            }

            def httpCli = new HttpCliAction(context)
            httpCli.endpoint(context.config.feHttpAddress)
            httpCli.uri("/rest/v1/query_profile")
            httpCli.op("get")
            httpCli.printResponse(false)

            if (context.config.fetchRunMode()) {
                httpCli.basicAuthorization(context.config.feCloudHttpUser, context.config.feCloudHttpPassword)
            } else {
                httpCli.basicAuthorization(context.config.feHttpUser, context.config.feHttpPassword)
            }
            httpCli.check { code, body ->
                if (code != 200) {
                    throw new IllegalStateException("Get profile list failed, code: ${code}, body:\n${body}")
                }

                def jsonSlurper = new JsonSlurper()
                List profileData = jsonSlurper.parseText(body).data.rows
                for (final def profileItem in profileData) {
                    if (profileItem["Sql Statement"].toString().contains(tag)) {
                        def profileId = profileItem["Profile ID"].toString()

                        def profileCli = new HttpCliAction(context)
                        profileCli.endpoint(context.config.feHttpAddress)
                        profileCli.uri("/rest/v1/query_profile/${profileId}")
                        profileCli.op("get")
                        profileCli.printResponse(false)

                        if (context.config.fetchRunMode()) {
                            profileCli.basicAuthorization(context.config.feCloudHttpUser, context.config.feCloudHttpPassword)
                        } else {
                            profileCli.basicAuthorization(context.config.feHttpUser, context.config.feHttpPassword)
                        }
                        profileCli.check { profileCode, profileResp ->
                            if (profileCode != 200) {
                                throw new IllegalStateException("Get profile failed, url: ${"/rest/v1/query_profile/${profileId}"}, code: ${profileCode}, body:\n${profileResp}")
                            }

                            def jsonSlurper2 = new JsonSlurper()
                            def profileText = jsonSlurper2.parseText(profileResp).data
                            profileText = profileText.replace("&nbsp;", " ")
                            profileText = profileText.replace("</br>", "\n")
                            this.check(profileText, exception)
                        }
                        profileCli.run()

                        break
                    }
                }
            }
            httpCli.run()
        } finally {
            JdbcUtils.executeToList(conn, "set enable_profile=false")
        }
    }
}
