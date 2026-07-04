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
    private static final long DEFAULT_PROFILE_WAIT_TIMEOUT_MS = 30000
    private static final long DEFAULT_PROFILE_WAIT_INTERVAL_MS = 500
    private static final String PROFILE_COMPLETE = "Profile Completion State: COMPLETE"

    private String tag
    private Runnable runCallback
    private Closure<String> check
    private SuiteContext context

    ProfileAction(SuiteContext context, String tag) {
        this.context = context
        this.tag = Objects.requireNonNull(tag, "tag can not be null")
    }

    ProfileAction(SuiteContext context) {
        this.context = context
        this.tag = null
    }

    void run(@ClosureParams(value = FromString, options = []) Runnable run) {
        runCallback = run
    }

    void check(
        @ClosureParams(value = FromString, options = ["String, Throwable"]) Closure check) {
        this.check = check
    }

    List getProfileList() {
        def httpCli = new HttpCliAction(context)
        def addr = context.getFeHttpAddress()
        httpCli.endpoint("${addr.hostString}:${addr.port}")
        httpCli.uri("/rest/v1/query_profile")
        httpCli.op("get")
        httpCli.printResponse(false)

        if (context.config.isCloudMode()) {
            httpCli.basicAuthorization(context.config.feCloudHttpUser, context.config.feCloudHttpPassword)
        } else {
            httpCli.basicAuthorization(context.config.feHttpUser, context.config.feHttpPassword)
        }
        List profileData = []
        httpCli.check { code, body ->
            if (code != 200) {
                throw new IllegalStateException("Get profile list failed, code: ${code}, body:\n${body}")
            }

            def jsonSlurper = new JsonSlurper()
            profileData = jsonSlurper.parseText(body).data.rows
        }
        httpCli.run()
        return profileData
    }

    private String normalizeProfileText(String profileText) {
        // Convert HTML entities and actual NBSPs to regular spaces,
        // retain line breaks, and then compress multiple consecutive spaces into a single space.
        profileText = profileText.replace("&nbsp;", " ")
        profileText = profileText.replace("</br>", "\n")
        profileText = profileText.replace('\u00A0' as char, ' ' as char)
        return profileText.replaceAll(" {2,}", " ")
    }

    boolean isProfileReady(String profileText, List<String> requiredContents = []) {
        if (profileText == null || profileText.isEmpty()) {
            return false
        }
        if (!profileText.contains(PROFILE_COMPLETE)) {
            return false
        }
        return requiredContents == null || requiredContents.isEmpty()
                || requiredContents.every { profileText.contains(it) }
    }

    String getProfile(String profileId) {
        def profileCli = new HttpCliAction(context)
        def addr = context.getFeHttpAddress()
        profileCli.endpoint("${addr.hostString}:${addr.port}")
        profileCli.uri("/rest/v1/query_profile/${profileId}")
        profileCli.op("get")
        profileCli.printResponse(false)

        if (context.config.isCloudMode()) {
            profileCli.basicAuthorization(context.config.feCloudHttpUser, context.config.feCloudHttpPassword)
        } else {
            profileCli.basicAuthorization(context.config.feHttpUser, context.config.feHttpPassword)
        }
        def result = [text: ""]
        profileCli.check { profileCode, profileResp ->
            if (profileCode != 200) {
                throw new IllegalStateException("Get profile failed, url: ${"/rest/v1/query_profile/${profileId}"}, code: ${profileCode}, body:\n${profileResp}")
            }

            def jsonSlurper2 = new JsonSlurper()
            def profileText = jsonSlurper2.parseText(profileResp).data
            result.text = normalizeProfileText(profileText)
        }
        profileCli.run()
        return result.text
    }

    String getProfile(String profileId, List<String> requiredContents, long timeoutMs = DEFAULT_PROFILE_WAIT_TIMEOUT_MS,
            long intervalMs = DEFAULT_PROFILE_WAIT_INTERVAL_MS) {
        return waitProfile({ getProfile(profileId) }, requiredContents, "Profile ${profileId}", timeoutMs, intervalMs)
    }

    String waitProfile(Closure<String> profileFetcher, List<String> requiredContents = [],
            String profileDescription = "Profile", long timeoutMs = DEFAULT_PROFILE_WAIT_TIMEOUT_MS,
            long intervalMs = DEFAULT_PROFILE_WAIT_INTERVAL_MS) {
        String profileText = ""
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() <= deadline) {
            String currentProfileText = profileFetcher.call()
            if (currentProfileText != null && !currentProfileText.isEmpty()) {
                profileText = currentProfileText
            }
            if (isProfileReady(currentProfileText, requiredContents)) {
                return currentProfileText
            }
            log.info("{} is not ready, required contents: {}", profileDescription, requiredContents)
            Thread.sleep(intervalMs)
        }
        return profileText
    }

    String getProfileBySql(String sqlPattern, List<String> requiredContents = [],
            long timeoutMs = DEFAULT_PROFILE_WAIT_TIMEOUT_MS, long intervalMs = DEFAULT_PROFILE_WAIT_INTERVAL_MS) {
        String profileId = ""
        String profileText = ""
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() <= deadline) {
            for (final def profileItem in getProfileList()) {
                if (profileItem["Sql Statement"].toString().contains(sqlPattern)) {
                    profileId = profileItem["Profile ID"].toString()
                    long remainingMs = Math.max(1, deadline - System.currentTimeMillis())
                    profileText = getProfile(profileId, requiredContents, remainingMs, intervalMs)
                    if (isProfileReady(profileText, requiredContents)) {
                        return profileText
                    }
                    break
                }
            }
            if (profileId == "") {
                log.info("Profile with sql pattern {} is not found yet", sqlPattern)
            } else {
                log.info("Profile {} with sql pattern {} is not ready", profileId, sqlPattern)
            }
            Thread.sleep(intervalMs)
        }

        if (profileId == "") {
            throw new IllegalStateException("Missing profile with sql pattern: " + sqlPattern)
        }
        return profileText
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
            def addr = context.getFeHttpAddress()
            httpCli.endpoint("${addr.hostString}:${addr.port}")
            httpCli.uri("/rest/v1/query_profile")
            httpCli.op("get")
            httpCli.printResponse(false)

            if (context.config.isCloudMode()) {
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
                def canFindProfile = false;
                for (final def profileItem in profileData) {
                    if (profileItem["Sql Statement"].toString().contains(tag)) {
                        canFindProfile = true
                        def profileId = profileItem["Profile ID"].toString()

                        def profileCli = new HttpCliAction(context)
                        profileCli.endpoint("${addr.hostString}:${addr.port}")
                        profileCli.uri("/rest/v1/query_profile/${profileId}")
                        profileCli.op("get")
                        profileCli.printResponse(false)

                        if (context.config.isCloudMode()) {
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
                if (!canFindProfile) {
                    throw new IllegalStateException("Missing profile with tag: " + tag)
                }
            }
            httpCli.run()
        } finally {
            JdbcUtils.executeToList(conn, "set enable_profile=false")
        }
    }
}
