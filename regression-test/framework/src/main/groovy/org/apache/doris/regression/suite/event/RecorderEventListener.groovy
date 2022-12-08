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

package org.apache.doris.regression.suite.event

import com.google.common.base.Throwables
import com.squareup.okhttp.OkHttpClient
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import net.minidev.json.JSONObject
import org.apache.avro.data.Json
import org.apache.commons.cli.CommandLine
import org.apache.doris.regression.Config
import org.apache.doris.regression.ConfigOptions
import org.apache.doris.regression.suite.ScriptContext
import org.apache.doris.regression.suite.ScriptInfo
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.suite.SuiteInfo
import org.apache.doris.regression.util.LoggerUtils
import org.apache.doris.regression.util.Recorder
import org.apache.tools.ant.util.DateUtils
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.text.SimpleDateFormat
import static org.apache.doris.regression.util.Http.httpPostJson
import static org.apache.doris.regression.util.Http.http_post


@Slf4j
@CompileStatic
class RecorderEventListener implements EventListener {
    private final Recorder recorder

    static String formatNow() {
        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(d)
    }

    static String escape(String str) {
        StringBuilder sb = new StringBuilder()
        char[] chars = str.toCharArray()
        for (int i = 0; i < chars.length; ++i) {
            char c = chars[i]

            switch (c) {
//                case '|': sb.append("||"); break
//                case '\'': sb.append("|'"); break
//                case '[': sb.append("|["); break
//                case ']': sb.append("|]"); break
//                case '\n': sb.append("\n"); break
//                case '\r': sb.append("|r"); break
//                case '\\':
//                    if (i + 5 < chars.length && chars[i + 1] == 'u' && isHex(chars[i + 2]) && isHex(chars[i + 3]) && isHex(chars[i + 4]) && isHex(chars[i + 5])) {
//                        sb.append("|0x")
//                        ++i
//                        break
//                    }
                default: sb.append(c)
            }
        }
        return sb.toString()
    }
    static boolean isHex(char c) {
        return (('0' as char) <= c && c <= ('9' as char)) || (('A' as char) <= c && c <= ('F' as char)) || (('a' as char) <= c && c <= ('f' as char))
    }




    RecorderEventListener(Recorder recorder) {
        this.recorder = recorder
    }

    @Override
    void onScriptStarted(ScriptContext scriptContext) {

    }

    @Override
    void onScriptFailed(ScriptContext scriptContext, Throwable t) {
        recorder.onFatal(new ScriptInfo(scriptContext.file))
    }

    @Override
    void onScriptFinished(ScriptContext scriptContext, long elapsed) {

    }

    @Override
    void onSuiteStarted(SuiteContext suiteContext) {

    }

    @Override
    void onSuiteFailed(SuiteContext suiteContext, Throwable t,Config config) {
        String timestamp = formatNow()
        log.info("every time when suite failed this will be called")
        def (Integer errorLine, String errorInfo) = LoggerUtils.getErrorInfo(t, suiteContext.scriptContext.file)
        String errorMsg = errorInfo == null
                ? "Exception in ${suiteContext.scriptContext.name}:"
                : "Exception in ${suiteContext.scriptContext.name}(line ${errorLine}):\n\n${errorInfo}\n\nException:"
        def stackTrace = Throwables.getStackTraceAsString(t)

        // these params delivered by teamcity settings after run-regression-test.sh
        log.info(config.repo.toString())
        def repo = config.repo.toString()
        log.info(config.testBranch.toString())
        def testBranch=config.testBranch.toString()
        log.info(config.serverUrl.toString())
        def serverUrl = config.serverUrl.toString()
        log.info(config.pipelineId.toString())
        def pipelineId = config.pipelineId.toString()
        log.info(config.buildId.toString())
        def buildId = config.buildId.toString()
        def teamcityLink = "http://${serverUrl}/buildConfiguration/${pipelineId}/${buildId}?buildTab=tests"

        // these params id defined in regression-conf.groovy
        log.info(config.feHttpAddress.toString())
        def feInfo = config.feHttpAddress.toString()
        log.info(config.jdbcUrl.toString()[13..26])
        def beInfo =config.jdbcUrl.toString()[13..26]

        if(suiteContext.config.otherConfigs.get("alertOn").toString().equalsIgnoreCase("on")) {
            def webhook = suiteContext.config.otherConfigs.get("webhook").toString()
            if(!webhook.isEmpty()){
                def params = new HashMap();
                params.put("msg_type","post")

                def colList = new ArrayList()

                def timeCol = new HashMap()
                timeCol.put("tag","text")
                timeCol.put("text","【TIME】:  ${timestamp} \n")
                colList.add(timeCol)

                def clickCol = new HashMap()
                clickCol.put("tag","text")
                clickCol.put("text","【CLICK】: ")
                colList.add(clickCol)

                def teamcityLinkCol = new HashMap()
                teamcityLinkCol.put("tag","a")
                teamcityLinkCol.put("text","teamcity url\n")
                teamcityLinkCol.put("href",teamcityLink)
                colList.add(teamcityLinkCol)


                def failedSuiteCol = new HashMap()
                failedSuiteCol.put("tag","text")
                failedSuiteCol.put("text","【FailedSuite】:  ${suiteContext.flowName}\n")
                colList.add(failedSuiteCol)

                def repoCol = new HashMap()
                repoCol.put("tag","text")
                repoCol.put("text","【REPO】:  ${repo}\n")
                colList.add(repoCol)

                def branchCol = new HashMap()
                branchCol.put("tag","text")
                branchCol.put("text","【BRANCH】:  ${testBranch}\n")
                colList.add(branchCol)

                def beInfoCol = new HashMap()
                beInfoCol.put("tag","text")
                beInfoCol.put("text","【BEINFO】:  ${beInfo}\n")
                colList.add(beInfoCol)
                def feInfoCol = new HashMap()
                feInfoCol.put("tag","text")
                feInfoCol.put("text","【FEINFO】:  ${feInfo}\n\n")
                colList.add(feInfoCol)

                def detailMessageCol = new HashMap()
                detailMessageCol.put("tag","text")
                detailMessageCol.put("text","【DETAIL】:  \n${escape(errorMsg)} \n\n")
                colList.add(detailMessageCol)


                def bestStackTrace = "${escape(stackTrace)}".toString().split("\t")[0]

                def stackTraceCol = new HashMap()
                stackTraceCol.put("tag","text")
                stackTraceCol.put("text","【STACKTRACE】:  \n${bestStackTrace} \n")
                colList.add(stackTraceCol)

                def outArrayList = new ArrayList()
                outArrayList.add(colList)

                def zhcnMap = new HashMap()
                zhcnMap.put("title","ALERT")
                zhcnMap.put("content",outArrayList)

                def postMap = new HashMap()
                postMap.put("zh_cn",zhcnMap)

                def contentMap = new HashMap()
                contentMap.put("post",postMap)

                params.put("content", contentMap)

                def res = httpPostJson(webhook.toString(), JsonOutput.toJson(params))

                log.info(res.toString())
            }
        }
        else {
            log.info("alertOn is close")
        }

        recorder.onFailure(new SuiteInfo(suiteContext.scriptContext.file, suiteContext.group, suiteContext.suiteName))
    }

    @Override
    void onSuiteFinished(SuiteContext suiteContext, boolean success, long elapsed) {
        if (success) {
            recorder.onSuccess(new SuiteInfo(suiteContext.scriptContext.file, suiteContext.group, suiteContext.suiteName))
        }
    }

    @Override
    void onThreadStarted(SuiteContext suiteContext) {

    }

    @Override
    void onThreadFailed(SuiteContext suiteContext, Throwable t) {
        log.info("every time when thread failed this will be called")

        def webhook = suiteContext.config.otherConfigs.get("webhook").toString()

        def params = new HashMap()
        params.put("msg_type","text")
        def content = new HashMap()
        content.put("text","thread failed, please pay attention! }")

        params.put("content",content)

        def res = httpPostJson(webhook.toString(), JsonOutput.toJson(params))

        log.info(res.toString())
    }

    @Override
    void onThreadFinished(SuiteContext suiteContext, long elapsed) {

    }

}

