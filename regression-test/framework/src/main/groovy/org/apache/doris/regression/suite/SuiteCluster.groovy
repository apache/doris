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

package org.apache.doris.regression.suite

import org.apache.doris.regression.Config

import groovy.json.JsonSlurper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.stream.Collectors

class Frontend {
    int index
    String host
    int queryPort
    boolean alive
    boolean isMaster

    static Frontend fromCompose(Object object) {
        Frontend fe = new Frontend()
        fe.index = object.index
        fe.host = object.host
        fe.queryPort = object.query_port
        fe.alive = object.alive
        fe.isMaster = object.is_master
        return fe
    }
}

class Backend {
    int index
    long backendId
    String host
    boolean alive
    int tabletNum

    static Backend fromCompose(Object object) {
        Backend be = new Backend()
        be.index = object.index
        be.backendId = object.backend_id
        be.host = object.host
        be.alive = object.alive
        be.tabletNum = object.tablet_num
        return be
    }
}

class SuiteCluster {
    static final Logger logger = LoggerFactory.getLogger(this.class)

    final String name
    final Config config
    List<Frontend> frontends = []
    List<Backend> backends = []

    SuiteCluster(String name, Config config) {
        this.name = name
        this.config = config
    }

    Frontend getMasterFe() {
        return frontends.stream().filter(fe -> fe.isMaster).findFirst().orElse(null)
    }

    Frontend getFeById(long id) {
        return frontends.stream().filter(fe -> fe.id == id).findFirst().orElse(null)
    }

    Frontend getFeByIndex(int index) {
        return frontends.stream().filter(fe -> fe.index == index).findFirst().orElse(null)
    }

    Backend getBeById(long id) {
        return backends.stream().filter(be -> be.id == id).findFirst().orElse(null)
    }

    Backend getBeByIndex(int index) {
        return backends.stream().filter(be -> be.index == index).findFirst().orElse(null)
    }

    List<Frontend> getAllFrontends(boolean needAlive = false) {
        return frontends.stream().filter(fe -> fe.alive || !needAlive).collect(Collectors.toList());
    }

    List<Backend> getAllBackends(boolean needAlive = false) {
        return backends.stream().filter(be -> be.alive || !needAlive).collect(Collectors.toList());
    }

    List<Frontend> addFe(int num) throws Exception {
        def result = add(num, 0)
        return result.first
    }

    List<Backend> addBe(int num) throws Exception {
        def result = add(0, num)
        return result.second
    }

    Tuple2<List<Frontend>, List<Backend>> add(int feNum, int beNum) throws Exception {
        assert name != null && name != ""
        assert config.image != null && config.image != ""
        assert feNum > 0 || beNum > 0

        def sb = new StringBuilder()
        sb.append("up " + name + " ")
        sb.append(config.image + " ")
        if (feNum > 0) {
            sb.append("--add-fe-num " + feNum + " ")
        }
        if (beNum > 0) {
            sb.append("--add-be-num " + beNum + " ")
        }
        sb.append("--wait-timeout 180 ")

        def data = runCmd(sb.toString())
        def newFrontends = data.fe.add.stream().map(obj -> Frontend.fromCompose(obj)).collect(Collectors.toList())
        def newBackends = data.be.add.stream().map(obj -> Backend.fromCompose(obj)).collect(Collectors.toList())

        frontends.addAll(newFrontends)
        backends.addAll(newBackends)

        // wait be report disk
        Thread.sleep(5000)

        return new Tuple2(newFrontends, newBackends)
    }

    void destroy() throws Exception {
        def cmd = "down " + name + " --clean"
        runCmd(cmd, 60)
    }

    Object runCmd(String cmd, int timeoutSecond = -1) throws Exception {
        def fullCmd = String.format("python  %s  %s  --output-json", config.dorisComposePath, cmd)
        logger.info("Run doris compose cmd: " + fullCmd)
        def proc = fullCmd.execute()
        def outBuf = new StringBuilder()
        def errBuf = new StringBuilder()
        proc.consumeProcessOutput(outBuf, errBuf)
        if (timeoutSecond > 0) {
            proc.waitForOrKill(timeoutSecond * 1000)
        } else {
            proc.waitFor()
        }
        def out = outBuf.toString()
        def err = errBuf.toString()
        if (proc.exitValue()) {
            throw new Exception(String.format("Exit value: %s != 0, stdout: %s, stderr: %s",
                                              proc.exitValue(), out, err))
        }
        def parser = new JsonSlurper()
        def object = parser.parseText(out)
        assert object instanceof Map
        if (object.code != 0) {
            throw new Exception(String.format("Code: %s != 0, err: %s", object.code, object.err))
        }
        return object.data
    }
}
