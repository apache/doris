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
import org.apache.doris.regression.util.Http

import com.google.common.collect.Maps
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import java.util.stream.Collectors

class ClusterOptions {

    int feNum = 1
    int beNum = 3
    int beDiskNum = 1
    List<String> feConfigs = []
    List<String> beConfigs = []

    void enableDebugPoints() {
        feConfigs.add('enable_debug_points=true')
        beConfigs.add('enable_debug_points=true')
    }

}

class ListHeader {

    Map<String, Integer> fields

    ListHeader(List<Object> fieldList) {
        this.fields = Maps.newHashMap()
        for (int i = 0; i < fieldList.size(); i++) {
            fields.put((String) fieldList.get(i), i)
        }
    }

    int indexOf(String field) {
        def index = fields.get(field)
        assert index != null : 'Not found field: ' + field
        return index
    }

}

enum NodeType {

    FE,
    BE,

}

class ServerNode {

    int index
    String host
    int httpPort
    boolean alive

    static void fromCompose(ServerNode node, ListHeader header, int index, List<Object> fields) {
        node.index = index
        node.host = (String) fields.get(header.indexOf('IP'))
        node.httpPort = (Integer) fields.get(header.indexOf('http_port'))
        node.alive = fields.get(header.indexOf('alive')) == 'true'
    }

    String getHttpAddress() {
        return 'http://' + host + ':' + httpPort
    }

    void enableDebugPoint(String name, Map<String, String> params = null) {
        def url = getHttpAddress() + '/api/debug_point/add/' + name
        if (params != null && params.size() > 0) {
            url += '?' + params.collect((k, v) -> k + '=' + v).join('&')
        }
        def result = Http.http_post(url, null, true)
        checkHttpResult(result)
    }

    void disableDebugPoint(String name) {
        def url = getHttpAddress() + '/api/debug_point/remove/' + name
        def result = Http.http_post(url, null, true)
        checkHttpResult(result)
    }

    void clearDebugPoints() {
        def url = getHttpAddress() + '/api/debug_point/clear'
        def result = Http.http_post(url, null, true)
        checkHttpResult(result)
    }

    private void checkHttpResult(Object result) {
        def type = getNodeType()
        if (type == NodeType.FE) {
            assert result.code == 0 : result.toString()
        } else if (type == NodeType.BE) {
            assert result.status == 'OK' : result.toString()
        }
    }

    NodeType getNodeType() {
        assert false : 'Unknown node type'
    }

}

class Frontend extends ServerNode {

    int queryPort
    boolean isMaster

    static Frontend fromCompose(ListHeader header, int index, List<Object> fields) {
        Frontend fe = new Frontend()
        ServerNode.fromCompose(fe, header, index, fields)
        fe.queryPort = (Integer) fields.get(header.indexOf('query_port'))
        fe.isMaster = fields.get(header.indexOf('is_master')) == 'true'
        return fe
    }

    NodeType getNodeType() {
        return NodeType.FE
    }

}

class Backend extends ServerNode {

    long backendId
    int tabletNum

    static Backend fromCompose(ListHeader header, int index, List<Object> fields) {
        Backend be = new Backend()
        ServerNode.fromCompose(be, header, index, fields)
        be.backendId = (long) fields.get(header.indexOf('backend_id'))
        be.tabletNum = (int) fields.get(header.indexOf('tablet_num'))
        return be
    }

    NodeType getNodeType() {
        return NodeType.BE
    }

}

@Slf4j
@CompileStatic
class SuiteCluster {

    static final Logger logger = LoggerFactory.getLogger(this.class)

    final String name
    final Config config
    private boolean inited

    SuiteCluster(String name, Config config) {
        this.name = name
        this.config = config
        this.inited = false
    }

    void init(ClusterOptions options) {
        if (inited) {
            return
        }

        assert name != null && name != ''
        assert options.feNum > 0 || options.beNum > 0
        assert config.image != null && config.image != ''

        def sb = new StringBuilder()
        sb.append('up ' + name + ' ')
        sb.append(config.image + ' ')
        if (options.feNum > 0) {
            sb.append('--add-fe-num ' + options.feNum + ' ')
        }
        if (options.beNum > 0) {
            sb.append('--add-be-num ' + options.beNum + ' ')
        }
        // TODO: need escape white space in config
        if (options.feConfigs != null && options.feConfigs.size() > 0) {
            sb.append('--fe-config ')
            options.feConfigs.forEach(item -> sb.append(' ' + item + ' '))
        }
        if (options.beConfigs != null && options.beConfigs.size() > 0) {
            sb.append('--be-config ')
            options.beConfigs.forEach(item -> sb.append(' ' + item + ' '))
        }
        sb.append('--be-disk-num ' + options.beDiskNum + ' ')
        sb.append('--wait-timeout 180')

        runCmd(sb.toString(), -1)

        // wait be report disk
        Thread.sleep(5000)

        inited = true
    }

    void injectDebugPoints(NodeType type, Map<String, Map<String, String>> injectPoints) {
        if (injectPoints == null || injectPoints.isEmpty()) {
            return
        }

        List<ServerNode> servers = []
        if (type == NodeType.FE) {
            servers.addAll(getFrontends())
        } else if (type == NodeType.BE) {
            servers.addAll(getBackends())
        } else {
            throw new Exception('Unknown node type: ' + type)
        }

        servers.each { server ->
            injectPoints.each { name, params ->
                server.enableDebugPoint(name, params)
            }
        }
    }

    void clearFrontendDebugPoints() {
        getFrontends().each { it.clearDebugPoints() }
    }

    void clearBackendDebugPoints() {
        getBackends().each { it.clearDebugPoints() }
    }

    Frontend getMasterFe() {
        return getFrontends().stream().filter(fe -> fe.isMaster).findFirst().orElse(null)
    }

    Frontend getFeByIndex(int index) {
        return getFrontends().stream().filter(fe -> fe.index == index).findFirst().orElse(null)
    }

    Backend getBeByBackendId(long backendId) {
        return getBackends().stream().filter(be -> be.backendId == backendId).findFirst().orElse(null)
    }

    Backend getBeByIndex(int index) {
        return getBackends().stream().filter(be -> be.index == index).findFirst().orElse(null)
    }

    List<Frontend> getAllFrontends(boolean needAlive = false) {
        return getFrontends().stream().filter(fe -> fe.alive || !needAlive).collect(Collectors.toList());
    }

    List<Backend> getAllBackends(boolean needAlive = false) {
        return getBackends().stream().filter(be -> be.alive || !needAlive).collect(Collectors.toList());
    }

    private List<Frontend> getFrontends() {
        List<Frontend> frontends = []
        List<Backend> backends = []
        getAllNodes(frontends, backends)
        return frontends
    }

    private List<Backend> getBackends() {
        List<Frontend> frontends = []
        List<Backend> backends = []
        getAllNodes(frontends, backends)
        return backends
    }

    private void getAllNodes(List<Frontend> frontends, List<Backend> backends) {
        def cmd = 'ls ' + name + ' --detail'
        def data = runCmd(cmd)
        assert data instanceof List
        def rows = (List<List<Object>>) data
        def header = new ListHeader(rows.get(0))
        for (int i = 1; i < rows.size(); i++) {
            def row = (List<Object>) rows.get(i)
            def name = (String) row.get(header.indexOf('NAME'))
            if (name.startsWith('be-')) {
                int index = name.substring('be-'.length()) as int
                backends.add(Backend.fromCompose(header, index, row))
            } else if (name.startsWith('fe-')) {
                int index = name.substring('fe-'.length()) as int
                frontends.add(Frontend.fromCompose(header, index, row))
            } else {
                assert false : 'Unknown node type with name: ' + name
            }
        }
    }

    List<Integer> addFrontend(int num) throws Exception {
        def result = add(num, 0)
        return result.first
    }

    List<Integer> addBackend(int num) throws Exception {
        def result = add(0, num)
        return result.second
    }

    Tuple2<List<Integer>, List<Integer>> add(int feNum, int beNum) throws Exception {
        assert feNum > 0 || beNum > 0

        def sb = new StringBuilder()
        sb.append('up ' + name + ' ')
        if (feNum > 0) {
            sb.append('--add-fe-num ' + feNum + ' ')
        }
        if (beNum > 0) {
            sb.append('--add-be-num ' + beNum + ' ')
        }
        sb.append('--wait-timeout 60')

        def data = (Map<String, Map<String, Object>>) runCmd(sb.toString(), -1)
        def newFrontends = (List<Integer>) data.get('fe').get('add_list')
        def newBackends = (List<Integer>) data.get('be').get('add_list')

        // wait be report disk
        Thread.sleep(5000)

        return new Tuple2(newFrontends, newBackends)
    }

    void destroy(boolean clean) throws Exception {
        def cmd = 'down ' + name
        if (clean) {
            cmd += ' --clean'
        }
        runCmd(cmd)
        inited = false
    }

    // if not specific fe indices, then start all frontends
    void startFrontends(int... indices) {
        runFrontendsCmd('start', indices)
        waitHbChanged()
    }

    // if not specific be indices, then start all backends
    void startBackends(int... indices) {
        runBackendsCmd('start', indices)
        waitHbChanged()
    }

    // if not specific fe indices, then stop all frontends
    void stopFrontends(int... indices) {
        runFrontendsCmd('stop', indices)
        waitHbChanged()
    }

    // if not specific be indices, then stop all backends
    void stopBackends(int... indices) {
        runBackendsCmd('stop', indices)
        waitHbChanged()
    }

    // if not specific fe indices, then restart all frontends
    void restartFrontends(int... indices) {
        runFrontendsCmd('restart', indices)
        waitHbChanged()
    }

    // if not specific be indices, then restart all backends
    void restartBackends(int... indices) {
        runBackendsCmd('restart', indices)
        waitHbChanged()
    }

    // if not specific fe indices, then drop all frontends
    void dropFrontends(boolean clean=false, int... indices) {
        def cmd = 'down'
        if (clean) {
            cmd += ' --clean'
        }
        runFrontendsCmd(cmd, indices)
    }

    // if not specific be indices, then decommission all backends
    void decommissionBackends(boolean clean=false, int... indices) {
        def cmd = 'down'
        if (clean) {
            cmd += ' --clean'
        }
        runBackendsCmd(300, cmd, indices)
    }

    // if not specific be indices, then drop force all backends
    void dropForceBackends(boolean clean=false, int... indices) {
        def cmd = 'down --drop-force'
        if (clean) {
            cmd += ' --clean'
        }
        runBackendsCmd(cmd, indices)
    }

    void checkFeIsAlive(int index, boolean isAlive) {
        def fe = getFeByIndex(index)
        assert fe != null : 'frontend with index ' + index + ' not exists!'
        assert fe.alive == isAlive : (fe.alive ? 'frontend with index ' + index + ' still alive'
                : 'frontend with index ' + index + ' dead')
    }

    void checkBeIsAlive(int index, boolean isAlive) {
        def be = getBeByIndex(index)
        assert be != null : 'backend with index ' + index + ' not exists!'
        assert be.alive == isAlive : (be.alive ? 'backend with index ' + index + ' still alive'
                : 'backend with index ' + index + ' dead')
    }

    void checkFeIsExists(int index, boolean isExists) {
        def fe = getFeByIndex(index)
        if (isExists) {
            assert fe != null : 'frontend with index ' + index + ' not exists!'
        } else {
            assert fe == null : 'frontend with index ' + index + ' exists!'
        }
    }

    void checkBeIsExists(int index, boolean isExists) {
        def be = getBeByIndex(index)
        if (isExists) {
            assert be != null : 'backend with index ' + index + ' not exists!'
        } else {
            assert be == null : 'backend with index ' + index + ' exists!'
        }
    }

    private void waitHbChanged() {
        Thread.sleep(6000)
    }

    private void runFrontendsCmd(String op, int... indices) {
        def cmd = op + ' ' + name + ' --fe-id ' + indices.join(' ')
        runCmd(cmd)
    }

    private void runBackendsCmd(Integer timeoutSecond = null, String op, int... indices) {
        def cmd = op + ' ' + name + ' --be-id ' + indices.join(' ')
        if (timeoutSecond == null) {
            runCmd(cmd)
        } else {
            runCmd(cmd, timeoutSecond)
        }
    }

    private Object runCmd(String cmd, int timeoutSecond = 60) throws Exception {
        def fullCmd = String.format('python %s %s --output-json', config.dorisComposePath, cmd)
        logger.info('Run doris compose cmd: {}', fullCmd)
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
            throw new Exception(String.format('Exit value: %s != 0, stdout: %s, stderr: %s',
                                              proc.exitValue(), out, err))
        }
        def parser = new JsonSlurper()
        def object = (Map<String, Object>) parser.parseText(out)
        if (object.get('code') != 0) {
            throw new Exception(String.format('Code: %s != 0, err: %s', object.get('code'), object.get('err')))
        }
        return object.get('data')
    }

}
