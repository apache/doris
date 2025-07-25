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
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.Http
import org.apache.doris.regression.util.JdbcUtils
import org.apache.doris.regression.util.NodeType

import com.google.common.collect.Maps
import org.awaitility.Awaitility
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import static java.util.concurrent.TimeUnit.SECONDS
import java.util.stream.Collectors
import java.sql.Connection

class ClusterOptions {

    int feNum = 1
    int beNum = 3
    int msNum = 1

    Boolean sqlModeNodeMgr = false
    Boolean beMetaServiceEndpoint = true
    Boolean beClusterId = false

    int waitTimeout = 180

    // don't add whitespace in feConfigs items,
    // for example, ' xx = yy ' is bad, should use 'xx=yy'
    List<String> feConfigs = [
        'heartbeat_interval_second=5',
        'workload_group_check_interval_ms=1000',
    ]

    // don't add whitespace in beConfigs items,
    // for example, ' xx = yy ' is bad, should use 'xx=yy'
    List<String> beConfigs = [
        'max_sys_mem_available_low_water_mark_bytes=0', //no check mem available memory
        'report_disk_state_interval_seconds=2',
        'report_random_wait=false',
        'enable_java_support=false',
    ]

    List<String> msConfigs = []

    List<String> recycleConfigs = []

    // host mapping(host:IP), for example: myhost:192.168.10.10
    // just as `docker run --add-host myhost:192.168.10.10` do.
    List<String> extraHosts = []

    boolean connectToFollower = false

    // 1. cloudMode = true, only create cloud cluster.
    // 2. cloudMode = false, only create none-cloud cluster.
    // 3. cloudMode = null, create both cloud and none-cloud cluster, depend on the running pipeline mode.
    Boolean cloudMode = false

    // in cloud mode, deployment methods are divided into
    // 1. master - multi observers
    // 2. mutli followers - multi observers
    // default use 1
    Boolean useFollowersMode = false

    // each be disks, a disks format is: disk_type=disk_num[,disk_capacity]
    // here disk_type=HDD or SSD,  disk capacity is in gb unit.
    // for example: beDisks = ["HDD=1", "SSD=2,10", "SSD=10,3"] means:
    // each be has 1 HDD disks without capacity limit, 2 SSD disks with 10GB capacity limit,
    // and 10 SSD disks with 3GB capacity limit
    // if not specific, docker will let each be contains 1 HDD disk.
    List<String> beDisks = null

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

class ServerNode {

    int index
    String host
    int httpPort
    boolean alive
    String path

    static void fromCompose(ServerNode node, ListHeader header, int index, List<Object> fields) {
        node.index = index
        node.host = (String) fields.get(header.indexOf('IP'))
        node.httpPort = (int) toLongOrDefault(fields.get(header.indexOf('http_port')), -1)
        node.alive = fields.get(header.indexOf('alive')) == 'true'
        node.path = (String) fields.get(header.indexOf('path'))
    }

    static long toLongOrDefault(Object val, long defValue) {
        return val == '' ? defValue : (long) val
    }

    def getHttpAddress() {
        return [host, httpPort]
    }

    void enableDebugPoint(String name, Map<String, String> params = null) {
        def (host, port) = getHttpAddress()
        DebugPoint.enableDebugPoint(host, port, getNodeType(), name, params)
    }

    void disableDebugPoint(String name) {
        def (host, port) = getHttpAddress()
        DebugPoint.disableDebugPoint(host, port, getNodeType(), name)
    }

    void clearDebugPoints() {
        def (host, port) = getHttpAddress()
        DebugPoint.clearDebugPoints(host, port, getNodeType())
    }

    NodeType getNodeType() {
        assert false : 'Unknown node type'
    }

    String getLogFilePath() {
        assert false : 'Unknown node type'
    }

    String getConfFilePath() {
        assert false : 'Unknown node type'
    }

    String getBasePath() {
        return path
    }
}

class Frontend extends ServerNode {

    int editLogPort
    int queryPort
    boolean isMaster

    static Frontend fromCompose(ListHeader header, int index, List<Object> fields) {
        Frontend fe = new Frontend()
        ServerNode.fromCompose(fe, header, index, fields)
        fe.queryPort = (int) toLongOrDefault(fields.get(header.indexOf('query_port')), -1)
        fe.editLogPort = (int) toLongOrDefault(fields.get(header.indexOf('edit_log_port')), -1)
        fe.isMaster = fields.get(header.indexOf('is_master')) == 'true'
        return fe
    }

    NodeType getNodeType() {
        return NodeType.FE
    }

    String getLogFilePath() {
        return path + '/log/fe.log'
    }

    String getConfFilePath() {
        return path + '/conf/fe.conf'
    }

}

class Backend extends ServerNode {

    int heartbeatPort
    long backendId
    int tabletNum

    static Backend fromCompose(ListHeader header, int index, List<Object> fields) {
        Backend be = new Backend()
        ServerNode.fromCompose(be, header, index, fields)
        be.heartbeatPort = (int) toLongOrDefault(fields.get(header.indexOf('heartbeat_port')), -1)
        be.backendId = toLongOrDefault(fields.get(header.indexOf('backend_id')), -1L)
        be.tabletNum = (int) toLongOrDefault(fields.get(header.indexOf('tablet_num')), 0L)

        return be
    }

    NodeType getNodeType() {
        return NodeType.BE
    }

    String getLogFilePath() {
        return path + '/log/be.INFO'
    }

    String getConfFilePath() {
        return path + '/conf/be.conf'
    }

    String getHeartbeatPort() {
        return heartbeatPort;
    }

}

class MetaService extends ServerNode {

    static MetaService fromCompose(ListHeader header, int index, List<Object> fields) {
        MetaService ms = new MetaService()
        ServerNode.fromCompose(ms, header, index, fields)
        return ms
    }

    NodeType getNodeType() {
        return NodeType.MS
    }

    String getLogFilePath() {
        return path + '/log/meta_service.INFO'
    }

    String getConfFilePath() {
        return path + '/conf/doris_cloud.conf'
    }

}

class Recycler extends ServerNode {

    static Recycler fromCompose(ListHeader header, int index, List<Object> fields) {
        Recycler rs = new Recycler()
        ServerNode.fromCompose(rs, header, index, fields)
        return rs
    }

    NodeType getNodeType() {
        return NodeType.RECYCLER
    }

    String getLogFilePath() {
        return path + '/log/recycler.INFO'
    }

    String getConfFilePath() {
        return path + '/conf/doris_cloud.conf'
    }

}

@Slf4j
@CompileStatic
class SuiteCluster {

    static final Logger logger = LoggerFactory.getLogger(this.class)

    // dockerImpl() will set jdbcUrl
    String jdbcUrl = ""
    final String name
    final Config config
    private boolean running
    private boolean sqlModeNodeMgr = false
    private boolean isCloudMode = false

    SuiteCluster(String name, Config config) {
        this.name = name
        this.config = config
        this.running = false
    }

    void init(ClusterOptions options, boolean isCloud) {
        assert name != null && name != ''
        assert options.feNum > 0 || options.beNum > 0
        assert config.image != null && config.image != ''

        this.isCloudMode = isCloud

        def cmd = [
            'up', name, config.image
        ]

        if (options.feNum > 0) {
            cmd += ['--add-fe-num', String.valueOf(options.feNum)]
        }
        if (options.beNum > 0) {
            cmd += ['--add-be-num', String.valueOf(options.beNum)]
        }
        if (options.msNum > 0) {
            cmd += ['--add-ms-num', String.valueOf(options.msNum)]
        }
        // TODO: need escape white space in config
        if (!options.feConfigs.isEmpty()) {
            cmd += ['--fe-config']
            cmd += options.feConfigs
        }
        if (!options.beConfigs.isEmpty()) {
            cmd += ['--be-config']
            cmd += options.beConfigs
        }
        if (!options.msConfigs.isEmpty()) {
            cmd += ['--ms-config']
            cmd += options.msConfigs
        }
        if (!options.recycleConfigs.isEmpty()) {
            cmd += ['--recycle-config']
            cmd += options.recycleConfigs
        }
        if (options.beDisks != null) {
            cmd += ['--be-disks']
            cmd += options.beDisks
        }
        if (!options.extraHosts.isEmpty()) {
            cmd += ['--extra-hosts']
            cmd += options.extraHosts
        }
        if (config.dockerCoverageOutputDir != null && config.dockerCoverageOutputDir != '') {
            cmd += ['--coverage-dir', config.dockerCoverageOutputDir]
        }
        if (isCloud) {
            cmd += ['--cloud']
        }

        if (isCloud && options.useFollowersMode) {
            cmd += ['--fe-follower']
        }

        if (options.sqlModeNodeMgr) {
            cmd += ['--sql-mode-node-mgr']
        }
        if (!options.beMetaServiceEndpoint) {
            cmd += ['--no-be-metaservice-endpoint']
        }
        if (options.beClusterId) {
            cmd += ['--be-cluster-id']
        }

        cmd += ['--wait-timeout', String.valueOf(options.waitTimeout)]

        sqlModeNodeMgr = options.sqlModeNodeMgr

        runCmd(cmd.join(' '), 180)

        // wait be report disk
        Thread.sleep(5000)

        running = true
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

    Frontend getOneFollowerFe() {
        return getFrontends().stream().filter(fe -> !fe.isMaster).findFirst().orElse(null)
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

    List<MetaService> getAllMetaservices(boolean needAlive = false) {
        return getMetaservices().stream().filter(ms -> ms.alive || !needAlive).collect(Collectors.toList());
    }

    List<MetaService> getAllRecyclers(boolean needAlive = false) {
        return getRecyclers().stream().filter(rc -> rc.alive || !needAlive).collect(Collectors.toList());
    }

    private List<Frontend> getFrontends() {
        def ret = getAllNodes()
        return ret.getV1()
    }

    private List<Backend> getBackends() {
        def ret = getAllNodes()
        return ret.getV2()
    }

    private List<MetaService> getMetaservices() {
        def ret = getAllNodes()
        return ret.getV3()
    }

    private List<Recycler> getRecyclers() {
        def ret = getAllNodes()
        return ret.getV4()
    }

    private Tuple4<List<Frontend>, List<Backend>, List<MetaService>, List<Recycler>> getAllNodes() {
        List<Frontend> frontends = []
        List<Backend> backends = []
        List<MetaService> metaservices = []
        List<Recycler> recyclers = []
        def cmd = 'ls ' + name + ' --detail'
        def data = runCmd(cmd)
        assert data instanceof List
        def rows = (List<List<Object>>) data
        logger.info('get all nodes {}', rows)
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
            } else if (name.startsWith('ms-')) {
                int index = name.substring('ms-'.length()) as int
                metaservices.add(MetaService.fromCompose(header, index, row))
            } else if (name.startsWith('recycle-')) {
                int index = name.substring('recycle-'.length()) as int
                recyclers.add(Recycler.fromCompose(header, index, row))
            } else if (name.startsWith('fdb-')) {
            // current not used
            } else {
                assert false : 'Unknown node type with name: ' + name
            }
        }
        return new Tuple4(frontends, backends, metaservices, recyclers)
    }

    List<Integer> addFrontend(int num, boolean followerMode=false) throws Exception {
        def result = add(num, 0, null, followerMode)
        return result.first
    }

    List<Integer> addBackend(int num, String ClusterName='') throws Exception {
        def result = add(0, num, ClusterName)
        return result.second
    }

    // ATTN: clusterName just used for cloud mode, 1 cluster has n bes
    // ATTN: followerMode just used for cloud mode
    Tuple2<List<Integer>, List<Integer>> add(int feNum, int beNum, String clusterName, boolean followerMode=false) throws Exception {
        assert feNum > 0 || beNum > 0

        def sb = new StringBuilder()
        sb.append('up ' + name + ' ')
        if (feNum > 0) {
            sb.append('--add-fe-num ' + feNum + ' ')
            if (followerMode) {
                sb.append('--fe-follower' + ' ')
            }
            if (sqlModeNodeMgr) {
                sb.append('--sql-mode-node-mgr' + ' ')
            }
        }
        if (beNum > 0) {
            sb.append('--add-be-num ' + beNum + ' ')
            if (clusterName != null && !clusterName.isEmpty()) {
                sb.append(' --be-cluster ' + clusterName + ' ')
            }
        }
        sb.append('--wait-timeout 60')

        def data = (Map<String, Map<String, Object>>) runCmd(sb.toString(), 180)
        def newFrontends = (List<Integer>) data.get('fe').get('add_list')
        def newBackends = (List<Integer>) data.get('be').get('add_list')

        // wait be report disk
        Thread.sleep(5000)

        return new Tuple2(newFrontends, newBackends)
    }

    void destroy(boolean clean) throws Exception {
        try {
            def cmd = 'down ' + name
            if (clean) {
                cmd += ' --clean'
            }
            runCmd(cmd)
        } finally {
            running = false
        }
    }

    boolean isRunning() {
        return running
    }

    boolean isCloudMode() {
        return this.isCloudMode
    }

    int START_WAIT_TIMEOUT = 120
    int STOP_WAIT_TIMEOUT = 60

    // if not specific fe indices, then start all frontends
    void startFrontends(int... indices) {
        runFrontendsCmd(START_WAIT_TIMEOUT + 5, "start  --wait-timeout ${START_WAIT_TIMEOUT}".toString(), indices)
    }

    // if not specific be indices, then start all backends
    void startBackends(int... indices) {
        runBackendsCmd(START_WAIT_TIMEOUT + 5, "start  --wait-timeout ${START_WAIT_TIMEOUT}".toString(), indices)
    }

    // if not specific fe indices, then stop all frontends
    void stopFrontends(int... indices) {
        runFrontendsCmd(STOP_WAIT_TIMEOUT + 5, "stop --wait-timeout ${STOP_WAIT_TIMEOUT}".toString(), indices)
        waitHbChanged()
    }

    // if not specific be indices, then stop all backends
    void stopBackends(int... indices) {
        runBackendsCmd(STOP_WAIT_TIMEOUT + 5, "stop --wait-timeout ${STOP_WAIT_TIMEOUT}".toString(), indices)
        waitHbChanged()
    }

    // if not specific fe indices, then restart all frontends
    void restartFrontends(int... indices) {
        runFrontendsCmd(START_WAIT_TIMEOUT + 5, "restart --wait-timeout ${START_WAIT_TIMEOUT}".toString(), indices)
    }

    // if not specific be indices, then restart all backends
    void restartBackends(int... indices) {
        runBackendsCmd(START_WAIT_TIMEOUT + 5, "restart --wait-timeout ${START_WAIT_TIMEOUT}".toString(), indices)
    }

    // if not specific ms indices, then restart all ms
    void restartMs(int... indices) {
        runMsCmd(START_WAIT_TIMEOUT + 5, "restart --wait-timeout ${START_WAIT_TIMEOUT}".toString(), indices)
    }

    // if not specific recycler indices, then restart all recyclers
    void restartRecyclers(int... indices) {
        runRecyclerCmd(START_WAIT_TIMEOUT + 5, "restart --wait-timeout ${START_WAIT_TIMEOUT}".toString(), indices)
    }

    // if not specific fe indices, then drop all frontends
    void dropFrontends(boolean clean, int... indices) {
        def cmd = 'down'
        if (clean) {
            cmd += ' --clean'
        }
        runFrontendsCmd(60, cmd, indices)
    }

    // if not specific be indices, then decommission all backends
    void decommissionBackends(boolean clean, int... indices) {
        def cmd = 'down'
        if (clean) {
            cmd += ' --clean'
        }
        runBackendsCmd(300, cmd, indices)
    }

    // if not specific be indices, then drop force all backends
    void dropForceBackends(boolean clean, int... indices) {
        def cmd = 'down --drop-force'
        if (clean) {
            cmd += ' --clean'
        }
        runBackendsCmd(60, cmd, indices)
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

    void addRWPermToAllFiles() {
        def cmd = 'add-rw-perm ' + name
        runCmd(cmd)
    }

    private void waitHbChanged() {
        // heart beat interval is 5s
        Thread.sleep(7000)
    }

    private void runFrontendsCmd(int timeoutSecond, String op, int... indices) {
        def cmd = op + ' ' + name + ' --fe-id ' + indices.join(' ')
        runCmd(cmd, timeoutSecond)
    }

    private void runBackendsCmd(int timeoutSecond, String op, int... indices) {
        def cmd = op + ' ' + name + ' --be-id ' + indices.join(' ')
        runCmd(cmd, timeoutSecond)
    }

    private void runMsCmd(int timeoutSecond, String op, int... indices) {
        def cmd = op + ' ' + name + ' --ms-id ' + indices.join(' ')
        runCmd(cmd, timeoutSecond)
    }

    private void runRecyclerCmd(int timeoutSecond, String op, int... indices) {
        def cmd = op + ' ' + name + ' --recycle-id ' + indices.join(' ')
        runCmd(cmd, timeoutSecond)
    }

    private Object runCmd(String cmd, int timeoutSecond = 60) throws Exception {
        def fullCmd = String.format('python -W ignore %s %s -v --output-json', config.dorisComposePath, cmd)
        logger.info('Run doris compose cmd: {}', fullCmd)
        def proc = fullCmd.execute()
        def outBuf = new StringBuilder()
        def errBuf = new StringBuilder()
        Awaitility.await().atMost(timeoutSecond, SECONDS).until({
            proc.waitForProcessOutput(outBuf, errBuf)
            return true
        })
        if (proc.exitValue() != 0) {
            throw new Exception(String.format('Exit value: %s != 0, stdout: %s, stderr: %s',
                                              proc.exitValue(), outBuf.toString(), errBuf.toString()))
        }
        def parser = new JsonSlurper()
        if (outBuf.toString().size() == 0) {
            throw new Exception(String.format('doris compose output is empty, err: %s', errBuf.toString()))
        }
        def object = (Map<String, Object>) parser.parseText(outBuf.toString())
        if (object.get('code') != 0) {
            throw new Exception(String.format('Code: %s != 0, err: %s', object.get('code'), object.get('err')))
        }
        return object.get('data')
    }

}
