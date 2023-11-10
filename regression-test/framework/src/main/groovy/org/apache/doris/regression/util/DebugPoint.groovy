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
package org.apache.doris.regression.util
import org.apache.doris.regression.util.Http
import org.codehaus.groovy.runtime.IOGroovyMethods
import org.apache.doris.regression.suite.Suite

enum NodeType {
    FE,
    BE,
}

class DebugPoint {
    Suite suite

    DebugPoint(Suite suite) {
        this.suite = suite
    }

    /* Enable debug point in regression
     * Note: set BE config::enable_debug_points = true to take effect
     * Parameters:
     *    host:        hostname or ip of target node
     *    httpPort:    http port of target node
     *    type:        NodeType.BE or NodeType.FE
     *    name:        debug point name
     *    params:      timeout, execute, or other customized input params
     */
    static def enableDebugPoint(String host, String httpPort, NodeType type, String name, Map<String, String> params = null) {
        def url = 'http://' + host + ':' + httpPort + '/api/debug_point/add/' + name
        if (params != null && params.size() > 0) {
            url += '?' + params.collect((k, v) -> k + '=' + v).join('&')
        }
        def result = Http.http_post(url, null, true)
        checkHttpResult(result, type)
    }

    /* Disable debug point in regression
     * Parameters:
     *    host:        hostname or ip of target node
     *    httpPort:    http port of target node
     *    type:        NodeType.BE or NodeType.FE
     *    name:        debug point name
     */
    static def disableDebugPoint(String host, String httpPort, NodeType type, String name) {
        def url = 'http://' + host + ':' + httpPort + '/api/debug_point/remove/' + name
        def result = Http.http_post(url, null, true)
        checkHttpResult(result, type)
    }

    /* Disable all debug points in regression
     * Parameters:
     *    host:        hostname or ip of target node
     *    httpPort:    http port of target node
     *    type:        NodeType.BE or NodeType.FE
     */
    static def clearDebugPoints(String host, String httpPort, NodeType type) {
        def url = 'http://' + host + ':' + httpPort + '/api/debug_point/clear'
        def result = Http.http_post(url, null, true)
        checkHttpResult(result, type)
    }

    def operateDebugPointForAllBEs(Closure closure) {
        def ipList = [:]
        def portList = [:]
        (ipList, portList) = getBEHostAndHTTPPort()
        ipList.each { beid, ip ->
            closure.call(ip, portList[beid])
        }
    }

    /* Enable specific debug point for all BE node in cluster */
    def enableDebugPointForAllBEs(String name, Map<String, String> params = null) {
        operateDebugPointForAllBEs({ host, port ->
            println "enable debug point $name for BE $host:$port"
            enableDebugPoint(host, port, NodeType.BE, name, params)
        })
    }

    /* Disable specific debug point for all BE node in cluster */
    def disableDebugPointForAllBEs(String name) {
        operateDebugPointForAllBEs { host, port ->
            disableDebugPoint(host, port, NodeType.BE, name)
        }
    }

    /* Disable all debug points for all BE node in cluster */
    def clearDebugPointsForAllBEs() {
        operateDebugPointForAllBEs { host, port ->
            clearDebugPoints(host, port, NodeType.BE)
        }
    }

    def getBEHostAndHTTPPort() {
        def ipList = [:]
        def portList = [:]
        suite.getBackendIpHttpPort(ipList, portList)
        return [ipList, portList]
    }

    def getFEHostAndHTTPPort() {
        assert false : 'not implemented yet'
    }

    def enableDebugPointForAllFEs(String name, Map<String, String> params = null) {
        assert false : 'not implemented yet'
    }

    def disableDebugPointForAllFEs(String name) {
        assert false : 'not implemented yet'
    }

    def clearDebugPointsForAllFEs() {
        assert false : 'not implemented yet'
    }

    static void checkHttpResult(Object result, NodeType type) {
        if (type == NodeType.FE) {
            assert result.code == 0 : result.toString()
        } else if (type == NodeType.BE) {
            assert result.status == 'OK' : result.toString()
        }
    }
}

