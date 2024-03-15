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
//
// This file is copy from selectdb-core.
package org.apache.doris.regression.util

import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.junit.Assert
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

import java.nio.charset.Charset

@Slf4j
class Http {

    final static Logger logger = LoggerFactory.getLogger(this.class)

    static void checkHttpResult(Object result, NodeType type) {
        if (type == NodeType.FE) {
            assert result.code == 0 : result.toString()
        } else if (type == NodeType.BE) {
            assert result.status == 'OK' : result.toString()
        }
    }

    static Object GET(url, isJson = false) {
        def conn = new URL(url).openConnection()
        conn.setRequestMethod('GET')
        conn.setRequestProperty('Authorization', 'Basic cm9vdDo=') //token for root
        def code = conn.responseCode
        def text = conn.content.text
        logger.info("http post url=${url}, isJson=${isJson}, response code=${code}, text=${text}")
        Assert.assertEquals(200, code)
        if (isJson) {
            def json = new JsonSlurper()
            def result = json.parseText(text)
            return result
        } else {
            return text
        }
    }

    static Object POST(url, data = null, isJson = false) {
        def conn = new URL(url).openConnection()
        conn.setRequestMethod('POST')
        conn.setRequestProperty('Authorization', 'Basic cm9vdDo=') //token for root
        if (data) {
            if (isJson) {
                conn.setRequestProperty('Content-Type', 'application/json')
                data = JsonOutput.toJson(data)
            }
            // Output request parameters
            conn.doOutput = true
            def writer = new OutputStreamWriter(conn.outputStream)
            writer.write(data)
            writer.flush()
            writer.close()
        }
        def code = conn.responseCode
        def text = conn.content.text
        logger.info("http post url=${url}, data=${data}, isJson=${isJson}, response code=${code}, text=${text}")
        Assert.assertEquals(200, code)
        if (isJson) {
            def json = new JsonSlurper()
            def result = json.parseText(text)
            return result
        } else {
            return text
        }
    }

    public static String httpJson(int ms, String url, String json) throws Exception{
        String err = '00', line = null
        StringBuilder sb = new StringBuilder()
        HttpURLConnection conn = null
        BufferedWriter out = null
        BufferedReader inB = null
        try {
            conn = (HttpURLConnection) (new URL(url.replaceAll('／','/'))).openConnection()
            conn.setRequestMethod('POST')
            conn.setDoOutput(true)
            conn.setDoInput(true)
            conn.setUseCaches(false)
            conn.setConnectTimeout(ms)
            conn.setReadTimeout(ms)
            conn.setRequestProperty('Content-Type', 'application/json;charset=utf-8')
            conn.connect()
            out = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), 'utf-8'))
            out.write(new String(json.getBytes(), 'utf-8'))
            out.flush()
            int code = conn.getResponseCode()
            if (conn.getResponseCode() == 200) {
                inB = new BufferedReader(new InputStreamReader(conn.getInputStream(), 'UTF-8'))
                while ((line = inB.readLine()) != null) {
                    sb.append(line)
                }
            }
        } catch (Exception ex) {
            err = ex.getMessage()
        }
        try { if (out != null) { out.close() } } catch (Exception ex) { }
        try { if (inB != null) { inB.close() } } catch (Exception ex) { }
        try { if (conn != null) { conn.disconnect() } } catch (Exception ex) { }
        if (!err.equals('00')) { throw new Exception(err) }
        return sb.toString()
    }

    public static String sendPost(String url, String param) {
        OutputStreamWriter out = null
        BufferedReader inB = null
        StringBuilder result = new StringBuilder('')
        try {
            URL realUrl = new URL(url)
            URLConnection conn = realUrl.openConnection()
            conn.setRequestProperty('Content-Type', 'application/json;charset=UTF-8')
            conn.setRequestProperty('accept', '*/*')
            conn.setDoOutput(true)
            conn.setDoInput(true)
            out = new OutputStreamWriter(conn.getOutputStream(), 'UTF-8')
            out.write(param)
            out.flush()
            inB = new BufferedReader(new InputStreamReader(conn.getInputStream(), 'UTF-8'))
            String line
            while ((line = inB.readLine()) != null) {
                result.append(line)
            }
        } catch (Exception e) {
            System.out.println('post exception' + e)
            e.printStackTrace()
        } finally {
            if (out != null) { try { out.close() } catch (Exception ex) { } }
            if (inB != null) { try { inB.close() } catch (Exception ex) { } }
        }
        return result.toString()
    }

    public static String httpPostJson(String url, String json) throws Exception{
        String data = ''
        CloseableHttpClient httpClient = null
        CloseableHttpResponse response = null
        try {
            httpClient = HttpClients.createDefault()
            HttpPost httppost = new HttpPost(url)
            httppost.setHeader('Content-Type', 'application/json;charset=UTF-8')
            StringEntity se = new StringEntity(json, Charset.forName('UTF-8'))
            se.setContentType('text/json')
            se.setContentEncoding('UTF-8')
            httppost.setEntity(se)
            response = httpClient.execute(httppost)
            int code = response.getStatusLine().getStatusCode()
            System.out.println('res statusCode:' + code)
            data = EntityUtils.toString(response.getEntity(), 'utf-8')
            EntityUtils.consume(response.getEntity())
        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            if (response != null) { try { response.close() } catch (IOException e) { } }
            if (httpClient != null) { try { httpClient.close() } catch (IOException e) { } }
        }
        return data
    }

}
