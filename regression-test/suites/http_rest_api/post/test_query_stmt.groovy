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
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import javax.net.ssl.*
import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.X509Certificate

/**
*   @Params url is "/xxx", data is request body
*   @Return response body
*/
def http_post = { url, data = null ->
    def protocol = (context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ? "https" : "http"
    def dst = "${protocol}://"+ context.config.feHttpAddress

    // 配置 SSLContext
    if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false ){
        def tlsVerifyMode = context.config.otherConfigs.get("tlsVerifyMode").toLowerCase()
        SSLContext sslContext = SSLContext.getInstance("TLS")
        def trustManagers = null
        def keyManagers = null

        if (tlsVerifyMode == 'none') {
            trustManagers = [ [ checkClientTrusted: { c, a -> },
                                checkServerTrusted: { c, a -> },
                                getAcceptedIssuers: { [] as X509Certificate[] } ] as X509TrustManager ] as TrustManager[]
        } else {
            // 加载 TrustStore (CA 证书)
            def trustStorePath = context.config.otherConfigs.get("trustStorePath")
            def trustStorePwd = context.config.otherConfigs.get("trustStorePassword")

            def trustStore = KeyStore.getInstance(context.config.otherConfigs.get("keyStoreType"))
            trustStore.load(new FileInputStream(trustStorePath), trustStorePwd.toCharArray())
            def tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            tmf.init(trustStore)
            trustManagers = tmf.trustManagers

            if (tlsVerifyMode == 'strict') {
                // 加载客户端证书 (mTLS)
                def keyStorePath = context.config.otherConfigs.get("keyStorePath")
                def keyStorePwd = context.config.otherConfigs.get("keyStorePassword")

                def keyStore = KeyStore.getInstance(context.config.otherConfigs.get("keyStoreType"))
                keyStore.load(new FileInputStream(keyStorePath), keyStorePwd.toCharArray())
                def kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                kmf.init(keyStore, keyStorePwd.toCharArray())
                keyManagers = kmf.keyManagers
            }
        }
        sslContext.init(keyManagers, trustManagers, new SecureRandom())
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.socketFactory)

        if ( tlsVerifyMode == "none") {
            // 关闭 Hostname 验证
            HttpsURLConnection.setDefaultHostnameVerifier({ hostname, session -> true })
        }
    }

    def conn = new URL(dst + url).openConnection()
    logger.info("request: " + dst + url)
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("Authorization", "Basic cm9vdDo=")
    if (data) {
        // 
        logger.info("query body: " + data)
        conn.doOutput = true
        def writer = new OutputStreamWriter(conn.outputStream)
        writer.write(data)
        writer.flush()
        writer.close()
    }
    return conn.content.text
}

def SUCCESS_MSG = "success"
def SUCCESS_CODE = 0

class Stmt {
    String stmt
}
suite("test_query_stmt") {
    def url= "/api/query/default_cluster/" + context.config.defaultDb

    // test select
    def stmt1 = """ select * from numbers('number' = '10') """ 
    def stmt1_json = JsonOutput.toJson(new Stmt(stmt: stmt1));

    def resJson = http_post(url, stmt1_json)
    def obj = new JsonSlurper().parseText(resJson)
    logger.info("the res is " + obj.toString())
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)
    def data = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]];
    assertEquals(obj.data.data, data)

    def tableName = "test_query_stmt"
    def stmt_drop_table =  """ DROP TABLE IF EXISTS ${tableName} """
    def stmt_drop_table_json = JsonOutput.toJson(new Stmt(stmt: stmt_drop_table));
    resJson = http_post(url, stmt_drop_table_json)
    obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    // test create table if not exists
    def stmt2 = """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            id TINYINT,
            name CHAR(10) NOT NULL DEFAULT "zs"
        )
        COMMENT "test query_stmt table"
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num" = "1");
    """
    def stmt2_json = JsonOutput.toJson(new Stmt(stmt: stmt2));
    resJson = http_post(url, stmt2_json)
    obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    // test insert
    def stmt3 = " insert into ${tableName} (id,name) values (1,'aa'),(2,'bb'),(3,'cc')"
    def stmt3_json = JsonOutput.toJson(new Stmt(stmt: stmt3));
    resJson = http_post(url, stmt3_json)
    obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    // test select
    def stmt4 = " select * from ${tableName}"
    def stmt4_json = JsonOutput.toJson(new Stmt(stmt: stmt4));
    resJson = http_post(url, stmt4_json)
    // println(resJson)
    obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)
    // we can only check the number is correctly
    assertEquals(obj.data.data.size, 3)

    url = "/api/query_schema/default_cluster/" + context.config.defaultDb
    def stmt5 = " select * from ${tableName}"
    def resValue = http_post(url, stmt5)
    assertTrue(resValue.contains("CREATE TABLE"))
}

