#!/usr/bin/env groovy

// 测试Apache HttpClient对Unicode字符的处理
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.CloseableHttpClient
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

def testHttpClientUnicodeHandling() {
    println "=== 测试Apache HttpClient Unicode处理 ==="
    
    // 测试字符串
    def chineseColumns = "OBJECTID,流程编号,发起人,发起日期,部门"
    println "原始中文列名: ${chineseColumns}"
    
    // 1. 测试默认HttpClient
    println "\n1. 测试默认HttpClient:"
    def client1 = HttpClients.createDefault()
    def request1 = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load")
        .setHeader("columns", chineseColumns)
        .build()
    
    def headers1 = request1.getAllHeaders()
    headers1.each { header ->
        if (header.name == "columns") {
            println "Header value: ${header.value}"
            def bytes = header.value.getBytes(StandardCharsets.UTF_8)
            def hexString = bytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
            println "UTF-8 bytes: ${hexString}"
            
            def questionCount = header.value.count("?")
            println "Question mark count: ${questionCount}"
        }
    }
    
    // 2. 测试配置了UTF-8的HttpClient
    println "\n2. 测试配置UTF-8的HttpClient:"
    def config = RequestConfig.custom()
        .setConnectTimeout(5000)
        .setSocketTimeout(5000)
        .build()
    
    def client2 = HttpClients.custom()
        .setDefaultRequestConfig(config)
        .build()
    
    def request2 = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load")
        .setHeader("columns", chineseColumns)
        .setHeader("Content-Type", "text/plain; charset=UTF-8")
        .build()
    
    def headers2 = request2.getAllHeaders()
    headers2.each { header ->
        if (header.name == "columns") {
            println "Header value: ${header.value}"
            def bytes = header.value.getBytes(StandardCharsets.UTF_8)
            def hexString = bytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
            println "UTF-8 bytes: ${hexString}"
            
            def questionCount = header.value.count("?")
            println "Question mark count: ${questionCount}"
        }
    }
    
    // 3. 测试URL编码
    println "\n3. 测试URL编码:"
    def urlEncoded = URLEncoder.encode(chineseColumns, StandardCharsets.UTF_8)
    println "URL编码后: ${urlEncoded}"
    
    def request3 = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load")
        .setHeader("columns", urlEncoded)
        .build()
    
    def headers3 = request3.getAllHeaders()
    headers3.each { header ->
        if (header.name == "columns") {
            println "Header value: ${header.value}"
            def bytes = header.value.getBytes(StandardCharsets.UTF_8)
            def hexString = bytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
            println "UTF-8 bytes: ${hexString}"
        }
    }
    
    // 4. 测试不同的中文字符
    println "\n4. 测试不同中文字符:"
    def testChars = ["流程", "编号", "发起", "人", "部门"]
    testChars.each { char ->
        def charBytes = char.getBytes(StandardCharsets.UTF_8)
        def charHex = charBytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
        println "${char}: ${charHex}"
        
        // 测试在HTTP头部中的表现
        def testRequest = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load")
            .setHeader("test-column", char)
            .build()
        
        def testHeaders = testRequest.getAllHeaders()
        testHeaders.each { header ->
            if (header.name == "test-column") {
                def testBytes = header.value.getBytes(StandardCharsets.UTF_8)
                def testHex = testBytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
                println "  HTTP头部: ${testHex}"
                
                if (testHex != charHex) {
                    println "  警告: 字节内容发生变化!"
                }
            }
        }
    }
}

// 运行测试
testHttpClientUnicodeHandling()
