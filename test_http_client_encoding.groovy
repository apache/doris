#!/usr/bin/env groovy

import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import java.net.URLEncoder

// 测试Apache HttpClient对中文HTTP头部的处理
def testHttpClientEncoding() {
    println "=== 测试Apache HttpClient中文编码处理 ==="
    
    // 测试字符串
    def chineseColumns = "OBJECTID,流程编号,发起人,发起日期,部门"
    println "原始中文列名: ${chineseColumns}"
    
    // 1. 直接设置中文头部（不编码）
    println "\n1. 直接设置中文头部:"
    def requestBuilder1 = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load")
    requestBuilder1.setHeader("columns", chineseColumns)
    
    // 获取头部值并打印字节
    def headers1 = requestBuilder1.build().getAllHeaders()
    headers1.each { header ->
        if (header.name == "columns") {
            println "Header value: ${header.value}"
            def bytes = header.value.getBytes("UTF-8")
            def hexString = bytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
            println "UTF-8 bytes: ${hexString}"
            
            // 检查是否包含问号
            def questionMarkCount = header.value.count("?")
            println "Question mark count: ${questionMarkCount}"
        }
    }
    
    // 2. URL编码后设置头部
    println "\n2. URL编码后设置头部:"
    def encodedColumns = URLEncoder.encode(chineseColumns, "UTF-8")
    println "URL编码后: ${encodedColumns}"
    
    def requestBuilder2 = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load")
    requestBuilder2.setHeader("columns", encodedColumns)
    
    def headers2 = requestBuilder2.build().getAllHeaders()
    headers2.each { header ->
        if (header.name == "columns") {
            println "Header value: ${header.value}"
            def bytes = header.value.getBytes("UTF-8")
            def hexString = bytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
            println "UTF-8 bytes: ${hexString}"
        }
    }
    
    // 3. 测试不同编码方式
    println "\n3. 测试不同编码方式:"
    def testStrings = [
        "流程编号",
        "发起人", 
        "部门"
    ]
    
    testStrings.each { str ->
        println "测试字符串: ${str}"
        
        // 直接设置
        def directBytes = str.getBytes("UTF-8")
        def directHex = directBytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
        println "  直接UTF-8字节: ${directHex}"
        
        // URL编码
        def urlEncoded = URLEncoder.encode(str, "UTF-8")
        def urlBytes = urlEncoded.getBytes("UTF-8")
        def urlHex = urlBytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
        println "  URL编码后字节: ${urlHex}"
        
        // 检查是否包含问号
        def questionCount = str.count("?")
        println "  问号数量: ${questionCount}"
    }
}

// 运行测试
testHttpClientEncoding()
