#!/usr/bin/env groovy

// 模拟回归测试框架的HTTP请求构造过程
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import java.net.URLEncoder

def debugHttpEncoding() {
    println "=== 调试HTTP编码问题 ==="
    
    // 模拟回归测试中的columns设置
    def columns = "OBJECTID,流程编号,发起人,发起日期,部门,发起人电话,转移原因,详细原因描述,转移方式,接受区域,接受明细区域,接受人"
    println "原始columns: ${columns}"
    
    // 1. 检查字符串本身的编码
    println "\n1. 字符串编码检查:"
    def bytes = columns.getBytes("UTF-8")
    def hexString = bytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
    println "UTF-8字节: ${hexString}"
    
    // 检查问号数量
    def questionCount = columns.count("?")
    println "问号数量: ${questionCount}"
    
    // 2. 模拟StreamLoadAction的set方法
    println "\n2. 模拟StreamLoadAction.set()方法:"
    def headers = [:]
    headers.put("columns", columns)
    println "headers map: ${headers}"
    
    // 3. 模拟prepareRequestHeader方法
    println "\n3. 模拟prepareRequestHeader方法:"
    def requestBuilder = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load")
    
    // 模拟headers设置过程
    headers.each { key, value ->
        println "设置header: ${key} = ${value}"
        requestBuilder.setHeader(key, value)
    }
    
    // 4. 检查最终HTTP请求的头部
    println "\n4. 检查最终HTTP请求头部:"
    def builtRequest = requestBuilder.build()
    def allHeaders = builtRequest.getAllHeaders()
    
    allHeaders.each { header ->
        if (header.name == "columns") {
            println "最终header值: ${header.value}"
            def finalBytes = header.value.getBytes("UTF-8")
            def finalHex = finalBytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
            println "最终UTF-8字节: ${finalHex}"
            
            def finalQuestionCount = header.value.count("?")
            println "最终问号数量: ${finalQuestionCount}"
            
            // 比较原始和最终
            if (finalHex != hexString) {
                println "警告: 字节内容发生了变化!"
                println "原始: ${hexString}"
                println "最终: ${finalHex}"
            }
        }
    }
    
    // 5. 测试URL编码
    println "\n5. 测试URL编码:"
    def urlEncoded = URLEncoder.encode(columns, "UTF-8")
    println "URL编码后: ${urlEncoded}"
    
    def urlBytes = urlEncoded.getBytes("UTF-8")
    def urlHex = urlBytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
    println "URL编码UTF-8字节: ${urlHex}"
    
    // 6. 测试不同的中文字符
    println "\n6. 测试不同中文字符:"
    def testChars = ["流程", "编号", "发起", "人", "部门"]
    testChars.each { char ->
        def charBytes = char.getBytes("UTF-8")
        def charHex = charBytes.collect { String.format("%02x", it & 0xFF) }.join(" ")
        println "${char}: ${charHex}"
    }
}

// 运行调试
debugHttpEncoding()
