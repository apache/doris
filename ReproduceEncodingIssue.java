import java.nio.charset.StandardCharsets;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.CharBuffer;
import java.nio.ByteBuffer;

public class ReproduceEncodingIssue {
    
    public static void main(String[] args) {
        System.out.println("=== 复现Apache HttpClient中文编码问题 ===");
        
        testSetHeaderBehavior();
        testISO88591Encoding();
        testCharsetEncoderReplace();
        simulateHttpSending();
        compareCurlBehavior();
        
        System.out.println("\n=== 总结 ===");
        System.out.println("问题根源: Apache HttpClient使用ISO-8859-1编码发送HTTP头，中文字符被替换为?");
        System.out.println("curl正常: curl直接发送UTF-8字节，不经过ISO-8859-1转换");
        System.out.println("解决方案: 在setHeader前对中文进行URL编码或Base64编码");
    }
    
    // 1. 模拟StreamLoadAction中的setHeader操作
    public static void testSetHeaderBehavior() {
        System.out.println("\n1. 测试 RequestBuilder.setHeader 行为");
        
        String chineseColumns = "OBJECTID,姓名,CREATEDTIME";
        System.out.println("原始中文字符串: " + chineseColumns);
        
        // 显示UTF-8字节
        byte[] utf8Bytes = chineseColumns.getBytes(StandardCharsets.UTF_8);
        System.out.print("UTF-8字节: ");
        for (byte b : utf8Bytes) {
            System.out.printf("0x%02X ", b);
        }
        System.out.println();
        
        // 模拟BasicHeader存储
        System.out.println("BasicHeader存储的值: " + chineseColumns);
        System.out.print("Header值的字符: ");
        for (char c : chineseColumns.toCharArray()) {
            System.out.printf("'%c'(%d) ", c, (int)c);
        }
        System.out.println();
    }
    
    // 2. 模拟ISO-8859-1编码转换
    public static void testISO88591Encoding() {
        System.out.println("\n2. 测试 ISO-8859-1 编码转换");
        
        String chineseColumns = "OBJECTID,姓名,CREATEDTIME";
        System.out.println("原始字符串: " + chineseColumns);
        
        // 模拟Apache HttpClient的编码行为
        try {
            // 尝试将中文字符串按ISO-8859-1编码
            byte[] iso88591Bytes = chineseColumns.getBytes("ISO-8859-1");
            System.out.print("ISO-8859-1字节: ");
            for (byte b : iso88591Bytes) {
                System.out.printf("0x%02X ", b);
            }
            System.out.println();
            
            // 重新解码看结果
            String decodedString = new String(iso88591Bytes, "ISO-8859-1");
            System.out.println("重新解码的字符串: " + decodedString);
            System.out.print("解码后的字符: ");
            for (char c : decodedString.toCharArray()) {
                System.out.printf("'%c'(%d) ", c, (int)c);
            }
            System.out.println();
            
        } catch (Exception e) {
            System.out.println("编码异常: " + e.getMessage());
        }
    }
    
    // 3. 模拟CharsetEncoder的REPLACE行为
    public static void testCharsetEncoderReplace() {
        System.out.println("\n3. 测试 CharsetEncoder REPLACE 行为");
        
        String chineseColumns = "OBJECTID,姓名,CREATEDTIME";
        
        // 创建ISO-8859-1编码器，设置REPLACE策略
        var encoder = Charset.forName("ISO-8859-1").newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);
        
        try {
            // 编码
            CharBuffer charBuffer = CharBuffer.wrap(chineseColumns);
            ByteBuffer byteBuffer = encoder.encode(charBuffer);
            
            byte[] encodedBytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(encodedBytes);
            
            System.out.print("CharsetEncoder编码后的字节: ");
            for (byte b : encodedBytes) {
                System.out.printf("0x%02X ", b);
            }
            System.out.println();
            
            // 解码看结果
            String result = new String(encodedBytes, "ISO-8859-1");
            System.out.println("最终结果字符串: " + result);
            System.out.print("结果字符详情: ");
            for (char c : result.toCharArray()) {
                System.out.printf("'%c'(%d) ", c, (int)c);
            }
            System.out.println();
            
            // 检查是否包含问号
            long questionMarkCount = result.chars().filter(ch -> ch == '?').count();
            System.out.println("问号(?)的数量: " + questionMarkCount);
            
        } catch (Exception e) {
            System.out.println("编码异常: " + e.getMessage());
        }
    }
    
    // 4. 完整模拟HTTP请求发送过程
    public static void simulateHttpSending() {
        System.out.println("\n4. 完整模拟HTTP请求发送过程");
        
        String chineseColumns = "OBJECTID,姓名,CREATEDTIME";
        
        // Step 1: setHeader (存储阶段)
        System.out.println("Step 1 - setHeader存储: " + chineseColumns);
        
        // Step 2: formatHeader (格式化阶段)
        String headerLine = "columns: " + chineseColumns;
        System.out.println("Step 2 - formatHeader结果: " + headerLine);
        
        // Step 3: writeLine (编码发送阶段)
        var encoder = Charset.forName("ISO-8859-1").newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);
        
        try {
            CharBuffer charBuffer = CharBuffer.wrap(headerLine);
            ByteBuffer byteBuffer = encoder.encode(charBuffer);
            
            byte[] finalBytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(finalBytes);
            
            System.out.print("Step 3 - 最终发送的字节: ");
            for (byte b : finalBytes) {
                System.out.printf("0x%02X ", b);
            }
            System.out.println();
            
            // Step 4: 服务器接收解码
            String receivedString = new String(finalBytes, "ISO-8859-1");
            System.out.println("Step 4 - 服务器接收到的字符串: " + receivedString);
            
            // 提取columns值 (去掉"columns: "前缀)
            String columnsValue = receivedString.substring("columns: ".length());
            System.out.println("最终的columns值: " + columnsValue);
            System.out.println("包含问号数量: " + columnsValue.chars().filter(ch -> ch == '?').count());
            
        } catch (Exception e) {
            System.out.println("编码异常: " + e.getMessage());
        }
    }
    
    // 5. 对比curl的行为
    public static void compareCurlBehavior() {
        System.out.println("\n5. 对比curl的行为");
        
        String chineseColumns = "OBJECTID,姓名,CREATEDTIME";
        
        // curl直接发送UTF-8字节
        byte[] curlBytes = chineseColumns.getBytes(StandardCharsets.UTF_8);
        System.out.print("curl发送的UTF-8字节: ");
        for (byte b : curlBytes) {
            System.out.printf("0x%02X ", b);
        }
        System.out.println();
        
        // 服务器按UTF-8解码
        String curlReceived = new String(curlBytes, StandardCharsets.UTF_8);
        System.out.println("curl - 服务器接收到的字符串: " + curlReceived);
        System.out.println("curl - 是否包含问号: " + curlReceived.contains("?"));
    }
}
