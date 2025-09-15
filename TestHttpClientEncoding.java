import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class TestHttpClientEncoding {
    public static void main(String[] args) {
        System.out.println("=== 测试Apache HttpClient中文编码处理 ===");
        
        // 测试字符串
        String chineseColumns = "OBJECTID,流程编号,发起人,发起日期,部门";
        System.out.println("原始中文列名: " + chineseColumns);
        
        // 1. 直接设置中文头部（不编码）
        System.out.println("\n1. 直接设置中文头部:");
        RequestBuilder requestBuilder1 = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load");
        requestBuilder1.setHeader("columns", chineseColumns);
        
        // 获取头部值并打印字节
        org.apache.http.Header[] headers1 = requestBuilder1.build().getAllHeaders();
        for (org.apache.http.Header header : headers1) {
            if ("columns".equals(header.getName())) {
                System.out.println("Header value: " + header.getValue());
                byte[] bytes = header.getValue().getBytes(StandardCharsets.UTF_8);
                StringBuilder hexString = new StringBuilder();
                for (byte b : bytes) {
                    hexString.append(String.format("%02x ", b & 0xFF));
                }
                System.out.println("UTF-8 bytes: " + hexString.toString().trim());
                
                // 检查是否包含问号
                long questionMarkCount = header.getValue().chars().filter(ch -> ch == '?').count();
                System.out.println("Question mark count: " + questionMarkCount);
            }
        }
        
        // 2. URL编码后设置头部
        System.out.println("\n2. URL编码后设置头部:");
        String encodedColumns = URLEncoder.encode(chineseColumns, StandardCharsets.UTF_8);
        System.out.println("URL编码后: " + encodedColumns);
        
        RequestBuilder requestBuilder2 = RequestBuilder.put("http://localhost:8030/api/test/test/_stream_load");
        requestBuilder2.setHeader("columns", encodedColumns);
        
        org.apache.http.Header[] headers2 = requestBuilder2.build().getAllHeaders();
        for (org.apache.http.Header header : headers2) {
            if ("columns".equals(header.getName())) {
                System.out.println("Header value: " + header.getValue());
                byte[] bytes = header.getValue().getBytes(StandardCharsets.UTF_8);
                StringBuilder hexString = new StringBuilder();
                for (byte b : bytes) {
                    hexString.append(String.format("%02x ", b & 0xFF));
                }
                System.out.println("UTF-8 bytes: " + hexString.toString().trim());
            }
        }
        
        // 3. 测试不同编码方式
        System.out.println("\n3. 测试不同编码方式:");
        String[] testStrings = {"流程编号", "发起人", "部门"};
        
        for (String str : testStrings) {
            System.out.println("测试字符串: " + str);
            
            // 直接设置
            byte[] directBytes = str.getBytes(StandardCharsets.UTF_8);
            StringBuilder directHex = new StringBuilder();
            for (byte b : directBytes) {
                directHex.append(String.format("%02x ", b & 0xFF));
            }
            System.out.println("  直接UTF-8字节: " + directHex.toString().trim());
            
            // URL编码
            String urlEncoded = URLEncoder.encode(str, StandardCharsets.UTF_8);
            byte[] urlBytes = urlEncoded.getBytes(StandardCharsets.UTF_8);
            StringBuilder urlHex = new StringBuilder();
            for (byte b : urlBytes) {
                urlHex.append(String.format("%02x ", b & 0xFF));
            }
            System.out.println("  URL编码后字节: " + urlHex.toString().trim());
            
            // 检查是否包含问号
            long questionCount = str.chars().filter(ch -> ch == '?').count();
            System.out.println("  问号数量: " + questionCount);
        }
    }
}
