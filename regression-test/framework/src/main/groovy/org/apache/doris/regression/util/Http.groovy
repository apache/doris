package org.apache.doris.regression.util

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.nio.charset.Charset

class Http {

    static String http_post(url, data = null, isJson = false) {
        def conn = new URL(url).openConnection()
        conn.setRequestMethod("POST")
        if (data) {
            if (isJson) {
                conn.setRequestProperty("Content-Type", "application/json")
                data = JsonOutput.toJson(data)
            }
            // Output request parameters
            conn.doOutput = true
            def writer = new OutputStreamWriter(conn.outputStream)
            writer.write(data)
            writer.flush()
            writer.close()
        }
        if(isJson){
            def json = new JsonSlurper()
            def result = json.parseText(conn.content.text)
            return result
        }else {
            return conn.content.text
        }
    }

    //ms超时毫秒,url地址,json入参
    public static String httpJson(int ms,String url,String json) throws Exception{
        String err = "00", line = null;
        StringBuilder sb = new StringBuilder();
        HttpURLConnection conn = null;
        BufferedWriter out = null;
        BufferedReader inB = null;
        try{
            conn = (HttpURLConnection) (new URL(url.replaceAll("／","/"))).openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setConnectTimeout(ms);
            conn.setReadTimeout(ms);
            conn.setRequestProperty("Content-Type","application/json;charset=utf-8");
            conn.connect();
            out = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(),"utf-8"));
            out.write(new String(json.getBytes(), "utf-8"));
            out.flush();//发送参数
            int code = conn.getResponseCode();
            if (conn.getResponseCode()==200){
                inB = new BufferedReader(new InputStreamReader(conn.getInputStream(),"UTF-8"));
                while ((line=inB.readLine())!=null)
                    sb.append(line);
            }//接收返回值

        }catch(Exception ex){
            err=ex.getMessage();
        }
        try{ if (out!=null) out.close(); }catch(Exception ex){};
        try{ if (inB!=null) inB.close(); }catch(Exception ex){};
        try{ if (conn!=null) conn.disconnect();}catch(Exception ex){}
        if (!err.equals("00")) throw new Exception(err);
        return sb.toString();
    }


    public static String sendPost(String url, String param) {
        OutputStreamWriter out = null;
        BufferedReader inB = null;
        StringBuilder result = new StringBuilder("");
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("Content-Type","application/json;charset=UTF-8");
            conn.setRequestProperty("accept", "*/*");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
            // 发送请求参数
            out.write(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            inB = new BufferedReader(new InputStreamReader(conn.getInputStream(),"UTF-8"));
            String line;
            while ((line = inB.readLine()) != null) {
                result.append(line);
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            if(out!=null){ try { out.close(); }catch(Exception ex){} }
            if(inB!=null){ try { inB.close(); }catch(Exception ex){} }
        }
        return result.toString();
    }


    public static String httpPostJson(String url,String json) throws Exception{
        String data="";
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        try {
            httpClient = HttpClients.createDefault();
            HttpPost httppost = new HttpPost(url);
            httppost.setHeader("Content-Type", "application/json;charset=UTF-8");
            StringEntity se = new StringEntity(json, Charset.forName("UTF-8"));
            se.setContentType("text/json");
            se.setContentEncoding("UTF-8");
            httppost.setEntity(se);
            response = httpClient.execute(httppost);
            int code = response.getStatusLine().getStatusCode();
            System.out.println("接口响应码:"+code);
            data = EntityUtils.toString(response.getEntity(), "utf-8");
            EntityUtils.consume(response.getEntity());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(response!=null){ try{response.close();}catch (IOException e){} }
            if(httpClient!=null){ try{httpClient.close();}catch(IOException e){} }
        }
        return data;
    }

}
