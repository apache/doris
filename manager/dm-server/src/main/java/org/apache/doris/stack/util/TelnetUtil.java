package org.apache.doris.stack.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * telnet util
 **/
@Slf4j
public class TelnetUtil {

    /**
     * Test the connectivity of the host port
     */
    public static boolean telnet(String host, int port) {
        Socket socket = new Socket();
        boolean isConnected = false;
        long str = System.currentTimeMillis();
        try {
            socket.connect(new InetSocketAddress(host, port), 1000);
            isConnected = socket.isConnected();
            System.out.println(isConnected);
        } catch (IOException e) {
            log.error("can not telnet {}:{}", host, port);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                log.error("can not telnet {}:{}", host, port);
            }
        }
        System.out.println(System.currentTimeMillis() - str);
        return isConnected;
    }

    public static void main(String[] args) {
        telnet("www.baidu.com", 11111);
    }
}
