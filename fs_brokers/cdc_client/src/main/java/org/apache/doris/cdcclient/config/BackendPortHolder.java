package org.apache.doris.cdcclient.config;

import org.apache.doris.cdcclient.common.Env;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class BackendPortHolder {

    @Value("${backend.http.port}")
    private int port;

    @PostConstruct
    public void init() {
        Env.getCurrentEnv().setBackendHttpPort(port);
    }
}
