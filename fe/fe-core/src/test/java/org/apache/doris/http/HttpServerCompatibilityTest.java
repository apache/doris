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

package org.apache.doris.http;

import org.apache.doris.httpv2.config.WebConfigurer;
import org.apache.doris.httpv2.config.WebServerFactoryCustomizerConfig;
import org.apache.doris.httpv2.exception.RestApiExceptionHandler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** Exercises the FE web configuration against the embedded servlet container and real HTTP requests. */
public class HttpServerCompatibilityTest {
    private static final MediaType JSON = MediaType.parse("application/json");
    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .followRedirects(false)
            .build();
    private static ServletWebServerApplicationContext applicationContext;
    private static String baseUrl;

    @BeforeAll
    static void startServer() {
        applicationContext = (ServletWebServerApplicationContext) new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.SERVLET)
                .properties("server.address=127.0.0.1", "server.port=0", "spring.main.banner-mode=off")
                .run();
        baseUrl = "http://127.0.0.1:" + applicationContext.getWebServer().getPort();
    }

    @AfterAll
    static void stopServer() {
        applicationContext.close();
    }

    @Test
    void preservesJsonAndQueryParametersWithTrailingSlash() throws Exception {
        for (String path : new String[] {"/api/dependency_compatibility", "/api/dependency_compatibility/"}) {
            try (Response response = send(new Request.Builder().url(baseUrl + path + "?q=hello%20world")
                    .get().build())) {
                Assertions.assertEquals(200, response.code());
                JsonObject body = JsonParser.parseString(response.body().string()).getAsJsonObject();
                Assertions.assertEquals("hello world", body.get("snake_case").getAsString());
                Assertions.assertEquals("2026-10-10", body.get("date").getAsString());
                Assertions.assertFalse(body.has("value"));
            }
        }
    }

    @Test
    void preservesPostMethodAndJsonBodyWithTrailingSlash() throws Exception {
        for (String path : new String[] {"/api/dependency_compatibility", "/api/dependency_compatibility/"}) {
            try (Response response = send(new Request.Builder().url(baseUrl + path + "?q=hello%20world")
                    .post(okhttp3.RequestBody.create("{\"snake_case\":\"body\",\"date\":\"2026-10-10\"}", JSON))
                    .build())) {
                Assertions.assertEquals(200, response.code());
                JsonObject body = JsonParser.parseString(response.body().string()).getAsJsonObject();
                Assertions.assertEquals("body:hello world", body.get("snake_case").getAsString());
                Assertions.assertEquals("2026-10-10", body.get("date").getAsString());
            }
        }
    }

    @Test
    void preservesCookieSecurityAttributes() throws Exception {
        try (Response response = send(new Request.Builder().url(baseUrl + "/api/dependency_cookie").get().build())) {
            Assertions.assertEquals(200, response.code());
            String cookie = response.header("Set-Cookie");
            Assertions.assertNotNull(cookie);
            cookie = cookie.toLowerCase(Locale.ROOT);
            Assertions.assertTrue(cookie.contains("httponly"));
            Assertions.assertTrue(cookie.contains("samesite=lax"));
        }
    }

    @Test
    void requiresAuthenticationWithAndWithoutTrailingSlash() throws Exception {
        for (String path : new String[] {"/rest/v1/dependency_compatibility", "/rest/v1/dependency_compatibility/"}) {
            try (Response response = send(new Request.Builder().url(baseUrl + path).get().build())) {
                // RestApiExceptionHandler reports an authentication failure as HTTP 200 with code 401 in the body.
                Assertions.assertEquals(200, response.code());
                JsonObject body = JsonParser.parseString(response.body().string()).getAsJsonObject();
                Assertions.assertEquals(401, body.get("code").getAsInt());
                Assertions.assertNull(response.header("Location"));
            }
        }
    }

    @Test
    void preservesRootPath() throws Exception {
        try (Response response = send(new Request.Builder().url(baseUrl + "/").get().build())) {
            Assertions.assertEquals(200, response.code());
            Assertions.assertEquals("root", response.body().string());
        }
    }

    private Response send(Request request) throws Exception {
        return CLIENT.newCall(request).execute();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @Import({WebConfigurer.class, WebServerFactoryCustomizerConfig.class,
            RestApiExceptionHandler.class, CompatibilityController.class})
    public static class TestApplication {
    }

    @RestController
    public static class CompatibilityController {
        @GetMapping("/api/dependency_compatibility")
        public LegacyPayload get(@RequestParam("q") String query) {
            return new LegacyPayload(query, LocalDate.of(2026, 10, 10));
        }

        @PostMapping("/api/dependency_compatibility")
        public LegacyPayload post(@RequestBody LegacyPayload payload, HttpServletRequest request) {
            return new LegacyPayload(payload.value() + ":" + request.getParameter("q"), payload.date());
        }

        @GetMapping("/api/dependency_cookie")
        public String cookie(HttpServletResponse response) {
            Cookie cookie = new Cookie("compatibility_session", "value");
            cookie.setHttpOnly(true);
            cookie.setAttribute("SameSite", "Lax");
            response.addCookie(cookie);
            return "ok";
        }

        @GetMapping("/rest/v1/dependency_compatibility")
        public String protectedEndpoint() {
            return "authenticated";
        }

        @GetMapping("/")
        public String root() {
            return "root";
        }
    }

    public static class LegacyPayload {
        private final String value;
        private final LocalDate date;

        @JsonCreator
        public LegacyPayload(@JsonProperty("snake_case") String value, @JsonProperty("date") LocalDate date) {
            this.value = value;
            this.date = date;
        }

        @JsonProperty("snake_case")
        public String value() {
            return value;
        }

        @JsonProperty("date")
        public LocalDate date() {
            return date;
        }
    }
}
