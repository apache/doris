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

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.config.WebConfigurer;
import org.apache.doris.httpv2.config.WebServerFactoryCustomizerConfig;
import org.apache.doris.httpv2.ui.UiApiExceptionHandler;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
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
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Locale;

/** Exercises the FE web configuration against the embedded servlet container and real HTTP requests. */
public class HttpServerCompatibilityTest {
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    private static ServletWebServerApplicationContext applicationContext;
    private static String baseUrl;

    @BeforeAll
    static void startServer() {
        Assertions.assertTrue(Config.enable_all_http_auth);
        Assertions.assertTrue(Config.enable_web_ui);
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
            HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(baseUrl + path + "?q=hello%20world"))
                    .GET().build());
            Assertions.assertEquals(200, response.statusCode());
            JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
            Assertions.assertEquals("hello world", body.get("snake_case").getAsString());
            Assertions.assertEquals("2026-10-10", body.get("date").getAsString());
            Assertions.assertFalse(body.has("value"));
        }
    }

    @Test
    void preservesPostMethodAndJsonBodyWithTrailingSlash() throws Exception {
        for (String path : new String[] {"/api/dependency_compatibility", "/api/dependency_compatibility/"}) {
            HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(baseUrl + path + "?q=hello%20world"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"snake_case\":\"body\",\"date\":\"2026-10-10\"}"))
                    .build());
            Assertions.assertEquals(200, response.statusCode());
            JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
            Assertions.assertEquals("body:hello world", body.get("snake_case").getAsString());
            Assertions.assertEquals("2026-10-10", body.get("date").getAsString());
        }
    }

    @Test
    void preservesCookieSecurityAttributes() throws Exception {
        HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(baseUrl + "/api/dependency_cookie"))
                .GET().build());
        Assertions.assertEquals(200, response.statusCode());
        String cookie = response.headers().firstValue("Set-Cookie").orElseThrow().toLowerCase(Locale.ROOT);
        Assertions.assertTrue(cookie.contains("httponly"));
        Assertions.assertTrue(cookie.contains("samesite=lax"));
    }

    @Test
    void requiresAuthenticationWithAndWithoutTrailingSlash() throws Exception {
        for (String path : new String[] {"/rest/v1/ui/dependency_compatibility", "/rest/v1/ui/dependency_compatibility/"}) {
            HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build());
            Assertions.assertEquals(401, response.statusCode());
            Assertions.assertFalse(response.headers().firstValue("Location").isPresent());
        }
    }

    @Test
    void preservesRootPath() throws Exception {
        HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(baseUrl + "/")).GET().build());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("root", response.body());
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        return CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @Import({WebConfigurer.class, WebServerFactoryCustomizerConfig.class,
            CompatibilityExceptionHandler.class, CompatibilityController.class})
    public static class TestApplication {
    }

    @RestControllerAdvice
    public static class CompatibilityExceptionHandler extends UiApiExceptionHandler {
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

        @GetMapping("/rest/v1/ui/dependency_compatibility")
        public String protectedEndpoint() {
            return "authenticated";
        }

        @GetMapping("/")
        public String root() {
            return "root";
        }
    }

    public record LegacyPayload(@JsonProperty("snake_case") String value, LocalDate date) {
    }
}
