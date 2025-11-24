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

package org.apache.doris.httpv2.config;

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.interceptor.AuthInterceptor;

import org.springframework.boot.web.server.ErrorPage;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfigurer implements WebMvcConfigurer {

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new AuthInterceptor())
                .addPathPatterns("/rest/v1/**")
                .excludePathPatterns("/", "/api/**", "/rest/v1/login", "/rest/v1/logout", "/static/**", "/metrics")
                .excludePathPatterns("/image", "/info", "/version", "/put", "/journal_id", "/role", "/check", "/dump");
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowCredentials(false)
                .allowedMethods("*")
                .allowedOrigins(Config.access_control_allowed_origin_domain)
                .allowedHeaders("*")
                .maxAge(3600);
    }

    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        configurer.setPathMatcher(new AntPathMatcher());
        configurer.setUseTrailingSlashMatch(true);
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/notFound")
                .setStatusCode(HttpStatus.OK)
                .setViewName("forward:/index.html");
        registry.addViewController("/{path:[^\\.]*}")
                .setViewName("forward:/index.html");
        registry.addViewController("/**/{path:[^\\.]*}")
                .setViewName("forward:/index.html");
    }

    @Bean
    public WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> containerCustomizer() {
        return container -> container.addErrorPages(
                new ErrorPage(HttpStatus.NOT_FOUND, "/notFound")
        );
    }

    @Bean(name = "multipartResolver")
    public StandardServletMultipartResolver multipartResolver() {
        return new StandardServletMultipartResolver();
    }
}
