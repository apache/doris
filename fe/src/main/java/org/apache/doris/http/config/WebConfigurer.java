package org.apache.doris.http.config;

import org.apache.doris.http.interceptor.AuthInterceptor;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfigurer implements WebMvcConfigurer {

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        System.out.println("================= Registry Interceptor");
        registry.addInterceptor(new AuthInterceptor())
                .addPathPatterns("/rest/v1/**")
                .excludePathPatterns("/","/api/**","/rest/v1/login","/static/**");
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        System.out.println("================= Registry Cors");
        registry.addMapping("/**")
                .allowCredentials(false)
                .allowedMethods("*")
                .allowedOrigins("*")
                .allowedHeaders("*")
                .maxAge(3600);

    }
}

