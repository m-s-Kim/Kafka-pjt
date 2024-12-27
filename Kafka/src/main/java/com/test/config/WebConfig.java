package com.test.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.servlet.config.annotation.*;

import java.util.Arrays;

@Configuration
public class WebConfig implements WebMvcConfigurer{

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        CorsConfiguration corsConfiguration = new CorsConfiguration();
        corsConfiguration.setAllowedOrigins(Arrays.asList("http://localhost:8081","http://localhost:8080", "http://localhost:8083", "http://192.168.0.72:8080"));
        corsConfiguration.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE"));
        corsConfiguration.setAllowCredentials(true);  
        
        registry.addMapping("/**").allowedOrigins(corsConfiguration.getAllowedOrigins().toArray(new String[0]))
                                  .allowedMethods(corsConfiguration.getAllowedMethods().toArray(new String[0]))
                                  .allowCredentials(true);
    }
}
