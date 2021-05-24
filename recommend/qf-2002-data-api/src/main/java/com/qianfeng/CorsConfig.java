package com.qianfeng;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * @ClassName CorsConfig
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 解决index.html:1 Access to XMLHttpRequest at 'http://localhost:7088/api/v1/my/dau/2020-09-14/2020-09-17/3/10/2000' from
 * origin 'null' has been blocked by CORS policy: No 'Access-Control-Allow-Origin' header is present on the requested resource.
 **/
@Configuration
public class CorsConfig  extends WebMvcConfigurerAdapter {
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins("*")
                .allowCredentials(true)
                .allowedMethods("GET", "POST", "DELETE", "PUT")
                .maxAge(3600);
    }
}