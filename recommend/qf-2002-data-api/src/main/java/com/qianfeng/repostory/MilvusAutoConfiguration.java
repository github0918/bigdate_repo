package com.qianfeng.repostory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;


/**
 * milvus 自动加载配置
 */
@org.springframework.context.annotation.Configuration
@EnableAutoConfiguration
@EnableConfigurationProperties(MilvusProperties.class)
public class MilvusAutoConfiguration {

    private static final String HOST = "host";
    private static final String PORT = "port";

    @Autowired
    private MilvusProperties milvusProperties;

    @Bean
    public MilvusTemplate milvusTemplate() {
        return new MilvusTemplate(this.milvusProperties.getHost(), this.milvusProperties.getPort());
    }
}