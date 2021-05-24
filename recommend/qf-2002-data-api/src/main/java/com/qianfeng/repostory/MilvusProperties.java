package com.qianfeng.repostory;

import org.springframework.boot.context.properties.ConfigurationProperties;


/**
 * milvus 配置属性，配置文件路径
 */
@ConfigurationProperties(prefix = "spring.data.milvus")
public class MilvusProperties {

    private String host;

    private int port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}