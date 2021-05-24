package com.qianfeng.repostory;

import org.springframework.boot.context.properties.ConfigurationProperties;


/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date
*@Description通过application.yml中的数据源注入hbase连接信息
**/
@ConfigurationProperties(prefix = "spring.data.hbase")
public class HbaseProperties {

    private String quorum;

    private String rootDir;

    private String nodeParent;

    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public String getNodeParent() {
        return nodeParent;
    }

    public void setNodeParent(String nodeParent) {
        this.nodeParent = nodeParent;
    }
}