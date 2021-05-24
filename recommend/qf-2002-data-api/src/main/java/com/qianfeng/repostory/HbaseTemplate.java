package com.qianfeng.repostory;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
* @Description
* @Author hbasetempet模板
* @param null
* @Return
* @Exception
*
*/
public class HbaseTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseTemplate.class);

    private Configuration configuration;

    private volatile Connection connection;

    public HbaseTemplate() {
    }

    public HbaseTemplate(Configuration configuration) {
        this.setConfiguration(configuration);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    //获取hbase的连接
    public Connection getConnection() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
                        this.connection = ConnectionFactory.createConnection(configuration);
                        LOGGER.info("hbase connection:"+ this.connection);
                    } catch (IOException e) {
                        LOGGER.error("hbase connection创建失败");
                    }
                }
            }
        }
        return this.connection;
    }
}