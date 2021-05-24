package com.qianfeng.repostory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;


/**
 * @Description: 数据源配置
 * @Author: QF
 * @Date: 2020/6/25 12:56 PM
 * @Version V1.0
 */

@Configuration
public class DataSourceConfig {

    private static Logger logger = LoggerFactory.getLogger(DataSourceConfig.class);

//    @Bean(name = "prestoDataSource")
//    @ConfigurationProperties(prefix = "spring.datasource.presto")
//    // presto 数据源
//    public DataSource prestoDataSource() {
//        return DataSourceBuilder.create().build();
//    }

    @Bean(name = "prestoDataSource")
    public DataSource prestoDataSource() {
        return new HikariDataSource(hikariConfig());
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.presto")
    public HikariConfig hikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "prestoTemplate")
    public JdbcTemplate prestoJdbcTemplate(@Qualifier("prestoDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    //clickhouse的相关bean注解
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.clickhouse")
    public HikariConfig clickHouseHikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "clickHouseDataSource")
    public DataSource clickHouseDataSource() {
        return new HikariDataSource(clickHouseHikariConfig());
    }


    @Bean(name = "clickHouseTemplate")
    public JdbcTemplate clickHouseJdbcTemplate(@Qualifier("clickHouseDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }



}
