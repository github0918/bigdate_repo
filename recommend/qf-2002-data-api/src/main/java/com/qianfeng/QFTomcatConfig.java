package com.qianfeng;

import org.apache.catalina.connector.Connector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 自定义tomcat配置
 */
@Configuration
public class QFTomcatConfig {

    @Bean
    public ServletWebServerFactory servletContainer() {
        TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
        tomcat.addConnectorCustomizers(new QFTomcatConnectorCustomizer());
        return tomcat;
    }

    /**
     * 自定义tomcat
     */
    public class QFTomcatConnectorCustomizer implements TomcatConnectorCustomizer {
        public QFTomcatConnectorCustomizer() {
        }

        public void customize(Connector connector) {

            connector.setPort(Integer.valueOf(port));
            connector.setProperty("connectionTimeout", connectionTimeout);
            connector.setProperty("acceptorThreadCount", acceptorThreadCount);
            connector.setProperty("maxThreads", maxThreads);
            connector.setProperty("maxConnections", maxConnections);
            connector.setProperty("protocol", protocol);
            connector.setProperty("acceptCount", acceptCount);
            connector.setProperty("compression", "compression");

        }

    }

    @Value("${spring.server.port}")
    private String port;
    @Value("${spring.server.acceptorThreadCount}")
    private String acceptorThreadCount;
    @Value("${spring.server.maxThreads}")
    private String maxThreads;
    @Value("${spring.server.maxConnections}")
    private String maxConnections;
    @Value("${spring.server.protocol}")
    private String protocol;
    @Value("${spring.server.connectionTimeout}")
    private String connectionTimeout;
    @Value("${spring.server.acceptCount}")
    private String acceptCount;

}
