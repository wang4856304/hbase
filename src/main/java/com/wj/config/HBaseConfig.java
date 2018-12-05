package com.wj.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@Configuration
@ConfigurationProperties(prefix = "hbase.config")
public class HBaseConfig {
    private String quorum;
    private String port;

    @Bean
    public org.apache.hadoop.conf.Configuration getHbaseConfiguration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.209.129");
        return configuration;
    }
}
