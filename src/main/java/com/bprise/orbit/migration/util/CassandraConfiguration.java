package com.bprise.orbit.migration.util;

import java.util.List;


import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.boot.autoconfigure.cassandra.ClusterBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Session;


@Configuration
@EnableConfigurationProperties(CassandraProperties.class)
public class CassandraConfiguration extends CassandraAutoConfiguration{

	@Autowired
	private CassandraProperties properties;
	
	public CassandraConfiguration(CassandraProperties properties,
			ObjectProvider<List<ClusterBuilderCustomizer>> builderCustomizers) {
		super(properties, builderCustomizers);
	}
	
	@Bean
	public Session session() {
		return cluster().connect(properties.getKeyspaceName());
	}

}
