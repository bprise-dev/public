package com.bprise.orbit.migration.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health.Builder;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.jedis.JedisConverters;

public class RedisHealthCheck extends AbstractHealthIndicator {

	@Autowired
	private JedisConfig jedisConfig;

	@Override
	protected void doHealthCheck(Builder builder) throws Exception {
		try {
			ClusterInfo clusterInfo = new ClusterInfo(
					JedisConverters.toProperties(jedisConfig.createConnection().clusterInfo()));
			builder.up().withDetail("cluster_size", clusterInfo.getClusterSize())
					.withDetail("slots_up", clusterInfo.getSlotsOk())
					.withDetail("slots_fail", clusterInfo.getSlotsFail());
		} finally {
			jedisConfig.createConnection().close();
		}
	}

}
