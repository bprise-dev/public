package com.bprise.orbit.migration.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@EnableConfigurationProperties(RedisProperties.class)
public class JedisConfig {

	@Autowired
	private RedisProperties properties;
	private static final Logger LOGGER = LoggerFactory.getLogger(JedisConfig.class);
	
	public JedisConfig() {
	}

	@SuppressWarnings("resource")
	@Bean(name="jedis")
	public Jedis createConnection() {
		Jedis jedis =null;
		try {
			//JedisPool jedisPool = new JedisPool(buildPoolConfig(), properties.getHost(), properties.getPort(), properties.getTimeout(), properties.getPassword());
			JedisPool jedisPool = new JedisPool("localhost", 6379);
			
			jedis = jedisPool.getResource();
			jedis.select(properties.getDatabase());
			LOGGER.info("INFO: Redis server has started");
		} catch (Exception e) {
			LOGGER.error("ERROR: Redis connection error --> ",e);
			LOGGER.info("INFO: Migration job has terminated.");
			System.exit(0);
		}
		return jedis;
	}

	private JedisPoolConfig buildPoolConfig() {
		final JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(128);
		poolConfig.setMaxIdle(128);
		/*
		 * poolConfig.setMinIdle(16); poolConfig.setTestOnBorrow(true);
		 * poolConfig.setTestOnReturn(true); poolConfig.setTestWhileIdle(true);
		 */
		return poolConfig;
	}
	
	@Bean(name="MQTTRedisConnection")
	public Jedis createConnectionRedis() {
		Jedis jedis =null;
		try {
			JedisPool jedisPoolMQTT = new JedisPool(buildPoolConfig(), properties.getHost(), properties.getPort(), properties.getTimeout(), properties.getPassword());
			jedis = jedisPoolMQTT.getResource();
			jedis.select(properties.getEmqttAuthDatabase());
			LOGGER.info("INFO: Redis server has started");
		} catch (Exception e) {
			LOGGER.error("ERROR: Redis connection error --> ",e);
			LOGGER.info("INFO: Migration job has terminated.");
			System.exit(0);
		}
		return jedis;
	}

}
