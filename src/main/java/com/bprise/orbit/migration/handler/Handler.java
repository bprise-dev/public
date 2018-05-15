package com.bprise.orbit.migration.handler;

import java.util.List;

import com.datastax.driver.core.Session;

import redis.clients.jedis.Jedis;

public interface Handler {
	void update(List<Long> ids, Session session, Jedis jedis);
}
