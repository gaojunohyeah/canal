package com.alibaba.otter.canal.sync.base;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConfig {
	// 对应mysql数据库名称
	private String mysqlDb;

	// redis主机地址
	private String redisHost;

	// redis主机端口
	private int redisPort;
	
	// redis过期时间
	private int expireTime;

	// jredis pool
	private JedisPool pool;

	public RedisConfig(String mysqlDb, String redisHost, int redisPort, int expireTime) {
		this.mysqlDb = mysqlDb;
		this.redisHost = redisHost;
		this.redisPort = redisPort;
		this.expireTime = expireTime;
		this.setPool();
	}

	public JedisPool getPool() {
		return pool;
	}

	/**
	 * 初始化redis连接池
	 */
	public void setPool() {
		this.pool = new JedisPool(new JedisPoolConfig(), this.redisHost, this.redisPort);
	}

	public String getMysqlDb() {
		return mysqlDb;
	}

	public void setMysqlDb(String mysqlDb) {
		this.mysqlDb = mysqlDb;
	}

	public String getRedisHost() {
		return redisHost;
	}

	public void setRedisHost(String redisHost) {
		this.redisHost = redisHost;
	}

	public int getRedisPort() {
		return redisPort;
	}

	public void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}

	public int getExpireTime() {
		return expireTime;
	}

	public void setExpireTime(int expireTime) {
		this.expireTime = expireTime;
	}
}
