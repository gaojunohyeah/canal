package com.alibaba.otter.canal.sync.base;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import com.alibaba.otter.canal.sync.global.Global;

import redis.clients.jedis.Jedis;

public class DestinationConfig {
	private String host;
	private int port;

	// 需要排除的mysql数据库，这些数据库变化不会进行redis同步
	private Set<String> ExcludeDbs = new HashSet<String>();

	/**
	 * redis缓存信息 key:String 数据库名称 value:RedisConfig redis配置
	 */
	private Map<String, RedisConfig> RedisMap = new HashMap<String, RedisConfig>();

	/**
	 * 加载redis同步配置
	 */
	public void initRedis(Properties redisPros) {
		host = redisPros.getProperty(Global.DESTINATION_HOST_KEY);
		port = Integer.parseInt(redisPros.getProperty(Global.DESTINATION_PORT_KEY));

		// 添加排除在外的mysql数据库变更
		String[] excludes = redisPros.getProperty(Global.REDIS_MYSQL_EXCLUDE_KEY).split(",");
		for (String exdb : excludes) {
			ExcludeDbs.add(exdb);
		}

		// 初始化redis配置
		int i = 0;
		while (true) {
			// 配置存在，则初始化配置
			if (redisPros.containsKey(Global.REDIS_MYSQL_DB_KEY + i) && redisPros.containsKey(Global.REDIS_HOST_KEY + i)
					&& redisPros.containsKey(Global.REDIS_PORT_KEY + i)) {
				String dbName = redisPros.getProperty(Global.REDIS_MYSQL_DB_KEY + i);

				// 初始化配置并放入缓存
				RedisConfig rc = new RedisConfig(dbName, redisPros.getProperty(Global.REDIS_HOST_KEY + i),
						Integer.parseInt(redisPros.getProperty(Global.REDIS_PORT_KEY + i)),
						Integer.parseInt(redisPros.getProperty(Global.REDIS_EXPIRE_KEY + i)));
				RedisMap.put(dbName, rc);

				i++;
			} else {
				break;
			}
		}
	}

	/**
	 * 根据key判定是否需要同步
	 * 
	 * @param dbname
	 *            mysql数据库名称
	 * @return 是否需要同步的标识
	 */
	public boolean checkKey(String dbname) {
		return ExcludeDbs.contains(dbname);
	}

	/**
	 * 获取配置的redis的所有的数据库名称集合
	 * 
	 * @return
	 */
	public Set<String> getDbnames() {
		return RedisMap.keySet();
	}

	/**
	 * 根据key获取数据库表
	 * 
	 * @param dbname
	 *            mysql数据库名称
	 * @return jredis对象
	 */
	public Jedis getJedisByKey(String dbname) throws Exception {
		// 需要排除的数据库
		if (ExcludeDbs.contains(dbname)) {
			return null;
		}

		// 特殊数据库redis配置存在
		if (RedisMap.containsKey(dbname)) {
			return RedisMap.get(dbname).getPool().getResource();
		}
		// 使用通用配置
		else {
			return RedisMap.get(Global.ALL_DB_KEY).getPool().getResource();
		}
	}

	/**
	 * jedis使用后需要返还给连接池
	 * 
	 * @param dbname
	 * @param jedis
	 */
	@SuppressWarnings("deprecation")
	public void returnJedis(String dbname, Jedis jedis) {
		// 特殊数据库redis配置存在
		if (RedisMap.containsKey(dbname)) {
			RedisMap.get(dbname).getPool().returnResource(jedis);
		}
		// 使用通用配置
		else {
			RedisMap.get(Global.ALL_DB_KEY).getPool().returnResource(jedis);
		}
	}

	/**
	 * 根据key获取redis过期时间
	 * 
	 * @param dbname
	 *            mysql数据库名称
	 * @return
	 */
	public int getExpireTime(String dbname) {
		// 需要排除的数据库
		if (ExcludeDbs.contains(dbname)) {
			return 0;
		}

		// 特殊数据库redis配置存在
		if (RedisMap.containsKey(dbname)) {
			return RedisMap.get(dbname).getExpireTime();
		}
		// 使用通用配置
		else {
			return RedisMap.get(Global.ALL_DB_KEY).getExpireTime();
		}
	}

	/**
	 * 销毁redis连接池
	 */
	public void destroy() {
		for (Entry<String, RedisConfig> entry : RedisMap.entrySet()) {
			// 销毁连接池
			entry.getValue().getPool().destroy();
		}
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
}
