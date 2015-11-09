package com.alibaba.otter.canal.sync.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.sync.SyncServer;
import com.alibaba.otter.canal.sync.base.DestinationConfig;
import com.alibaba.otter.canal.sync.base.SyncConfig;
import com.alibaba.otter.canal.sync.global.Global;

import redis.clients.jedis.Jedis;

public class RedisExpireJob implements Job {
	protected final static Logger logger = LoggerFactory.getLogger(RedisExpireJob.class);

	public static String jobName = "RedisExpireJob";
	public static String jobCron = "0 */1 * * * ?";

	private static long now;

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		logger.info("*** redis hash expire sync job start ***");

		now = System.currentTimeMillis();

		// 进行redis库hash的数据过期处理
		try {
			reisHashExpire();
		} catch (Exception e) {
			logger.error("process error!", e);
		}

		logger.info("*** redis hash expire sync job end ***");
	}

	/**
	 * 进行redis库hash的数据过期处理
	 */
	private void reisHashExpire() throws Exception {
		// 获取所有的配置
		SyncConfig config = SyncServer.config;

		// 遍历所有的destination
		loopDestination(config);
	};

	/**
	 * 遍历所有destination进行处理
	 * 
	 * @param config
	 *            同步配置
	 */
	private void loopDestination(SyncConfig config) throws Exception {
		Set<String> desNames = config.getDesNames();

		// 遍历所有destination，获取redis链接，进行同步操作
		Iterator<String> it = desNames.iterator();
		while (it.hasNext()) {
			String desName = it.next();

			DestinationConfig desConfig = config.getDesConfig(desName);

			// 遍历其下的redis链接
			loopDbRedis(desConfig);
		}
	}

	/**
	 * 遍历destination下的所有redis连接进行处理
	 * 
	 * @param desConfig
	 *            destination配置
	 */
	private void loopDbRedis(DestinationConfig desConfig) throws Exception {
		// 遍历所有数据库
		Set<String> dbNames = desConfig.getDbnames();
		Iterator<String> dbIt = dbNames.iterator();
		while (dbIt.hasNext()) {
			String dbname = dbIt.next();

			Jedis jedis = desConfig.getJedisByKey(dbname);

			try {
				// 进行过期处理
				doHashExpire(jedis, dbname);
			} catch (Exception e) {
				throw e;
			} finally {
				desConfig.returnJedis(dbname, jedis);
			}

		}
	}

	/**
	 * 操作redis进行数据过期处理
	 * 
	 * @param jedis
	 *            redis连接API对象
	 * @param dbname
	 *            数据库名称
	 */
	private void doHashExpire(Jedis jedis, String dbname) throws Exception {
		// 获取所有应该过期的数据并从redis中移除
		Set<String> keys = jedis.zrangeByScore(dbname, 0, now);

		// 从sort set中移除
		jedis.zremrangeByScore(dbname, 0, now);

		Map<String, List<String>> expires = new HashMap<String, List<String>>();

		// 遍历所有的过期key，进行统计
		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
			String nameStr = it.next();
			String[] names = nameStr.split(Global.REDIS_PERIOD_SPLIT);
			String tableName = names[0];
			String tableId = names[1];

			// 添加到移除map中
			if (!expires.containsKey(tableName)) {
				expires.put(tableName, new ArrayList<String>());
			}
			expires.get(tableName).add(tableId);
		}

		// 统一进行移除
		for (Entry<String, List<String>> entry : expires.entrySet()) {
			String[] ids = new String[entry.getValue().size()];
			jedis.hdel(entry.getKey(), entry.getValue().toArray(ids));
			logger.info("*** redis expire data remove -> [{}] [{}] [{}]", new Object[] { dbname, entry.getKey(), ids });
		}
	}
}
