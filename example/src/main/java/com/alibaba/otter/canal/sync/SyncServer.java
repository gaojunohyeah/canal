package com.alibaba.otter.canal.sync;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.sync.base.DestinationConfig;
import com.alibaba.otter.canal.sync.base.SyncConfig;
import com.alibaba.otter.canal.sync.client.RedisCanalClient;

public class SyncServer {
	protected final static Logger logger = LoggerFactory.getLogger(SyncServer.class);

	/*
	 * 全局配置
	 */
	public static SyncConfig config;

	/**
	 * 与canal服务器之间的连接map key:destination名称 value:连接对象
	 */
	private static Map<String, RedisCanalClient> ClientMap = new HashMap<String, RedisCanalClient>();

	public static void main(String[] args) {
		// 初始化配置 并启动连接池
		config = new SyncConfig();

		Set<String> desNames = config.getDesNames();
		for (String desName : desNames) {
			DestinationConfig desConfig = config.getDesConfig(desName);

			// 启动连接，连接canal
			// 根据ip，直接创建链接，无HA的功能
			CanalConnector connector = CanalConnectors.newSingleConnector(
					new InetSocketAddress(desConfig.getHost(), desConfig.getPort()), desName, "", "");

			final RedisCanalClient client = new RedisCanalClient(desName);
			client.setDesConfig(desConfig);
			client.setConnector(connector);
			client.start();

			ClientMap.put(desName, client);
		}

		// 定时器启动
		JobScheduler.init(config.getConfPath());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					logger.info("## stop the canal client");
					// 移除所有客户端连接
					for (Entry<String, RedisCanalClient> entry : ClientMap.entrySet()) {
						entry.getValue().stop();
					}

					// 销毁配置以及redis链接
					config.destroy();

					// 定时器关闭
					JobScheduler.stop();
				} catch (Throwable e) {
					logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
				} finally {
					logger.info("## canal client is down.");
				}
			}

		});
	}
}
