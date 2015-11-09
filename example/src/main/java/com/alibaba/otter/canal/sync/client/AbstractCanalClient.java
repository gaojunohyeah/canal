package com.alibaba.otter.canal.sync.client;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

/**
 * 客户端同步基类
 * 
 * @author jun.gao
 */
public abstract class AbstractCanalClient {
	protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
	protected volatile boolean running = false;

	protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
		public void uncaughtException(Thread t, Throwable e) {
			logger.error("parse events has an error", e);
		}
	};

	protected static final String SEP = SystemUtils.LINE_SEPARATOR;
	protected Thread thread = null;
	protected CanalConnector connector;
	protected String destination;

	public AbstractCanalClient(String destination) {
		this(destination, null);
	}

	public AbstractCanalClient(String destination, CanalConnector connector) {
		this.destination = destination;
		this.connector = connector;
	}

	public void start() {
		Assert.notNull(connector, "connector is null");
		thread = new Thread(new Runnable() {

			public void run() {
				process();
			}
		});

		thread.setUncaughtExceptionHandler(handler);
		thread.start();
		running = true;
	}

	public void stop() {
		if (!running) {
			return;
		}
		running = false;
		if (thread != null) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}

		MDC.remove("destination");
	}

	protected void process() {
		int batchSize = 5 * 1024;
		while (running) {
			try {
				MDC.put("destination", destination);
				connector.connect();
				connector.subscribe();
				while (running) {
					Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
					long batchId = message.getId();
					int size = message.getEntries().size();
					if (batchId == -1 || size == 0) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
						}
					} else {
						// 处理消息同步
						if (handler(message)) {
							connector.ack(batchId); // 提交确认
						} else {
							connector.rollback(batchId); // 处理失败, 回滚数据
						}
					}
				}
			} catch (Exception e) {
				logger.error("process error!", e);
			} finally {
				connector.disconnect();
				MDC.remove("destination");
			}
		}
	}

	public void setConnector(CanalConnector connector) {
		this.connector = connector;
	}

	// 消息处理
	abstract public boolean handler(Message message) throws Exception;
}
