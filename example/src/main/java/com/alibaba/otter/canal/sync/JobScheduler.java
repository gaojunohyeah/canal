package com.alibaba.otter.canal.sync;

import java.util.Date;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.sync.job.RedisExpireJob;

public class JobScheduler {
	protected final static Logger logger = LoggerFactory.getLogger(JobScheduler.class);

	private static String JOB_GROUP_NAME = "group1";
	private static String TRIGGER_GROUP_NAME = "trigger1";

	private static Scheduler scheduler;

	private Scheduler createScheduler(String confPath) throws SchedulerException {// 创建调度器
//		System.out.println(confPath + "quartz.properties");
		return new StdSchedulerFactory(confPath + "quartz.properties").getScheduler();
	}

	/**
	 * 添加一个定时任务，使用默认的任务组名，触发器名，触发器组名
	 * 
	 * @param jobName
	 *            任务名
	 * @param job
	 *            任务
	 * @param time
	 *            时间设置，参考quartz说明文档
	 * @throws Exception
	 */
	public void addJob(String jobName, Class<? extends Job> jobclass, String cronTime) throws Exception {
		// 实例化job
		JobDetail jobDetail = JobBuilder.newJob(jobclass).withIdentity(jobName, JOB_GROUP_NAME).build();

		// 实例化触发器
		CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName, TRIGGER_GROUP_NAME)
				.withSchedule(CronScheduleBuilder.cronSchedule(cronTime)).build();

		scheduler.scheduleJob(jobDetail, trigger);
	}

	/**
	 * 定时器启动
	 * 
	 * @param confPath
	 */
	public static void init(String confPath) {
		JobScheduler jobScheduler = new JobScheduler();
		try {
			scheduler = jobScheduler.createScheduler(confPath);

			// 定时任务注册
			// redis hash数据过期处理
			jobScheduler.addJob(RedisExpireJob.jobName, RedisExpireJob.class, RedisExpireJob.jobCron);

			// Start the scheduler running
			scheduler.start();

			logger.info("Scheduler started at " + new Date());
		} catch (Exception e) {
			logger.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

	/**
	 * 定时器关闭
	 */
	public static void stop() {
		try {
			scheduler.shutdown();
		} catch (Exception e) {
			logger.error(ExceptionUtils.getFullStackTrace(e));
		}
	}
}
