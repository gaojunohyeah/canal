package com.alibaba.otter.canal.sync.job;

import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisExpireJob implements Job {
	protected final static Logger logger = LoggerFactory.getLogger(RedisExpireJob.class);
	
	public static String jobName = "RedisExpireJob";
	public static String jobCron = "0 */1 * * * ?";

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		
		System.out.println(new Date());
		
	}

}
