package com.alibaba.otter.canal.sync.global;

public class Global {
	// 配置文件路径
	public static String baseDir = "/src/main/resources/";

	// 基础配置文件名称
	public static String configFile = "sync.properties";

	public static String Replace_Word = "{$}";
	
	// 服务端destination名称，详细参照canal配置
	public static String SYNC_DESTINATION_KEY = "sync.destination";
	// redis配置文件key
	public static String SYNC_FILE_KEY = "sync.{$}.file";

	// redis配置mysql排除数据库名称key
	public static String REDIS_MYSQL_EXCLUDE_KEY = "sync.mysql.excludes";
	// redis配置mysql数据库名称key
	public static String REDIS_MYSQL_DB_KEY = "sync.mysql.dbname.";
	// redis配置主机key
	public static String REDIS_HOST_KEY = "sync.redis.host.";
	// redis配置端口key
	public static String REDIS_PORT_KEY = "sync.redis.port.";
	// redis配置过期时间key
	public static String REDIS_EXPIRE_KEY = "sync.redis.expire.";
	
	// destination地址
	public static String DESTINATION_HOST_KEY = "sync.destination.host";
	// destination端口
	public static String DESTINATION_PORT_KEY = "sync.destination.port";
}
