package com.alibaba.otter.canal.sync.base;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.alibaba.otter.canal.sync.global.Global;
import com.alibaba.otter.canal.sync.util.FileUtils;

public class SyncConfig {
	// destination配置
	private Map<String, DestinationConfig> DesMap = new HashMap<String, DestinationConfig>();
	
	private static String confPath;
	
	public String getConfPath(){
		return confPath;
	}
	
	/**
	 * 加载同步配置
	 */
	public SyncConfig() {
		// 加载基础配置
		confPath = System.getProperty("sync.conf");
		if(null==confPath || "".equals(confPath)){
			confPath = System.getProperty("user.dir") + Global.baseDir;
		}
		String path = confPath + Global.configFile;
		Properties prop = FileUtils.loadProFiles(path);

		// 加载redis同步配置
		String[] destinations = prop.getProperty(Global.SYNC_DESTINATION_KEY).split(",");
		for(String destination : destinations){
			// 获取配置文件
			String desConfigFile = prop.getProperty(
					Global.SYNC_FILE_KEY.replace(Global.Replace_Word, destination));
			desConfigFile = confPath + desConfigFile;
			
			// 加载配置
			Properties redisPros = FileUtils.loadProFiles(desConfigFile);
			if(null!=redisPros){
				DestinationConfig desConfig = new DestinationConfig();
				desConfig.initRedis(redisPros);
				
				// 添加到缓存
				DesMap.put(destination, desConfig);
			}
		}
	}
	
	/**
	 * 获取所有的destination名称
	 * 
	 * @return 名称集合
	 */
	public Set<String> getDesNames(){
		return DesMap.keySet();
	}
	
	/**
	 * 根据destination获取配置信息
	 * 
	 * @param desName
	 * @return 配置信息
	 */
	public DestinationConfig getDesConfig(String desName){
		if(DesMap.containsKey(desName)){
			return DesMap.get(desName);
		}else{
			return null;
		}
	}

	/**
	 * 销毁配置以及redis链接
	 */
	public void destroy() {
		for (Entry<String, DestinationConfig> desEntry : DesMap.entrySet()) {
			// 清理redis链接
			desEntry.getValue().destroy();
		}
	}
}
