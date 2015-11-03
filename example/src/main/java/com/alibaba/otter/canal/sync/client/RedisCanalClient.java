package com.alibaba.otter.canal.sync.client;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.sync.base.DestinationConfig;

import redis.clients.jedis.Jedis;

public class RedisCanalClient extends AbstractCanalClient{
	protected final static Logger logger = LoggerFactory.getLogger(RedisCanalClient.class);
	
	private DestinationConfig desConfig;
	
	public RedisCanalClient(String destination) {
		this(destination, null);
	}
	
    public RedisCanalClient(String destination, CanalConnector connector){
        super(destination, connector);
    }

	public DestinationConfig getDesConfig() {
		return desConfig;
	}

	public void setDesConfig(DestinationConfig desConfig) {
		this.desConfig = desConfig;
	}

	@Override
	public boolean handler(Message message) {
		// 获取数据库名称
		List<Entry> entrys = message.getEntries();
		// 循环消息的实体
		for(Entry entry : entrys){
			// 如果该条实体是数据行
			if(entry.getEntryType() == EntryType.ROWDATA){
				String dbname = entry.getHeader().getSchemaName();
				
				// 该数据库需要排除
				if(desConfig.checkKey(dbname)){
					continue;
				}else{
					String tableName = entry.getHeader().getTableName();
					
					// 转译数据
					RowChange rowChage = null;
	                try {
	                    rowChage = RowChange.parseFrom(entry.getStoreValue());
	                } catch (Exception e) {
	                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
	                }
	                
	                // 变更类型
	                EventType eventType = rowChage.getEventType();
	                
	                // 如果是查询语句，则跳过
	                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
	                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
	                    continue;
	                }

	                // 数据更新
	                for (RowData rowData : rowChage.getRowDatasList()) {
	                	// 如果是删除
	                    if (eventType == EventType.DELETE) {
	                    	delColumn(dbname, tableName, rowData.getBeforeColumnsList());
//	                        printColumn(rowData.getBeforeColumnsList());
	                    } 
	                    // 插入或者更新
	                    else{
	                    	updateColumn(dbname, tableName, rowData.getAfterColumnsList());
//	                        printColumn(rowData.getAfterColumnsList());
	                    }
	                }
				}
			}
		}
		
//		Jredis jredis = desConfig.getJedisByKey(dbname)
		return true;
	}
	
	/**
	 * 从redis中删除数据
	 * 
	 * @param dbname 数据库名称
	 * @param tableName 表名称
	 * @param columns 字段信息
	 */
	private void delColumn(String dbname, String tableName, List<Column> columns){
		Jedis jredis = desConfig.getJedisByKey(dbname);
		
		String key = null;
		
		for(Column column : columns){
			// 主键
			if(column.getIsKey()){
				key = column.getValue();
			}
		}
		
		// 主键存在
		if(null!=key){
			jredis.hdel(tableName, key);
			jredis.expire(tableName, this.desConfig.getExpireTime(dbname));
			logger.info("delete info to redis -> " + tableName + " " + key);
		}
	}

	
	/**
	 * 更新redis中的数据
	 * 
	 * @param dbname 数据库名称
	 * @param tableName 表名称
	 * @param columns 字段信息
	 */
	private void updateColumn(String dbname, String tableName, List<Column> columns){
		Jedis jredis = desConfig.getJedisByKey(dbname);
		
		String key = null;
		
		JSONObject json = new JSONObject();
		for(Column column : columns){
			// 主键
			if(column.getIsKey()){
				key = column.getValue();
			}
			
			// 添加字段
			json.put(column.getName(), column.getValue());
		}

		// 主键存在
		if(null!=key){
			String msg = json.toJSONString();
			jredis.hset(tableName, key, msg);
			jredis.expire(tableName, this.desConfig.getExpireTime(dbname));
			logger.info("update info to redis -> " + tableName + " " + key + " " + msg);
		}
	}
	
	@Deprecated
    protected void printColumn(List<Column> columns) {
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());
        }
    }
}
