package com.alibaba.otter.canal.sync.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class FileUtils {
	/**
	 * 根据文件路径 加载 properties 文件
	 * 
	 * @param filePath
	 *            文件路径
	 * @return
	 * @throws Exception
	 */
	public static Properties loadProFiles(String filePath){
		try {
			Properties prop = new Properties();
			// 读取文件
			InputStream in = new BufferedInputStream(new FileInputStream(filePath));
			
			// 加载输入流
			prop.load(in);
			
			return prop;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}
}
