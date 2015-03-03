package com.giant.etllog;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.util.ConfigUtil;
import com.giant.etllog.util.LogTimer;
import com.google.common.base.Preconditions;

/**
 * <pre>
 *  hadoop jar logserver-1.0.0.jar com.giant.etllog.EtlLog
 *  hadoop dfs -ls /tmp/ztgame/zt/
 *  hadoop dfs -rm -r /tmp/ztgame/zt/*
 *  
 *  find . -name '*.pos' |xargs rm -rf
 *  
 *  ls -lrtha -R
 * </pre>
 * 
 * @author wankun
 * @date 2014年5月27日
 * @version 1.0
 */
public class EtlLog {

	private final static Logger logger = LoggerFactory.getLogger(EtlLog.class);

	public static void main(String[] args) {
		if (args.length != 1 || !new File(args[0]).exists()) {
			logger.info("请指定正确的程序配置文件");
			System.exit(-1);
		}
		ConfigUtil.loadConfig(args[0]);

		// 2. start core service
		String monitorPathStr = ConfigUtil.get("monitor.path");
		Preconditions.checkNotNull(monitorPathStr, "监控路径不能为空");
		Path monitorPath;
		try {
			monitorPath = Paths.get(monitorPathStr).toRealPath();
			// 启动监控程序
			new Timer(true).schedule(new LogTimer(TailerFactory.getInstance()), 0, 1000);
			// 启动文件变化消息中心服务
			new Thread(new EventService(), "EventService").start();
			init(monitorPath);
			new Thread(new WatchDirService(monitorPath, true), "WatchDirService").start();
		} catch (IOException e) {
			logger.error("EtlLog ERROR:", e);
		}
	}

	/**
	 * 初始化程序，对目标文件夹下的未处理的文件，启动tail程序
	 * 
	 * @throws IOException
	 */
	public static void init(Path monitorPath) {
		try {
			Files.walkFileTree(monitorPath, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
					EventService.putFile(path);						
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			logger.error("EtlLog init ERROR:", e);
		}
	}
}
