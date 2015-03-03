package com.giant.etllog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.util.CommonUtil;
import com.giant.etllog.util.ConfigUtil;
import com.giant.etllog.util.JedisUtil;

/**
 * 文件变化消息中心
 * 
 * @author wankun
 * @date 2014年6月24日
 * @version 1.0
 */
public class EventService implements Runnable {
	
	private final static Logger logger = LoggerFactory.getLogger(EventService.class);
	
	private TailerFactory factory = TailerFactory.getInstance();
	private static Set<String> eventqueue = new HashSet<>();
	private ExecutorService executerService = Executors.newCachedThreadPool();

	protected final int BATCH_SIZE = ConfigUtil.getInt("event.batchSize", 500);
	protected final int BATCH_SLEEP = ConfigUtil.getInt("event.batchSleep", 50);

	public static void putFile(Path path) {
		try {
			if (CommonUtil.checkRulePath(path)){
				logger.info("adding path : "+path.toString());
				synchronized (eventqueue) {
					eventqueue.add(path.toString());
				}
			}
		} catch (Exception e) {
			logger.error("放文件变化消息失败,file:" + path.toString(), e);
		}
	}

	public static int getQueueSize() {
		return eventqueue.size();
	}

	@Override
	public void run() {
		while (true) {
			// 批量取待处理文件,批量设置为500个
			List<String> files = new ArrayList<>();
			int i = 0;
			synchronized (eventqueue) {
				Iterator<String> it = eventqueue.iterator();
				while (it.hasNext() && i < BATCH_SIZE) {
					String f1 = it.next();
					files.add(f1);
					i++;
				}
			}
			try {
				if (files.size() == 0) {
					Thread.currentThread().sleep(BATCH_SLEEP);
					continue;
				}
				Map<String, Long> filespos = null;
				try {
					filespos = JedisUtil.mget(files.toArray(new String[files.size()]));
				} catch (Exception e) {
					filespos = JedisUtil.mget(files.toArray(new String[files.size()]));
				}
				synchronized (eventqueue) {
					for (String f1 : files)
						eventqueue.remove(f1);
				}
				for (Map.Entry<String, Long> en : filespos.entrySet()) {
					String fullname = en.getKey();
					long filepos = en.getValue();
					if (Files.exists(Paths.get(fullname)) && filepos != Files.size(Paths.get(fullname))) {
						Tailer tailer = factory.getTailer(fullname);
						if (tailer == null) {
							tailer = factory.create(Paths.get(fullname)); // 在创建的时候还是有可能为空
							if (tailer != null) {
								executerService.submit(tailer); // 启动新线程处理程序
								logger.debug("watch starting tailing file : " + fullname);
							} else {
								logger.error("创建文件tailer失败" + fullname);
							}
						} else {
							tailer.setNeedreset(true);
						}
					} else {
						logger.warn("文件已经读完毕 --> " + fullname);
					}
				}
			} catch (Exception e) {
				logger.error("eventqueue 处理出错", e);
			}
		}
	}
}
