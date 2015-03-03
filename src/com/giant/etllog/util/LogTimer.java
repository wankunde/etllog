package com.giant.etllog.util;

import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.EventService;
import com.giant.etllog.TailerFactory;

public class LogTimer extends TimerTask {
	
	private final static Logger logger = LoggerFactory.getLogger(LogTimer.class);

	private TailerFactory factory = null;

	public LogTimer(TailerFactory factory) {
		this.factory = factory;
	}

	private static AtomicLong counter = new AtomicLong(0);

	public static void addRownum(long rows){
		counter.addAndGet(rows);
	}
	
	@Override
	public void run() {
		StringBuffer sinkmsg = new StringBuffer("\n----------------HDFS sink 统计------------------\n");
		long nums = 0;
		int i = 0;

		synchronized (factory.writers) {
			for (HdfsWriterUtil writer : factory.writers) {
				Entry<String, Long> en = writer.resetCounter();
				sinkmsg.append(en.getKey());
				nums = nums + en.getValue();
				i = i + writer.getTailerSize().get();
			}
		}
		
		sinkmsg.append("  counter nums:" + counter.get());
		counter.set(0);
		sinkmsg.append("  rate nums:" + nums);
		sinkmsg.append("  event queue size: " + EventService.getQueueSize());
		sinkmsg.append("  tailers size : " + TailerFactory.getInstance().getTailersSize());
		sinkmsg.append("  hdfs writer中tailer size: " + i);
		logger.info(sinkmsg.toString());
	}

}
