package com.giant.etllog.util;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.Tailer;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

/**
 * 
 * <p>
 * Hdfs sink 线程，每个实例负责将多个文件中的消息发送到一个hdfs的文件中
 * </p>
 * 
 * @author wankun
 * @date 2014年6月11日
 * @version 1.0
 */
public class HdfsWriterUtil {
	
	private final static Logger logger = LoggerFactory.getLogger(HdfsWriterUtil.class);

	// 数据写入计数器
	private AtomicLong counter1 = new AtomicLong(0l);

	private AtomicInteger tailerSize = new AtomicInteger(0);

	// 实例变量
	private FSDataOutputStream outStream;
	private String hdfspath;

	public HdfsWriterUtil(String hdfspath) throws IOException {
		this.hdfspath = hdfspath;
		Path dstPath = new Path(hdfspath);
		
		DistributedFileSystem hdfs=(DistributedFileSystem)FileSystem.get(ConfigUtil.getConf());
		synchronized (hdfs) {
			if (hdfs.isFile(dstPath))
				try {
					outStream = hdfs.append(dstPath);
				} catch (Exception e) {
					logger.info("try to recover file Lease : " + hdfspath);
					hdfs.recoverLease(dstPath);
					boolean isclosed = hdfs.isFileClosed(dstPath);
					Stopwatch sw = new Stopwatch().start();
					while (!isclosed) {
						// 超过1分钟,程序仍未解锁,程序退出
						if (sw.elapsedMillis() > 60 * 1000)
							throw e;
						try {
							Thread.currentThread().sleep(1000);
						} catch (InterruptedException e1) {
						}
						isclosed = hdfs.isFileClosed(dstPath);
					}
					logger.info("file is closed" + dstPath);
					outStream = hdfs.append(dstPath);
				}
			else
				outStream = hdfs.create(dstPath);
		}
	}

	@Override
	public boolean equals(Object obj) {
		HdfsWriterUtil w = (HdfsWriterUtil) obj;
		return this.hdfspath.equals(w.hdfspath);
	}
	
	@Override
	public int hashCode() {
		return this.hdfspath.hashCode();
	}

	public String getHdfspath() {
		return hdfspath;
	}

	public void addTailer(Tailer tailer) {
		tailerSize.incrementAndGet();
	}

	public void removeTailer(Tailer tailer) {
		tailerSize.decrementAndGet();
	}
	
	public AtomicInteger getTailerSize() {
		return tailerSize;
	}

	/**
	 * 在数据写入和计数的时候，对方法加锁，防止出错
	 * 
	 * @param data
	 * @throws IOException
	 */
	public synchronized void append(long nums, String data) throws IOException {
		counter1.addAndGet(nums);
		outStream.write(data.getBytes());
		outStream.flush();
		outStream.hflush();
	}

	public void close() throws IOException {
		// IOUtils.closeStream(outStream);
		outStream.close();
	}

	public Entry<String, Long> resetCounter() {
		long val = counter1.get();
		counter1.set(0l);
		String info = "  path:" + hdfspath + "  rate nums:" + val + " current tailer:" + tailerSize.get() + "\n";
		return Maps.immutableEntry(info, val);
	}
}
