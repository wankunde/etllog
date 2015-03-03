package com.giant.etllog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.parser.LineParser;
import com.giant.etllog.util.FileReaderUtil;
import com.giant.etllog.util.HdfsWriterUtil;
import com.giant.etllog.util.JedisUtil;
import com.giant.etllog.util.LogTimer;
import com.google.common.base.Preconditions;

public class Tailer implements Runnable {
	
	private final static Logger logger = LoggerFactory.getLogger(Tailer.class);

	private Path srcPath = null; // 数据源文件
	private long lastPos = 0; // 当前已经发送到HDFS writer的位置
	private TailerFactory factory = null;

	private FileReaderUtil reader = null;
	private LineParser parser = null;
	private HdfsWriterUtil writer = null;

	private boolean needreset; // 外界有新变化事件过来

	public Tailer(String path, HdfsWriterUtil writer, TailerFactory factory) {
		try {
			this.srcPath = Paths.get(path).toRealPath();

			this.factory = factory;
			this.needreset = true;
			this.reader = new FileReaderUtil(srcPath);
			this.writer = writer;
			this.parser = LineParser.getLineParser(srcPath);
			logger.debug("new Tailer :" + this.srcPath + "  ");
		} catch (Exception e) {
			this.srcPath = null;
			logger.warn("创建Tailer出错", e);
		}
	}

	public void run() {
		try {
			// pos 不能比当前文件的还大
			lastPos = JedisUtil.get(srcPath.toString());
			if (lastPos > Files.size(srcPath)) {
				logger.error("文件大小发生变化,file:" + srcPath.toString() + " \t 原文件指针:" + lastPos + "\t 新文件大小:"
						+ Files.size(srcPath));
				needreset = false;
			}

			// 4 获取HDFS写连接
			while (needreset) {
				needreset = false;
				reader.reset(lastPos);
				StringBuffer msgbuffer = new StringBuffer();
				long nums = 0;

				while (reader.hasNext()) {
					Entry<Long, String> en = reader.next();
					String conv = parser.parseLine(srcPath, (String) en.getValue());
					if (conv != null) {
						msgbuffer.append(conv);
						nums++;
						lastPos = en.getKey();
					}
				}
				// 判断缓存的最后一条记录是否为最后一条记录
				Thread.currentThread().sleep(1000);
				Entry<Long, String> en = reader.lastLine();
				if (en != null) {
					String conv = parser.parseLine(srcPath, (String) en.getValue());
					if (conv != null) {
						msgbuffer.append(conv);
						nums++;
						lastPos = en.getKey();
					}
				}

				try {
					writer.append(nums, msgbuffer.toString());
					logger.info("redis insert "+srcPath.toString()+":"+ lastPos);
					JedisUtil.put(srcPath.toString(), lastPos);
					LogTimer.addRownum(nums);
				} catch (Exception e) {
					logger.error(
							"writer flush error. tailer srcpath:" + srcPath + "  writer path:" + writer.getHdfspath(),
							e);
					logger.error("writer中tailerList size:" + writer.getTailerSize().get());
					EventService.putFile(srcPath);
				}
			}
		}  catch (Exception e) {
			logger.error("tailer运行异常错误", e);
		} finally {
			if (reader != null)
				reader.close();

			Tailer tailer = factory.getTailer(srcPath.toString());
			Preconditions.checkNotNull(tailer, "tailer 获取失败: srcPath" + srcPath);
			factory.removeTailer(tailer, writer);
		}
	}

	public void setNeedreset(boolean needreset) {
		this.needreset = needreset;
	}

	public Path getSrcPath() {
		return srcPath;
	}
}
