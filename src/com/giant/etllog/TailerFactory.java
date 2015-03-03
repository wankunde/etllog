package com.giant.etllog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.parser.LineParser;
import com.giant.etllog.util.ConfigUtil;
import com.giant.etllog.util.HdfsWriterUtil;
import com.google.common.base.Preconditions;

public class TailerFactory {
	
	private final static Logger logger = LoggerFactory.getLogger(TailerFactory.class);

	/**
	 * 工厂单实例
	 */
	private static TailerFactory factory = new TailerFactory();

	private TailerFactory() {
		Preconditions.checkState(ConfigUtil.getBoolean("hdfs.append.support", true), "hdfs 不支持追加写,程序退出");
	}

	public static TailerFactory getInstance() {
		return factory;
	}

	private volatile Map<String, Tailer> tailers = new HashMap<>();
	public volatile List<HdfsWriterUtil> writers = new ArrayList<>();
	private final static Semaphore sem = new Semaphore(ConfigUtil.getInt("tailer.num", 50));

	public Tailer create(Path path) throws Exception {
		String hdfspath = LineParser.getLineParser(path).getHdfsPath(path);
		Preconditions.checkNotNull(hdfspath);
		
		String srcPath = path.toString();
		// 获取创建新的tailer令牌
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			logger.error("令牌获取出错",e);
		}

		// 如发现已经实例化tailer,释放令牌,否则先占坑再初始化
		synchronized (tailers) {
			if (tailers.containsKey(srcPath)) {
				sem.release();
				return null;
			} else {
				tailers.put(srcPath, null);
			}
		}

		try {
			Tailer tailer = null;
			synchronized (writers) {
				// 1.取hdfs写句柄
				HdfsWriterUtil writer = null;
				for (HdfsWriterUtil w : writers) {
					if (hdfspath.equals(w.getHdfspath())) {
						logger.info("found writer  writer's path :" + w.getHdfspath() + "  dest path :" + hdfspath);
						writer = w;
					}
				}

				if (writer == null) {
					writer = createWriter(hdfspath);
					Preconditions.checkNotNull(writer, "获取hdfs writer 失败!");
					Preconditions.checkNotNull(writer.getHdfspath(), "hdfspath is null");
					// 注册组件
					writers.add(writer);
				}

				// 2.根据hdfs句柄创建读文件线程
				logger.info("creating tailer :" + srcPath);
				tailer = new Tailer(srcPath, writer, this);
				Preconditions.checkNotNull(tailer, "获取hdfs writer 失败!");
				Preconditions.checkNotNull(tailer.getSrcPath(), "获取hdfs writer 失败!");

				// Tailer创建成功,并向两个组件进行注册
				writer.addTailer(tailer);
			}

			synchronized (tailers) {
				tailers.put(srcPath, tailer);
			}
			return tailer;
		} catch (Exception e) {
			synchronized (tailers) {
				tailers.remove(srcPath);
			}
			logger.error("creating tailer error:" + srcPath, e);
			sem.release();
			return null;
		}
	}

	/**
	 * 
	 * @param path
	 * @return
	 * @throws InterruptedException
	 */
	public Tailer getTailer(String path) {
		synchronized (tailers) {
			return tailers.get(path);
		}
	}

	public void removeTailer(Tailer tailer, HdfsWriterUtil writer) {
		logger.info("removing tailer :" + tailer.getSrcPath());
		try {
			synchronized (tailers) {
				tailers.remove(tailer.getSrcPath().toString());
			}

			synchronized (writers) {
				logger.info("removing tailer  writer path:" + writer.getHdfspath() + "  writer size--> "
						+ writer.getTailerSize().get());
				writer.removeTailer(tailer);
				if (writer.getTailerSize().get() == 0) {
					// 这里先移除该writer,防止下面关闭时关闭出错
					HdfsWriterUtil dest = null;
					for (HdfsWriterUtil w : writers) {
						if (w.equals(writer)) {
							dest = w;
						}
					}

					if (dest != null) {
						logger.info("removing writer :" + writer.getHdfspath());
						writers.remove(dest);
					}

					writer.close();
				}
			}
		} catch (Exception e) {
			logger.error("remove tailer  error :" + tailer.getSrcPath(), e);
		}
		sem.release();
	}

	public int getTailersSize() {
		synchronized (tailers) {
			return tailers.size();
		}
	}

	private HdfsWriterUtil createWriter(String hdfspath) throws IOException {
		HdfsWriterUtil writer = null;
		try {
			writer = new HdfsWriterUtil(hdfspath);
		} catch (Exception e) {
			logger.error("创建HDFS Writer失败,retry...", e);
			writer = new HdfsWriterUtil(hdfspath);
		}
		return writer;
	}
}
