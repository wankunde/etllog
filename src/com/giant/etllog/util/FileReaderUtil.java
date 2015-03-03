package com.giant.etllog.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * 可以缓存一行数据的工具类
 * 
 * @author wankun
 * @date 2014年6月23日
 * @version 1.0
 */
public class FileReaderUtil {
	
	private final static Logger logger = LoggerFactory.getLogger(FileReaderUtil.class);
	
	private Path srcPath = null;

	// byteBuffer 进行读数据,数据定位,buffer对byteBuffer进行包装
	private MappedByteBuffer byteBuffer = null;
	private long fileLength = 0;
	private long beginPos = 0;
	private RandomAccessFile file = null;

	public FileReaderUtil(Path srcPath) {
		this.srcPath = srcPath;
	}

	public synchronized void reset(long pos) throws IOException {
		if (file != null) {
			file.close();
		}

		this.beginPos = pos;
		// 1 检查要读取的文件是否存在
		Preconditions.checkState(Files.exists(srcPath), "数据文件不存在 : " + srcPath.toString());
		file = new RandomAccessFile(srcPath.toFile(), "r");
		fileLength = Files.size(srcPath);
		Preconditions.checkState(fileLength>=pos, "文件大小小于文件最后读位置", "file size:"+fileLength+"   last pos:"+pos);
		if (fileLength - pos > Integer.MAX_VALUE)
			byteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, pos, Integer.MAX_VALUE);
		else
			byteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, pos, fileLength - pos);
		byteBuffer.position(0);
		Preconditions.checkNotNull(byteBuffer, "读文件byteBuffer为空");
	}

	// Entry key:指针偏移量 value:一行数据
	private Entry<Long, String> en = null;

	// TODO 可以添加一个方法,重置buffer开始读的位置

	public synchronized boolean hasNext() {
		int pos = byteBuffer.position();
		int oldlimit = byteBuffer.limit();

		try {
			boolean needRead = true;
			while (needRead) {
				// 1.buffer已经读完毕,2文件大小-Buffer>2G--> 更新byteBuffer为下一个2G数据
				if (!byteBuffer.hasRemaining() && this.fileLength - this.beginPos > Integer.MAX_VALUE) {
					String logmsg="read new buffer ,old beginPos:" + this.beginPos + " limit:" + oldlimit+"\n";
					this.beginPos =this.beginPos+ pos;
					byteBuffer.clear();
					if (fileLength - this.beginPos > Integer.MAX_VALUE)
						byteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, beginPos, Integer.MAX_VALUE);
					else
						byteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, beginPos, fileLength - beginPos);
					byteBuffer.position(0);
					pos = byteBuffer.position();
					oldlimit = byteBuffer.limit();
					logger.info(logmsg+"read new buffer ,new beginPos:" + this.beginPos + " limit:" + oldlimit);
				}

				// 更新byteBuffer后,读数据
				if (byteBuffer.hasRemaining()) {
					byte b = byteBuffer.get();
					if (b == '\n') {
						needRead = false;
						int newpos = byteBuffer.position();
						byteBuffer.position(pos);
						byteBuffer.limit(newpos);
						ByteBuffer sb = byteBuffer.slice();
						String line = Charset.forName("GBK").decode(sb).toString();
						if (line.endsWith("\n"))
							line = line.substring(0, line.length() - 1);
						if (line.endsWith("\r"))
							line = line.substring(0, line.length() - 1);
						en = Maps.immutableEntry(beginPos + newpos, line);

						byteBuffer.position(newpos);
						byteBuffer.limit(oldlimit);
						logger.debug("reading : " + en.getKey() + "--->" + en.getValue());
						return true;
					}
				} else
					needRead = false;
			}

			// 未读取到换行符，则读取数据为最后一行
			byteBuffer.position(pos);
			byteBuffer.limit(oldlimit);
			ByteBuffer sb = byteBuffer.slice();
			String line = Charset.forName("GBK").decode(sb).toString();
			en = Maps.immutableEntry(beginPos + oldlimit, line);
			return false;
		} catch (Exception e) {
			logger.error("GBK编码解析文件失败,src文件:" + srcPath.toString(), e);
		}
		return false;
	}

	public synchronized Entry<Long, String> next() {
		Preconditions.checkNotNull(en, "读的数据行为空,请先执行hasNext方法");
		Entry<Long, String> j = en;
		en = null;
		return j;
	}

	/**
	 * 如果文件大小未发送变化,发送缓存的数据
	 * 
	 * @return
	 * @throws IOException
	 */
	public synchronized Entry<Long, String> lastLine() throws IOException {
		logger.debug("fileLength:" + fileLength + "  \t fies.size:" + Files.size(srcPath) + "\t  line:" + en.getValue());
		if (fileLength == Files.size(srcPath) && en.getValue().length() > 0) {
			return en;
		}
		return null;
	}

	public void close() {
		try {
			if (file != null)
				file.close();
		} catch (IOException e) {
			logger.error("关闭文件reader失败,srcPath:" + srcPath, e);
		}
	}
}