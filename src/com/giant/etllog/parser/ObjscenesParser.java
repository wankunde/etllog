package com.giant.etllog.parser;

import java.nio.file.Path;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.parser.GameTypeService.GameType;
import com.giant.etllog.util.ConfigUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class ObjscenesParser extends LineParser {

	private final static Logger logger = LoggerFactory.getLogger(ObjscenesParser.class);

	protected final String HDFS_FILE_PREFIX = ConfigUtil.get("hdfs.file.prefix");

	@Override
	public void initParser() throws Exception {
	}

	@Override
	public String getHdfsPath(Path path) throws Exception {
		String hdfspath = HDFS_FILE_PREFIX;
		if (hdfspath.endsWith("/"))
			hdfspath = hdfspath.substring(0, hdfspath.length() - 1);
		
		GameType gametype = GameTypeService.getGameType(path);
		if (gametype == null) {
			logger.error("文件:" + path.toString() + "  gametype 查询为空");
			return null;
		}

		String filename = path.getFileName().toString();
		if (filename.indexOf(".log.") < 0) {
			throw new Exception("日志文件解析失败失败,file name:" + filename);
		}

		String hdfsname = filename.substring(filename.indexOf(".log.") + 5);
		Preconditions.checkArgument(hdfsname.length()>=9, "log文件名不是yymmdd-hh结尾的");
		String hdfsdir = hdfsname.substring(0, 6)+"/"+hdfsname.substring(7,9);
		
		return hdfspath +"/"+gametype.getGame()+ "/" + hdfsdir + "/" + hdfsname+"-"+gametype.getZone();
	}

	@Override
	public String parseLine(Path path, String line) {
		GameType gametype = GameTypeService.getGameType(path);
		if (gametype == null) {
			logger.error("文件:" + path.toString() + "  gametype 查询为空");
			return null;
		}

		String[] fields = line.split(",");
		if (fields.length < 12) {
			logger.warn("字段分割没有12个字段--->" + line);
			return null;
		}

		if (fields[0].indexOf("WS") < 15 || fields[0].indexOf("DEBUG") < 15) {
			logger.warn("数据不符合规范--->" + line);
			return null;
		}

		String logtime = fields[0].substring(0, fields[0].indexOf("WS") - 1);
		String buildID = fields[0].substring(fields[0].indexOf("DEBUG") + 6);
		return Joiner.on("\t").join(new String[] { logtime, gametype.getZone(), buildID }) + "\t"
				+ Joiner.on("\t").join(Arrays.copyOfRange(fields, 1, fields.length)) + "\n";
	}
}
