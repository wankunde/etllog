package com.giant.etllog.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.parser.GameTypeService;
import com.giant.etllog.parser.GameTypeService.GameType;
import com.google.common.base.Strings;

public class CommonUtil {

	private final static Logger logger = LoggerFactory.getLogger(CommonUtil.class);

	/**
	 * 判断是否为合法IP
	 * 
	 * @return the ip
	 */
	public static boolean isIp(String ipAddress) {
		// 定义正则表达式
		String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\." + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
				+ "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\." + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
		if (ipAddress.matches(regex))
			return true;
		else
			return false;
	}

	public static String findIp(Path path) {
		Iterator<Path> it = path.iterator();
		while (it.hasNext()) {
			String cname = it.next().getFileName().toString();
			if (isIp(cname))
				return cname;
		}
		return null;
	}

	public static boolean checkRulePath(Path path) {
		try {
			// 文件不存在,文件夹,隐藏文件,
			if (!Files.exists(path) || Files.isDirectory(path) || Files.isHidden(path))
				return false;

			// 文件必须为 xxxx.log.150106-10 格式,且日期必须在最近5天内
			if (!checkFileName(path.toString()) || Strings.isNullOrEmpty(findIp(path)))
				return false;

			// 游戏类型数据匹配游戏类型
			if (GameTypeService.getGameType(path) == null)
				return false;

			// 是否为我们要处理的目标文件
			String matches = ConfigUtil.get("monitor.matches");
			if (Strings.isNullOrEmpty(matches))
				return true;
			else
				return path.getFileName().toString().matches(matches);
		} catch (Exception e) {
			logger.error("检查文件路径失败", e);
		}
		return false;
	}

	private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HH");

	private static boolean checkFileName(String filename) {
		try {
			// 根据文件名取 天,小时
			if (filename.indexOf(".log.") < 0 || filename.length() < filename.indexOf(".log.") + 13) {
				logger.error("分割文件名失败,file name:" + filename);
			}
			String tmp = "20" + filename.substring(filename.indexOf(".log.") + 5);
			if (tmp.indexOf("-") < 0) {
				logger.warn("日志文件名不满足 xxxx.log.150106-10 格式需求,file name:" + filename);
			}

			Date date = format.parse(tmp);
			Calendar fts = Calendar.getInstance();
			fts.setTime(date);

			Calendar ts = Calendar.getInstance();
			ts.add(Calendar.DATE, -5);
			return fts.after(ts);
		} catch (Exception e) {
			logger.error("文件名解析判断失败!filename:" + filename, e);
		}
		return false;
	}
}
