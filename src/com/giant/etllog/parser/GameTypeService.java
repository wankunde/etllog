package com.giant.etllog.parser;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.giant.etllog.util.ConfigUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public abstract class GameTypeService implements Runnable {

	protected final static Logger logger = LoggerFactory.getLogger(GameTypeService.class);

	private static Map<String, GameType> gametypemap = null;

	private static GameTypeService ins = new GameTypeServiceByHost();

	protected GameTypeService() {
		try {
			Map<String, GameType> map = readAllGameType();
			if (map != null && map.size() > 0)
				synchronized (GameTypeService.class) {
					gametypemap = map;
				}
		} catch (Exception e) {
			logger.error("获取gametypemap失败", e);
		}
	}

	public static GameTypeService getIns() {
		return ins;
	}

	@Override
	public void run() {
		for (;;) {
			try {
				Thread.currentThread().sleep(ConfigUtil.getInt("gametype.interval", 300000));
				Map<String, GameType> map = readAllGameType();
				if (map != null && map.size() > 0)
					synchronized (GameTypeService.class) {
						gametypemap = map;
					}
				logger.info("更新gametypemap映射表,mapsize:" + gametypemap.size());
			} catch (Exception e) {
				logger.error("更新gametypemap映射表失败", e);
			}
		}
	}

	public abstract Map<String, GameType> readAllGameType() throws Exception;

	/**
	 * @param filename
	 *            文件全路径
	 * @return 分区映射特征值
	 */
	public static GameType getGameType(Path path) {
		String idcname = "";
		String hostname = path.getParent().getParent().getFileName().toString();
		String[] sp = hostname.split("-");
		if (sp.length == 2)
			idcname = sp[1];
		else if (sp.length == 3)
			idcname = hostname.substring(0, hostname.lastIndexOf("-"));
		else 
			return null;

		synchronized (GameTypeService.class) {
			Preconditions.checkNotNull(gametypemap, "gametypemap映射表为空");
			return gametypemap.get(idcname);
		}
	}

	public static class GameType {
		private String path;
		private String game;
		private String zone;
		private String code;

		public String getPath() {
			return path;
		}

		public void setPath(String path) {
			this.path = path;
		}

		public String getGame() {
			return game;
		}

		public void setGame(String game) {
			this.game = game;
		}

		public String getZone() {
			return zone;
		}

		public void setZone(String zone) {
			this.zone = zone;
		}

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}

		public boolean check() {
			if (Strings.isNullOrEmpty(path) || Strings.isNullOrEmpty(game))
				return false;
			return true;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof GameType) {
				GameType dest = (GameType) obj;
				return Objects.equals(this.path, dest.path) && Objects.equals(this.game, dest.game)
						&& Objects.equals(this.zone, dest.zone);
			} else
				return false;
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this).append("path", path).append("game", game).append("zone", zone).toString();
		}
	}
}
