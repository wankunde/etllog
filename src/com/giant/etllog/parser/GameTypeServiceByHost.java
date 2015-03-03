package com.giant.etllog.parser;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.giant.etllog.util.ConfigUtil;
import com.google.common.base.Strings;

/**
 * <pre>
 * 测试库:
 * host: 10.10.102.26
 * user: datacter
 * pswd: ztgame@!@#$%^
 * 
 * dbname:  zoneInfo_DC
 * tbname:  zoneInfo
 * </pre>
 * 
 * @author wankun
 * @date 2015年1月7日
 * @version 1.0
 */
public class GameTypeServiceByHost extends GameTypeService {

	/**
	 * <pre>
	 * 由ip转换为游戏区号服务
	 * 	江湖	38
	 * 	征途经典版	40
	 * </pre>
	 */
	public Map<String, GameType> readAllGameType() throws Exception {
		Map<String, GameType> map = new HashMap<>();
		Connection conn = null;
		try {
			// 1. 从数据库中拖数据
			conn = getConn();
			String sql = "select game,substr(desc_order,1,instr(desc_order,'_')-1) as idcname from zoneInfo where isUse=1 ";
			String gametype = ConfigUtil.get("monitor.gametype");
			if (!Strings.isNullOrEmpty(gametype))
				sql = sql.concat("and game in ( " + gametype + " )");

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String gameid = rs.getString(1);
				String idcname = rs.getString(2);
				GameType gt = new GameType();
				gt.setPath(idcname);
				gt.setGame(gameid);
				gt.setZone(idcname);
				map.put(idcname, gt);
			}
			logger.info("成功获取hostname->game,zone映射数据," + map.size());
			return map;
		} catch (Exception e) {
			logger.error("mysql数据库查询失败", e);
			throw e;
		} finally {
			closeConn(conn);
		}
	}

	private static Connection getConn() throws ClassNotFoundException, SQLException {
		Connection conn = null;

		Class.forName("com.mysql.jdbc.Driver");
		String url = ConfigUtil.get("mysql.url");
		String user = ConfigUtil.get("mysql.user");
		String password = ConfigUtil.get("mysql.password");
		conn = DriverManager.getConnection(url, user, password);
		return conn;
	}

	private static void closeConn(Connection conn) {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			logger.error("close mysql连接失败", e);
		}
	}
}
