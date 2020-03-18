package com.xiruibin.db.util;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.xiruibin.DBDriverAutoLoad;
import com.xiruibin.DBInfo;
import com.xiruibin.Log;
import com.xiruibin.Parameters;
import com.xiruibin.SerializationUtil;

public final class DBUtils {

	private static final String TABLE_INFO_FILE_NAME = "table.cache";
	private static final String DB_INFO_FILE_NAME = "db.cache";
	private String cacheDir = this.getClass().getResource("/").getPath();
	private Parameters parameters = null;
	private Connection conn = null;
	private Statement stm = null;
	private ResultSet rs = null;
	private Map<String, String> tableinfo = new LinkedHashMap<String, String>();
	private Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> dbInfo = new HashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>>();
	
	private Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> lastDbInfo = new HashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>>();
	private Map<String, String> lastTableInfo = new LinkedHashMap<String, String>();
	
	private Map<String, String> updateLog = new HashMap<String, String>();
	
	{
		loadCache();
	}
	
	public DBUtils(Parameters parameters) {
		this.parameters = parameters;
		DBDriverAutoLoad.load();
	}
	
	public void loadCache() {
		Log.info("正在加载上次数据库缓存...");
		try {
			lastDbInfo = (Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>>) SerializationUtil.deserialize(cacheDir + File.separator + DB_INFO_FILE_NAME);
			lastTableInfo = (Map<String, String>) SerializationUtil.deserialize(cacheDir + File.separator + TABLE_INFO_FILE_NAME);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void writeCache() {
		Log.info("正在写入最新数据库缓存...");
		try {
			SerializationUtil.serialize(getDatabaseInfo(), cacheDir + File.separator + DB_INFO_FILE_NAME);
			SerializationUtil.serialize(getTableInfo(), cacheDir + File.separator + TABLE_INFO_FILE_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Map<String, String> compare() {
		// compare table info
		for (Entry<String, String> entry : tableinfo.entrySet()) {
			if (!lastTableInfo.containsKey(entry.getKey())) {
				updateLog.put(entry.getKey(), String.format("新增【%s】表", entry.getKey()));
			}
		}
		
		// compare columns
		for (Entry<String, LinkedHashMap<String, LinkedHashMap<String, String>>> entry : dbInfo.entrySet()) {
			List<String> insertLog = new ArrayList<String>();
			List<String> updateFieldLog = new ArrayList<String>();
			
			String tableName = entry.getKey();
			if (!lastDbInfo.containsKey(tableName) && updateLog.containsKey(tableName)) {
				updateLog.put(tableName, String.format("新增【%s】表", tableName));
				continue;
			}
			
			StringBuilder logSb = new StringBuilder();
			Map<String, LinkedHashMap<String, String>> lastColumnMap = lastDbInfo.get(tableName);
			Map<String, LinkedHashMap<String, String>> columnMap = entry.getValue();
			for (Entry<String, LinkedHashMap<String, String>> column : columnMap.entrySet()) {
				String columnName = column.getKey();
				if (!lastColumnMap.containsKey(columnName)) {
					insertLog.add(columnName);
					continue;
				}
				
				Map<String, String> lastDescMap = lastColumnMap.get(columnName);
				Map<String, String> descMap = column.getValue();
				for (Entry<String, String> desc : descMap.entrySet()) {
					String descField = desc.getKey();
					String descValue = desc.getValue();
					String lastDescValue = lastDescMap.get(descField);
					if (!descValue.equals(lastDescValue)) {
						updateFieldLog.add(String.format("%s修改为%s", descField, entry.getValue()));
					}
				}
			}
			
			if (insertLog.size() > 0) {
				logSb.append("新增");
				logSb.append(StringUtils.join(insertLog.toArray(new Object[0]), ','));
				logSb.append("字段");
			}
			
			if (updateFieldLog.size() > 0) {
				logSb.append("修改字段：");
				logSb.append(StringUtils.join(updateFieldLog.toArray(new Object[0]), ','));
			}
			
			if (logSb.toString().trim().length() > 0) {
				updateLog.put(tableName, logSb.toString());
			}
		}
		return this.updateLog;
	}
	
	public void load() throws SQLException {
		Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> info = new LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>>();
		conn = DriverManager.getConnection(
				DBInfo.getCurrentDriverUrl(parameters.getHost(), parameters.getPort(), parameters.getDatabase()),
				parameters.getUser(), parameters.getPassword());
		DatabaseMetaData dmd = conn.getMetaData();
		String schema = null;
		if (parameters.getSchema() != null) {
			schema = parameters.getSchema().toUpperCase();
		}

		ResultSet dbrs = dmd.getTables(null, schema, null, new String[] { "TABLE" });
		int n = 0;
		while (dbrs.next()) {
			String table_name = dbrs.getString("TABLE_NAME");
			if (!parameters.getTables().isEmpty()) {
				if (!parameters.getTables().contains(table_name)) {
					continue;
				}
			}
//			 if (n>27)
//				 break;

			// 获取表的主键
			List<String> pkList = new ArrayList<String>();
			ResultSet pkInfo = dmd.getPrimaryKeys(null, schema, table_name);
			while (pkInfo.next()) {
				pkList.add(pkInfo.getString("COLUMN_NAME"));
			}

			LinkedHashMap<String, LinkedHashMap<String, String>> tablesMap = info.get(table_name);
			if (tablesMap == null) {
				tablesMap = new LinkedHashMap<String, LinkedHashMap<String, String>>();
			}

			// tablesMap.put(column_name, columnInfo);

			Log.info("===========================" + table_name + "===========================");
			ResultSetMetaData rsmd = dbrs.getMetaData();
			String remark = dbrs.getString("REMARKS");
			if (remark != null && !"null".equals(remark)) {
				tableinfo.put(dbrs.getString("TABLE_NAME"), remark);
			} else {
				tableinfo.put(dbrs.getString("TABLE_NAME"), "");
			}

			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				rsmd.getColumnName(i);
				Log.info(rsmd.getColumnName(i) + ":" + dbrs.getString(i));
			}

			Log.info("----------------------------------------------------------");
			ResultSet rsc = getColumns(table_name);
			ResultSetMetaData rscmd = rsc.getMetaData();
			while (rsc.next()) {
				String column_name = rsc.getString("COLUMN_NAME");
				LinkedHashMap<String, String> columnInfo = tablesMap.get(column_name);
				if (columnInfo == null) {
					columnInfo = new LinkedHashMap<String, String>();
				}
				
				try {
					columnInfo.put("column_comment", rsc.getString("REMARKS"));
				} catch (SQLException e) {
					columnInfo.put("column_comment", "");
				}

				String typeName = rsc.getString("TYPE_NAME");
				if ("VARCHAR".equals(typeName) || "CHAR".equals(typeName)) {
					columnInfo.put("column_type", typeName + "(" + rsc.getString("COLUMN_SIZE") + ")");
				} else if ("INT".equals(typeName) || "INTEGER".equals(typeName) || "BIGINT".equals(typeName) || "TINYINT".equals(typeName) || "SMALLINT".equals(typeName) || "MEDIUMINT".equals(typeName)) {
					columnInfo.put("column_type", typeName + "(" + rsc.getString("COLUMN_SIZE") + ")");
				} else if ("DECIMAL".equals(typeName) || "FLOAT".equals(typeName) || "DOUBLE".equals(typeName) || "NUMERIC".equals(typeName)) {
					columnInfo.put("column_type", typeName + "(" + rsc.getString("COLUMN_SIZE") + "," + rsc.getInt("DECIMAL_DIGITS") + ")");
				} else {
					columnInfo.put("column_type", typeName);
				}

				String def = String.valueOf(rsc.getObject("COLUMN_DEF"));
				if ("null".equals(def)) {
					columnInfo.put("column_default", "");
				} else {
					columnInfo.put("column_default", def);
				}

				String key = rsc.getString("IS_AUTOINCREMENT");
				if ("YES".equals(key)) {
					columnInfo.put("column_si", "√");
				} else {
					columnInfo.put("column_si", "");
				}

				if (pkList.contains(column_name)) {
					columnInfo.put("column_key", "√");
				} else {
					columnInfo.put("column_key", "");
				}

				if ("YES".equals(rsc.getString("IS_NULLABLE"))) {
					columnInfo.put("is_nullable", "");
				} else {
					columnInfo.put("is_nullable", "Ｘ");
				}

				StringBuilder sb = new StringBuilder();
				for (int j = 1; j <= rscmd.getColumnCount(); j++) {
					sb.append(rscmd.getColumnName(j)).append("[").append(rsc.getObject(j)).append("]").append(" ");
				}
				Log.info(sb.toString());
				tablesMap.put(column_name, columnInfo);
			}
			info.put(table_name, tablesMap);
			Log.info("");
			n++;
		}

		stm = conn.createStatement();

		releaseDBResource();
		this.dbInfo = info;
	}
	
	public Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> getDatabaseInfo() {
		return this.dbInfo;
	}

	public Map<String, String> getTableInfo() {
		return tableinfo;
	}

	public ResultSet getColumns(String tableName) {
		try {
			DatabaseMetaData db2dmd = conn.getMetaData();
			String schema = null;
			if (parameters.getSchema() != null) {
				schema = parameters.getSchema().toUpperCase();
			}
			return db2dmd.getColumns(null, schema, tableName, null);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 释放资源
	 * 
	 * @throws SQLException
	 */
	public void releaseDBResource() throws SQLException {
		if (null != rs) {
			rs.close();
		}

		if (null != stm) {
			stm.close();
		}

		if (null != conn) {
			conn.close();
		}
	}

}