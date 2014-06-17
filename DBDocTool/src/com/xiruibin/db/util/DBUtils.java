package com.xiruibin.db.util;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import com.ibm.db2.jcc.DB2DatabaseMetaData;
import com.xiruibin.Parameters;

public final class DBUtils {

    private Parameters parameters = null;

    private Connection conn       = null;

    private Statement  stm        = null;

    private ResultSet  rs         = null;

    static {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");// 加载数据库驱动
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public DBUtils(Parameters parameters){
        this.parameters = parameters;
    }

    public Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> getDatabaseInfo() throws Exception {
        Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> info = new LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>>();
        conn = DriverManager.getConnection("jdbc:db2://" + parameters.getHost() + ":" + parameters.getPort()
                                           + "/"+ parameters.getDatabase(), parameters.getUser(), parameters.getPassword());
        DB2DatabaseMetaData dmd = (DB2DatabaseMetaData) conn.getMetaData();
        ResultSet rs = dmd.getTables(null, parameters.getSchema().toUpperCase(), null, new String[]{"TABLE"});
        int n = 0;
        while(rs.next()) {
//        	if (n>5)
//        		break;
        	String table_name = rs.getString("TABLE_NAME");
        	//String table_name = "ES_SHOP_JIFEN_LOG";
        	LinkedHashMap<String, LinkedHashMap<String, String>> tablesMap = info.get(table_name);
            if (tablesMap == null) {
                tablesMap = new LinkedHashMap<String, LinkedHashMap<String, String>>();
            }
            

            //tablesMap.put(column_name, columnInfo);
            
        	System.out.println("===========================" + rs.getString("TABLE_NAME") + "===========================");
        	ResultSetMetaData rsmd = rs.getMetaData();
        	for (int i = 1; i<=rsmd.getColumnCount(); i++) {
        		rsmd.getColumnName(i);
        		System.out.println(rsmd.getColumnName(i) + ":" + rs.getString(i));
        	}
        	
        	System.out.println("----------------------------------------------------------");
        	ResultSet rsc = getColumns(table_name);
        	ResultSetMetaData rscmd = rsc.getMetaData();
        	while(rsc.next()) {
        		String column_name = rsc.getString("COLUMN_NAME");
                LinkedHashMap<String, String> columnInfo = tablesMap.get(column_name);
                if (columnInfo == null) columnInfo = new LinkedHashMap<String, String>();
                try {
                	columnInfo.put("column_comment", rsc.getString("REMARKS"));
                } catch (SQLException e) {
                	columnInfo.put("column_comment", "");
                }
                
                String typeName = rsc.getString("TYPE_NAME");
                if ("CHAR".equals(typeName) || "VARCHAR".equals(typeName)) {
                	columnInfo.put("column_type", typeName + "(" + rsc.getString("COLUMN_SIZE") + ")");
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
                if ("YES".equals(key)) {
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
	        	for (int j = 1; j<=rscmd.getColumnCount(); j++) {
	        		sb.append(rscmd.getColumnName(j)).append("[").append(rsc.getObject(j)).append("]").append(" ");
	        	}
	        	System.out.println(sb.toString());
	        	tablesMap.put(column_name, columnInfo);
        	}
        	info.put(table_name, tablesMap);
        	System.out.println("");
        	n++;
        }
        stm = conn.createStatement();

        releaseDBResource();
        return info;
    }
    
    public ResultSet getColumns(String tableName) {
    	try {
			DB2DatabaseMetaData db2dmd = (DB2DatabaseMetaData) conn.getMetaData();
			return db2dmd.getColumns(null, parameters.getSchema().toUpperCase(), tableName, null);
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