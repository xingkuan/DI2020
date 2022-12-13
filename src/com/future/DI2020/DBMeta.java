package com.future.DI2020;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;

class DBMeta {
	private Connection repConn;
	private Statement repStmt;
	private ResultSet repRSet;

	private static final Logger logger = LogManager.getLogger();

	private static Map<String, JSONObject> instructions=new HashMap<>();

	private JSONObject jdbcDataTypeMap=null;
	
	private static DBMeta instance = null; // use lazy instantiation ;

	public static DBMeta getInstance() {
		if (Objects.isNull(instance)) {
			instance = new DBMeta();
			instance.initMeta();
		}
		return instance;
	}

	private DBMeta() {
		Conf conf = Conf.getInstance();
		String fileName = conf.getConf("jdbcDataTypeMap");
	    try {
	    	String typeText = Files.readString(Paths.get(fileName));
	    	jdbcDataTypeMap = stringToJSONObject(typeText.toUpperCase());
	      } catch (Exception e) {
	          System.out.println(e);
	      }
	}

	private void initMeta() {
		Conf conf = Conf.getInstance();
		String uID = conf.getConf("repDBuser");
		String uPW = conf.getConf("repDBpasswd");
		String url = conf.getConf("repDBurl");
		String dvr = conf.getConf("repDBdriver");

		try {
			Class.forName(dvr);
		} catch (ClassNotFoundException e) {
			logger.error("DB Driver error has occured");
			logger.error(e);
		}

		try {
			repConn = DriverManager.getConnection(url, uID, uPW);
			repConn.setAutoCommit(false);
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		} catch (SQLException e) {
			logger.error(e);
		}
	}

	public String avroSchFromCols(String tbl, JSONArray cols) {
		return null;
	}
	
	public JSONObject getInstrs(String dbid) {
		return instructions.get(dbid);
	}
	
	public String getCDCNameTemp(String dbid, String tblName) {
		String sql= "select instruct "
					+ " from DATA_POINT " + " where db_id='" + dbid + "'";
		JSONArray ja = SQLtoJSONArray(sql);
		JSONObject jo;
		String cdcTblName=null;
		int nameMaxLen;
		if (ja != null) {
			jo = (JSONObject) ja.get(0);
			String instruStr = (String) jo.get("instruct");
			jo = stringToJSONObject(instruStr);
			cdcTblName = (String) jo.get("cdcObj");
			Long tempL = (Long)jo.get("cdcObjNameLen");  //somehow, it is not Integer.
			if(null!=tempL) {
				nameMaxLen = tempL.intValue();
				if(tblName.length()>nameMaxLen) {//
					int randNum = (int) (Math.random()*9);
					tblName=tblName.subSequence(0, nameMaxLen)+String.valueOf(randNum);
				}
			}
			cdcTblName = cdcTblName.replace("<DISRCTBL>", tblName);
		}
		
		return cdcTblName;
	}
	private JSONObject stringToJSONObject(String str) {
		JSONObject rslt;
		JSONParser parser = new JSONParser();
		
		try {
			rslt = (JSONObject) parser.parse(str);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			rslt = null;
		}
		
		return rslt;
	}

	
	// return db details as a simpleJSON object, (instead of a cumbersome POJO).
	// used only by DataPointMgr
	public JSONObject getDB(String dbid) {
		String sql= "select d.db_id, d.db_cat, d.db_role, d.db_conn, e.db_driver, d.db_engine, "
				+ "d.db_usr, d.db_pwd, d.instruct "
					+ " from DATA_POINT d, DB_ENGINE e where d.db_engine=e.db_engine and d.db_id='" + dbid + "'";
		JSONArray ja = SQLtoJSONArray(sql);
		JSONObject rslt;
		if (ja == null) {
			rslt = null;
		}else {
			rslt = (JSONObject) ja.get(0);
		}
		return rslt;
	}
	
	public JSONArray getDCCsByPoolID(int poolID) {
	String sql = "select src_db_id, tgt_db_id, src_jurl_name from task where pool_id = " + poolID;

	JSONArray jRslt = SQLtoJSONArray(sql);
	return jRslt;
	}
	public JSONArray SQLtoJSONArray(String sql) {
		JSONArray rslt = null;
		JSONObject jsonObject = null;

		JSONObject jo = new JSONObject();
		Statement stmt = null;
		ResultSet rset = null;
		String column;
		Object value;
		try {
			rslt = new JSONArray();
			stmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rset = stmt.executeQuery(sql);

			ResultSetMetaData rsmd = rset.getMetaData();
			int columnCount = rsmd.getColumnCount();

			while (rset.next()){
				jsonObject = new JSONObject();
				for (int index = 1; index <= columnCount; index++) {
					column = rsmd.getColumnName(index);
					value = rset.getObject(column);
					if (value == null) {
						jsonObject.put(column, "");
					} else if (value instanceof Integer) {
						jsonObject.put(column, (Integer) value);
					} else if (value instanceof String) {
						jsonObject.put(column, (String) value);
					} else if (value instanceof Boolean) {
						jsonObject.put(column, (Boolean) value);
					} else if (value instanceof Date) {
						jsonObject.put(column, ((Date) value).getTime());
					} else if (value instanceof Long) {
						jsonObject.put(column, (Long) value);
					} else if (value instanceof Double) {
						jsonObject.put(column, (Double) value);
					} else if (value instanceof Float) {
						jsonObject.put(column, (Float) value);
					} else if (value instanceof BigDecimal) {
						jsonObject.put(column, (BigDecimal) value);
					} else if (value instanceof Timestamp) {
						jsonObject.put(column, (Timestamp) value);
					} else if (value instanceof Byte) {
						jsonObject.put(column, (Byte) value);
					} else if (value instanceof byte[]) {
						jsonObject.put(column, (byte[]) value);
					} else {
						throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
					}	
				}
				rslt.add(jsonObject);
			}
			rset.close();
			stmt.close();
		} catch (SQLException e) {
			rslt=null;
			logger.error(e);
		} 
		return rslt;
	}

	
	public void close() {
		try {
			repRSet.close();
			repStmt.close();
			repConn.close();
		} catch (Exception e) {
			logger.warn("TODO: closed already. " + e);
		}

	}

	public String getType(String srcDB, String type, String tgtDB) {
		String dataSpec=null;
		JSONArray ja=(JSONArray) jdbcDataTypeMap.get("MAP");
		for(Object jo: ja ){  
		    JSONObject rowJ = (JSONObject) jo;  //e.g ["PostgreSQL":["character(s)","character varying(s)"], ...]
		    //System.out.println(rowJ);
		    JSONArray srcTypes = (JSONArray) rowJ.get(srcDB.toUpperCase());
		    //System.out.println(srcTypes);
		    boolean t = srcTypes.contains(type);
		    System.out.println(t);

		    if(t) {  //simply grab the first match
		    	JSONArray itemJ = (JSONArray) rowJ.get(tgtDB.toUpperCase());
		    	System.out.println(itemJ);
		    	dataSpec = (String) itemJ.get(0);  //simply return the first one
		    	break;
		    }
		}
		return dataSpec;

	}
}