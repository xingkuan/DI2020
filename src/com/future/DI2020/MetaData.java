package com.future.DI2020;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;

import java.text.*;
import java.sql.*;
import java.sql.Date;

/*
 class Meta
 
 Meta, singleton class of replication metat data, stored in RDBMS
*/

class MetaData {

	private String jobID;
	private int actID;
	Timestamp tsThisRefesh;

	private Connection repConn;
	private Statement repStmt;
	private ResultSet repRSet;

	private int taskID;
	private String keyDataType;

	private int fldCnt;

	private Timestamp tsLastAudit;

	private int poolID;
	private long startMS;
	private long endMS;

	private String srcTblAb7;

	private String lName, jName;

	private Timestamp tsThisRef;
	private long seqThisRef;

	private static final Logger logger = LogManager.getLogger();

	private static final Metrix metrix = Metrix.getInstance();

	// encapsulate the details into tskDetailJSON;
	private JSONObject tskDetailJSON;
	private JSONObject tmpDetailJSON;
	private JSONObject dccDetailJSON;
	private JSONObject srcDBDetail;
	private JSONObject tgtDBDetail;
	private JSONObject dccDBDetail;
	private JSONObject miscValues=new JSONObject();
	
	private String avroSchema;
	
	ArrayList<Integer> fldType = new ArrayList<Integer>();
	ArrayList<String> fldNames = new ArrayList<String>();
	
	int totalDelCnt, totalInsCnt, totalErrCnt, totalMsgCnt;
	
	private static MetaData instance = null; // use lazy instantiation ;

	public static MetaData getInstance() {
		if (instance == null) {
			instance = new MetaData();
		}
		return instance;
	}

	public MetaData() {
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

	public void setJobName(String jobName) {
		jobID =jobName;
	}
	public int setupTaskForAction(String jID, int tskID, int aId) {
		int rtc;
		
		jobID = jID;
		taskID = tskID;
		actID = aId;
		
		tskDetailJSON=null;
		dccDetailJSON=null;
		srcDBDetail=null;
		tgtDBDetail=null;
		dccDBDetail=null;
		miscValues=null;

		lName=null;
		jName=null;
		
		if(initTaskDetails() == -1 ) {  // tmpDetailJSON is included in initTableDetails();
			rtc = -1;
			return rtc;
		}
		//verify table level status; if ok, finish the setup for action.
		String currState= tskDetailJSON.get("curr_state").toString();
		switch(aId) {
		case 0:
			if (!(currState.equals("")||currState.equals("0") )) {
				logger.warn("This task is already enabled.");
				rtc = -1;
			}
			rtc = 0;
			break;
		case 1:
		case 2:
			if (!currState.equals("2")) {
				logger.warn("This task is not in sync state.");
				return -1;
			}
			break;
		case 21:  //testing code
			break;
		case -1:
			logger.info("To unregister task " + taskID + ": "
					+ tskDetailJSON.get("src_schema")+"."+tskDetailJSON.get("src_table"));
			break;
		default:
			logger.error("unsupported action or just for dev/test purpose.");	
		}
		
		initFieldMetaData();
		
		return 0;
	}

	// return db details as a simpleJSON object, (instead of a cumbersome POJO).
	public JSONObject readDBDetails(String dbid) {
		String sql= "select db_id, db_cat, db_type, db_conn, db_driver, db_usr, db_pwd "
					+ " from DATA_POINT " + " where db_id='" + dbid + "'";
		JSONObject jo = (JSONObject) SQLtoJSONArray(sql).get(0);
		return jo;
	}
	
	private int initTaskDetails() {
		JSONArray jo;
		String sql = "select task_id, template_id, data_pk, src_db_id, src_schema, src_table, tgt_db_id, tgt_schema, tgt_table, \n" + 
					"pool_id, init_dt, init_duration, curr_state, src_dcc_pgm, src_dcc_tbl, dcc_db_id, \n" + 
					"dcc_store, ts_regist, ts_last_ref, seq_last_ref, db_type,src_stmt0, tgt_stmt0 "
					+ " from task a, DATA_POINT b " + " where a.src_db_id=b.db_id and task_id=" + taskID;
		jo = SQLtoJSONArray(sql);
		if(jo.isEmpty()) {
			logger.error("task does not exist.");
			return -1;
		}
		tskDetailJSON = (JSONObject) jo.get(0);

		if((actID==-1)||(actID==21)) {  //no further setup if it is unregistering or testing.
			return 0;
		}else {
			sql= "select template_id, act_id, info, stmts from TASK_TEMPLATE where template_id='" 
						+ tskDetailJSON.get("template_id") + "' and act_id=" + actID;
			jo = SQLtoJSONArray(sql);
			if(jo.isEmpty()) {
				logger.error("action not applicable.");
				return -1;
			}
			tmpDetailJSON = (JSONObject) jo.get(0);
		
			String templateId=tskDetailJSON.get("template_id").toString();
			if(templateId.equals("DATA_")) {  
				String journalName=tskDetailJSON.get("src_dcc_tbl").toString();
				String[] temp = journalName.split("\\.");
				lName=temp[0]; jName=temp[1];
				
				sql="select task_id, src_db_id, tgt_db_id, src_schema, src_table, seq_last_ref, ts_last_ref, curr_state "
						+ " from task " 
						+ " where src_db_id='" + tskDetailJSON.get("src_db_id") + "' and src_schema='"
						+ lName + "' and src_table='" + jName + "' and tgt_schema='*'";
				jo = SQLtoJSONArray(sql);
				if(jo.isEmpty()) {
					logger.error("error in DCC, e. g. DB2/AS400 journal");
					return -1;
				}
				dccDetailJSON = (JSONObject) jo.get(0);
			}
		}
		return 0;
	}

//	public JSONArray getDCCsByPoolID(int poolID) {
//	String sql = "select src_db_id, tgt_db_id, src_jurl_name from task where pool_id = " + poolID;
//
//	JSONArray jRslt = SQLtoJSONArray(sql);
//	return jRslt;
//}
	public JSONArray SQLtoJSONArray(String sql) {
		JSONArray jArray = new JSONArray();
		JSONObject jsonObject = null;

		JSONObject jo = new JSONObject();
		Statement stmt = null;
		ResultSet rset = null;
		String column;
		Object value;
		try {
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
				jArray.add(jsonObject);
			}
			rset.close();
			stmt.close();
		} catch (SQLException e) {
			logger.error(e);
		} 
		return jArray;
	}

	public JSONObject getTaskDetails() {
		return tskDetailJSON;
	}
	public JSONObject getActDetails() {
		return tmpDetailJSON;
	}
	public JSONObject getMiscValues() {
		return miscValues;
	}
	public String getKeyDataType() {
		return keyDataType;
	}
	
	public boolean taskHasDependency(String dbID, String srcSch, String srcTbl) {
		boolean rtc=true;
		String sql="select 1 from task "
				+ "where src_db_id='"+dbID+"' "
				+ " and src_dcc_tbl='"+srcSch+"."+srcTbl + "'";
		JSONArray jo = SQLtoJSONArray(sql);
		if((jo==null)||jo.isEmpty()) {
			rtc=false;
		}
		return rtc;
	}
	public int begin() {
			logger.warn("    Action: " + tmpDetailJSON.get("info").toString());
			Calendar cal = Calendar.getInstance();
			startMS = cal.getTimeInMillis();
			updateCurrState(1);  //indicating table is being worked on
			return 0;
	}

	public void end(int state) {
		Calendar cal = Calendar.getInstance();
		endMS = cal.getTimeInMillis();
		
		updateCurrState(state);
	}

	private void updateCurrState(int st) {
		String sql = "update task set curr_state = " + st 
				+ " where task_id = " + taskID;
		runUpdateSQL(sql);
	}
	public void saveInitStats() {
		//markEndTime();
		int duration = (int) (endMS - startMS) / 1000;
		logger.info("    " + " duration: " + duration + " sec");

		// report to InfluxDB:
		metrix.sendMX(
				"duration,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + duration + "\n");
		metrix.sendMX(
				"insCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalInsCnt + "\n");
		metrix.sendMX(
				"delCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalDelCnt + "\n");
		metrix.sendMX(
				"errCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalErrCnt + "\n");
		metrix.sendMX(
				//"JurnalSeq,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");
				"JurnalSeq,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + seqThisRef + "\n");

		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String sql = "update task set init_dt = now() "
				+ ", init_duration = " + duration 
				//+ ", curr_state = " + currState
				//+ " seq_last_seq = " + miscValues.get("thisJournalSeq")
				+ ", seq_last_ref = " + seqThisRef
				+ " where task_id = " + taskID;
		runUpdateSQL(sql);
	}
	public void saveSyncStats() {
		//markEndTime();
		int duration = (int) (endMS - startMS) / 1000;
		logger.info(jobID + " duration: " + duration + " sec");

		// report to InfluxDB:
		metrix.sendMX(
				"duration,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + duration + "\n");
		metrix.sendMX(
				"insCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalInsCnt + "\n");
		metrix.sendMX(
				"delCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalDelCnt + "\n");
		metrix.sendMX(
				"errCnt,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + totalErrCnt + "\n");
		metrix.sendMX(
				//"JurnalSeq,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");
				"JurnalSeq,jobId=" + jobID + ",taskID=" + taskID + "~" + srcTblAb7 + " value=" + seqThisRef + "\n");

		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String sql = "update task set"
				//+ " curr_sate = " + currState
				+ " ts_last_ref = now(),"
				//+ " seq_last_seq = " + miscValues.get("thisJournalSeq")
				+ " seq_last_ref = " + seqThisRef
				+ " where task_id = " + taskID;
		runUpdateSQL(sql);
	}
	private boolean runUpdateSQL(String sql) {
		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt=null; 
		try {
			stmt = repConn.createStatement();
			int rslt = stmt.executeUpdate(sql);
			stmt.close();
			repConn.commit();
		} catch (SQLException e) {
			logger.error(e);
		} 
		return true;
	}

	// TODO: move most code into DB as as part of registering table.
	private void initFieldMetaData() {
		Statement lrepStmt;
		ResultSet lrRset;
		int i;

		avroSchema = "{\"namespace\": \"com.future.DI2020.avro\", \n" 
				    + "\"type\": \"record\", \n" 
				    + "\"name\": \"" + tskDetailJSON.get("src_schema")+"."+ tskDetailJSON.get("src_table") + "\", \n" 
				    + "\"fields\": [ \n" ;
		
		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		i = 0;
		lrRset = lrepStmt.executeQuery(
			  "select src_field, src_field_type, tgt_field, java_type, avro_type from data_field "
			+ " where task_id=" + taskID + " order by field_id");

		//first line
		if (lrRset.next()) {
			//fldType[i] = lrRset.getInt("java_type");
			//fldNames[i] = lrRset.getString("src_field");
			fldType.add(lrRset.getInt("java_type"));
			fldNames.add(lrRset.getString("src_field"));

			avroSchema = avroSchema 
					+ "{\"name\": \"" + lrRset.getString("tgt_field") + "\", " + lrRset.getString("avro_type") + "} \n" ;
			i++;
		}
		//rest line (but not the last)
		while (lrRset.next() ) {   
			//if( lrRset.isLast()) {                                               //In DB2AS400, a.rrn(a) as DB2RRN is wrong syntaxly;
			//	if(tskDetailJSON.get("db_type").toString().contains("DB2/AS400")){  // but "a." is needed for Oracle.
			//	avroSchema = avroSchema 
			//			+ ", {\"name\": \"DB2RRN\", \"type\": " + lrRset.getString("avro_type") + "} \n" ;
			//	}if(tskDetailJSON.get("db_type").toString().contains("ORACLE")){
			//		avroSchema = avroSchema 
			//				+ ", {\"name\": \"ORARID\", \"type\": " + lrRset.getString("avro_type") + "} \n" ;
			//	}
			//	keyDataType = lrRset.getString("src_field_type");  //TODO: not a safe way to assume the last one is the PK!!
			//}else {
				keyDataType = lrRset.getString("src_field_type");  //TODO: not a safe way to assume the last one is the PK!!
				avroSchema = avroSchema 
						+ ", {\"name\": \"" + lrRset.getString("tgt_field") + "\", " + lrRset.getString("avro_type") + "} \n" ;
			//}
			fldType.add(lrRset.getInt("java_type"));
			fldNames.add(lrRset.getString("src_field"));
			i++;
			// System.out.println(i);
		}

		fldCnt=i;
		lrRset.close();
		lrepStmt.close();
		
		avroSchema = avroSchema 
				+ "] }";

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//may not needed later on.
	public ArrayList<Integer> getFldJavaType() {
		return fldType;
	}
	public ArrayList<String> getFldNames() {
		return fldNames;
	}
	public String getSQLInsTgt() {
		//return sqlInsertTarget;
		return tskDetailJSON.get("tgt_stmt0").toString();
	}
	private JSONObject getAct1SQLs() {
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		pre.add(1, getBareSrcSQL() );
		jo.put("PRE", pre);
		
		return jo;
	}
	private JSONObject getD2V_act2SQLs(boolean fast, boolean relaxed) { 
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		//1. get keys from Kafka
		//2. either --- skip it (too complicated). Keep only the following b1 and b2.
		//     a. compose where clause and add to sqlSelectSource
		//  or b1. declare temp. tbl and batch the keys
		String sql ="DECLARE GLOBAL TEMPORARY TABLE qtemp.DCC"+taskID + "(" + tskDetailJSON.get("data_pk") + " " + keyDataType + ") " 
				+" NOT LOGGED"; 
		pre.add(sql);
		pre.add("INSERT INTO qtemp.DCC" + taskID + " VALUES (?)" );
		sql = getBareSrcSQL() + ", qtemp.DCC"+taskID + " b "
				+ " where a..rrn(a)=b." +tskDetailJSON.get("data_pk");  //TOTO: may have problem!
		pre.add(sql)
;
		jo.put("PRE", pre);

		return jo;
	}
	
	public void setRefreshTS(Timestamp thisRefreshHostTS) {
		tsThisRef = thisRefreshHostTS;
	}
	public void setRefreshSeqThis(long seq) {
		if (seq > 0) {  
			seqThisRef = seq;
		} else {   //should never happen. no?
			//seqThisRef = (long) miscValues.get("thisJournalSeq");
			seqThisRef = Long.valueOf( (dccDetailJSON.get("seq_last_ref").toString()));
			logger.info("... need to see why can't retrieve Journal Seq!!!");
		}
	}

	public String getJobID() {
		return jobID;
	}

	public Timestamp getLastAudit() {
		return tsLastAudit;
	}

	public long getDCCSeqLastRefresh() {
		try {
			return Long.valueOf(tskDetailJSON.get("seq_last_ref").toString());
		}catch (NullPointerException e) {
			return -1;
		}
	}

	public int getPoolID() {
		return poolID;
	}


	public String getPK() {
		return tskDetailJSON.get("data_pk").toString();
	}

	public String getBareSrcSQL() {
		//return sqlSelectSource;
		return tskDetailJSON.get("src_stmt0").toString();
	}

	public int getCurrState() {
		return (int) tskDetailJSON.get("curr_state");
	}

	public int getTableID() {
		return taskID;
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

	public String getLabel() {
		return jobID;
	}

	public List<Integer> getTblsByPoolID(int poolID) {
		Statement lrepStmt = null;
		ResultSet lrRset;
		List<Integer> tList = new ArrayList<Integer>();
		String strSQL;

		if (poolID < 0)
			strSQL = "select task_id,curr_state from task order by 1";
		else
			strSQL = "select task_id,curr_state from task where pool_id = " + poolID + " order by 1";

		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			lrRset = lrepStmt.executeQuery(strSQL);
			while (lrRset.next()) {
				// Retrieve by column name
				int id = lrRset.getInt(1);
				tList.add(id);
			}
		} catch (SQLException se) {
			logger.error("OJDBC driver error has occured" + se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			logger.error(e);
		} finally {
			// make sure the resources are closed:
			try {
				if (lrepStmt != null)
					lrepStmt.close();
			} catch (SQLException se2) {
			}
		}

		return tList;
	}

	public List<Integer> getTableIDsAll() {
		return getTblsByPoolID(-1);
	}

// ... move to MetaData ?
	public void setThisRefreshHostTS() {
		tsThisRefesh = new Timestamp(System.currentTimeMillis());
	}

	public void setTotalMsgCnt(int v) {
		totalMsgCnt = v;
	}
	public void setTotalDelCnt(int v) {
		totalDelCnt = v;
	}

	public void setTotalInsCnt(int v) {
		totalInsCnt = v;
	}

	public void setTotalErrCnt(int v) {
		totalErrCnt = v;
	}

	public String getDCCPoolID() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getNextTblID() {
		Statement repStmt;
		ResultSet rRset;

		int taskID = 0;

		try {
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rRset = repStmt.executeQuery("select max(task_id) from task ");

			rRset.next();
			taskID = rRset.getInt(1);
			rRset.close();
			repStmt.close();
		} catch (SQLException e) {
			logger.error(e);
		}

		return taskID + 1;
	}

	public String getAvroSchema(){
		return avroSchema;
	}
	/**** Registration APIs ****/
	public boolean preRegistCheck(int taskID, String srcDBid, String srcSch, String srcTbl, String dccDBid) {
		String sql;
		JSONArray rslt;
		
		sql = "select task_id from task where task_id = " + taskID;
		rslt = (JSONArray) SQLtoJSONArray(sql);
		if(rslt.size()>0) {
			logger.error("task ID is already used!");
			return false;
		}
		sql = "select task_id from task where SRC_DB_ID='" + srcDBid + "' and SRC_SCHEMA='"
				+ srcSch + "' and SRC_TABLE='" + srcTbl + "';";
		rslt = (JSONArray) SQLtoJSONArray(sql);
		if(rslt.size()>0) {
			logger.error("Task is already registered!");
			return false;
		}

		if(!dccDBid.equals("na")){ //That means an aux tbl with table ID=taskID+1 need to be created. 
			sql = "select task_id from task where task_id = " + taskID+1;
			rslt = (JSONArray) SQLtoJSONArray(sql);
			if(rslt.size()>0) {
				logger.warn("!!! aux task id " + (taskID+1) + "exist already!");
				return true;
			}
		}
		return true;
	}
	public void runRegSQL(String sql) {
		runUpdateSQL(sql);
	}
	/*************************************/
}