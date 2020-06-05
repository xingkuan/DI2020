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
	private ResultSet rRset;

	private int tableID;
	private String keyFeildType;

	private String sqlSelectSource;
	private String sqlInsertTarget;
	private String sqlDeleteTarget;
	
	private int fldCnt;

	private Timestamp tsLastAudit;
//	private Timestamp tsLastRef;

	private int poolID;
	private long startMS;
	private long endMS;

	private String srcTblAb7;

	private String lName, jName;

	private Timestamp tsThisRef;
	private long seqThisRef;

	private static final Logger ovLogger = LogManager.getLogger();

	private static final Metrix metrix = Metrix.getInstance();

	// encapsulate the details into tblDetailJSON;
	private JSONObject tblDetailJSON;
	private JSONObject actDetailJSON;
	private JSONObject dccDetailJSON;
	private JSONObject srcDBDetail;
	private JSONObject tgtDBDetail;
	private JSONObject dccDBDetail;
	private JSONObject miscValues=new JSONObject();
	
	private String avroSchema;
	
	//may not needed
	//private Map<Integer, Integer> fldType = new HashMap<>();
	//int[] fldType = new int[] {}; 
	//String[] fldNames = new String[] {}; 
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
			ovLogger.error("DB Driver error has occured");
			ovLogger.error(e);
		}

		try {
			repConn = DriverManager.getConnection(url, uID, uPW);
			repConn.setAutoCommit(false);
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		} catch (SQLException e) {
			ovLogger.error(e);
		}
	}

	public int setupTableAct(String jID, int tblID, int aId) {
		jobID = jID;
		tableID = tblID;
		actID = aId;
		
		tblDetailJSON=null;
		dccDetailJSON=null;
		srcDBDetail=null;
		tgtDBDetail=null;
		dccDBDetail=null;
		miscValues=null;

		lName=null;
		jName=null;
		
		if(initTableDetails() == -1 )
			return -1;
		// actDetailJSON is included in initTableDetails();
		String currState= tblDetailJSON.get("curr_state").toString();
		if ((currState.equals("")||currState.equals("0") ) &&
				(aId != 0)) {
			ovLogger.warn("This table is not enabled yet.");
			return -1;
		}
		if ((!currState.equals("2")) && (aId == 2)) {
			ovLogger.warn("This table is not enabled yet.");
			return -1;
		}
		
		initFieldMetaData();
		
		srcDBDetail = readDBDetails(tblDetailJSON.get("src_db_id").toString());
		tgtDBDetail = readDBDetails(tblDetailJSON.get("tgt_db_id").toString());
		//if dccData is not from src, eg. Kafka, initialize it here.
		return 0;
	}

	//should be removed!
	/*
	public void setupDCCJob(String jID, String srcDB, String l, String j, String tgtDB) {
		jobID = jID;
		lName=l;
		jName=j;
		
		tblDetailJSON=null;
		dccDetailJSON=null;
		
		srcDBDetail=null;
		dccDBDetail=null;  //not applicable for sync to dcc. The target is dcc, that is Kafka
		tgtDBDetail=null;
		//miscValues=null;

		//initFieldMetaData();
		srcDBDetail = readDBDetails(srcDB);
		tgtDBDetail = readDBDetails(tgtDB);
		initDCCDetails();
		tableID=Integer.valueOf(tblDetailJSON.get("tbl_id").toString());
	}
	private void initDCCDetails() {
		String sql="select tbl_id, src_db_id, tgt_db_id, src_schema, src_table, seq_last_ref, ts_last_ref, curr_state "
					+ " from meta_table " + " where src_db_id='" + srcDBDetail.get("db_id") + "' and src_schema='"
					+ lName + "' and src_table='" + jName + "' and tgt_schema='*'";
		tblDetailJSON = (JSONObject) SQLtoJSONArray(sql).get(0);
	}
*/

	// return db details as a simpleJSON object, (instead of a Java object, which is
	// too cumbersome).
	public JSONObject readDBDetails(String dbid) {
		String sql= "select db_id, db_cat, db_type, db_conn, db_driver, db_usr, db_pwd "
					+ " from meta_db " + " where db_id='" + dbid + "'";
		JSONObject jo = (JSONObject) SQLtoJSONArray(sql).get(0);
		return jo;
	}

	// return Table details as a simpleJSON object, (instead of a Java object, which
	// is too cumbersome).
	private int initTableDetails() {
		JSONArray jo;
		String sql = "select tbl_id, temp_id, tbl_pk, src_db_id, src_schema, src_table, tgt_db_id, tgt_schema, tgt_table, \n" + 
					"pool_id, init_dt, init_duration, curr_state, src_dcc_pgm, src_dcc_tbl, dcc_db_id, \n" + 
					"dcc_store, ts_regist, ts_last_ref, seq_last_ref "
							+ " from meta_table " + " where tbl_id=" + tableID;
		jo = SQLtoJSONArray(sql);
		if(jo.isEmpty()) {
			ovLogger.error("tableId does not exist.");
			return -1;
		}
		tblDetailJSON = (JSONObject) jo.get(0);
		
		Object dccDBIDObj = tblDetailJSON.get("dcc_db_id");
		if(!dccDBIDObj.toString().equals("")) {  //only sync via kafka has it.
			String journalName=tblDetailJSON.get("src_dcc_tbl").toString();
			String[] temp = journalName.split("\\.");
			lName=temp[0]; jName=temp[1];
			
		sql="select tbl_id, src_db_id, tgt_db_id, src_schema, src_table, seq_last_ref, ts_last_ref, curr_state "
				+ " from meta_table " + " where src_db_id='" + tblDetailJSON.get("src_db_id") + "' and src_schema='"
				+ lName + "' and src_table='" + jName + "' and tgt_schema='*'";
		jo = SQLtoJSONArray(sql);
		if(jo.isEmpty()) {
			ovLogger.error("no log journal.");
			return -1;
		}
		dccDetailJSON = (JSONObject) jo.get(0);

		}
		
		sql= "select info, stmts from meta_template where temp_id='" 
					+ tblDetailJSON.get("temp_id") + "' and act_id=" + actID;
		jo = SQLtoJSONArray(sql);
		if(jo.isEmpty()) {
			ovLogger.error("action not applicable.");
			return -1;
		}
		actDetailJSON = (JSONObject) jo.get(0);

		return 0;
	}

	private JSONArray SQLtoJSONArray(String sql) {
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
		} catch (SQLException e) {
			ovLogger.error(e);
		} finally {
			try {
			rset.close();
				stmt.close();
			} catch (SQLException e) {
				ovLogger.error(e);
			}
		}

		return jArray;
	}

	public JSONObject getTableDetails() {
		return tblDetailJSON;
	}
	public JSONObject getActDetails() {
		return actDetailJSON;
	}
	public JSONObject getSrcDBinfo() {
		return srcDBDetail;
	}
	public JSONObject getTgtDBinfo() {
		return tgtDBDetail;
	}
	public JSONObject getMiscValues() {
		return miscValues;
	}
	public boolean tblReadyForInit() {
		boolean rtv=true;
		
		String srcDBt=srcDBDetail.get("db_type").toString();
		if(dccDetailJSON != null) {
			try {
			if ( Timestamp.valueOf(dccDetailJSON.get("ts_last_ref").toString()).before(
				Timestamp.valueOf(tblDetailJSON.get("ts_regist").toString())) ) {
					rtv=false;
				}
			}catch(Exception e) {
				ovLogger.error(e);  //in case of null objects	
			}
		}	
		return rtv;
	}
	public int begin() {
			ovLogger.warn(actDetailJSON.get("info").toString());
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
		String sql = "update meta_table set curr_state = " + st 
				+ " where tbl_id = " + tableID;
		runUpdateSQL(sql);
	}
	public void saveInitStats() {
		//markEndTime();
		int duration = (int) (endMS - startMS) / 1000;
		ovLogger.info(jobID + " duration: " + duration + " sec");

		// report to InfluxDB:
		metrix.sendMX(
				"duration,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + duration + "\n");
		metrix.sendMX(
				"insCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalInsCnt + "\n");
		metrix.sendMX(
				"delCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalDelCnt + "\n");
		metrix.sendMX(
				"errCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalErrCnt + "\n");
		metrix.sendMX(
				//"JurnalSeq,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");
				"JurnalSeq,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + seqThisRef + "\n");

		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String sql = "update meta_table set init_dt = now() "
				+ ", init_duration = " + duration 
				//+ ", curr_state = " + currState
				//+ " seq_last_seq = " + miscValues.get("thisJournalSeq")
				+ ", seq_last_ref = " + seqThisRef
				+ " where tbl_id = " + tableID;
		runUpdateSQL(sql);
	}
	public void saveSyncStats() {
		//markEndTime();
		int duration = (int) (endMS - startMS) / 1000;
		ovLogger.info(jobID + " duration: " + duration + " sec");

		// report to InfluxDB:
		metrix.sendMX(
				"duration,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + duration + "\n");
		metrix.sendMX(
				"insCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalInsCnt + "\n");
		metrix.sendMX(
				"delCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalDelCnt + "\n");
		metrix.sendMX(
				"errCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalErrCnt + "\n");
		metrix.sendMX(
				//"JurnalSeq,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");
				"JurnalSeq,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + seqThisRef + "\n");

		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String sql = "update meta_table set"
				//+ " curr_sate = " + currState
				+ " ts_last_ref = now(),"
				//+ " seq_last_seq = " + miscValues.get("thisJournalSeq")
				+ " seq_last_ref = " + seqThisRef
				+ " where tbl_id = " + tableID;
		runUpdateSQL(sql);
	}
	public void saveTblInitStats() {
		int duration = (int) (endMS - startMS) / 1000;
		ovLogger.info(jobID + " duration: " + duration + " sec");

		// report to InfluxDB:
		metrix.sendMX(
				"duration,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + duration + "\n");
		metrix.sendMX(
				"insCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalInsCnt + "\n");
		metrix.sendMX(
				"delCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalDelCnt + "\n");
		metrix.sendMX(
				"errCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalErrCnt + "\n");
		metrix.sendMX(
				//"JurnalSeq,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");
		"JurnalSeq,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + seqThisRef + "\n");

		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		String sql = "update meta_table set init_dt = now()" 
				+ " init_duration = " + duration 
				+ " where tbl_id = " + tableID;
		runUpdateSQL(sql);
	}


	private boolean runUpdateSQL(String sql) {
		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt=null; 
		try {
			stmt = repConn.createStatement();
			int rslt = stmt.executeUpdate(sql);
		} catch (SQLException e) {
			ovLogger.error(e);
		} finally {
			try {
				stmt.close();
				repConn.commit();
			} catch (SQLException e) {
				ovLogger.error(e);
			}
		}
		
		return true;
	}

	public void saveAudit(int srcRC, int tgtRC) {
		java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		ovLogger.info(jobID + "source record count: " + srcRC + "     target record count: " + tgtRC);
//TODO: matrix
		try {
			rRset.updateTimestamp("TS_LAST_AUDIT", ts);
			rRset.updateInt("AUD_SOURCE_RECORD_CNT", srcRC);
			rRset.updateInt("AUD_TARGET_RECORD_CNT", tgtRC);
			rRset.updateRow();
			repConn.commit();
			// System.out.println("audit info saved");
		} catch (SQLException e) {
			ovLogger.error(jobID + e.getMessage());
		}
	}
	
//	private String sqlWhereClause;
	// creates select and insert strings
	private void initFieldMetaData() {
		Statement lrepStmt;
		ResultSet lrRset;
		int i;

		avroSchema = "{\"namespace\": \"com.future.DI2020.avro\", \n" 
				    + "\"type\": \"record\", \n" 
				    + "\"name\": \"" + tblDetailJSON.get("src_scheama")+"-"+ tblDetailJSON.get("src_table") + "\", \n" 
				    + "\"fields\": [ \n" ;
		
		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		i = 0;
		lrRset = lrepStmt.executeQuery(
			  "select src_field, src_field_type, tgt_field, java_type, avro_type from meta_table_field "
			+ " where tbl_id=" + tableID + " order by field_id");

		//first line
		if (lrRset.next()) {
			sqlSelectSource = "select " + lrRset.getString("src_field");
			sqlInsertTarget = "insert into " + tblDetailJSON.get("tgt_schema") + "." + tblDetailJSON.get("tgt_table")
				+ "(\""+ lrRset.getString("tgt_field") + "\""	;
			//fldType[i] = lrRset.getInt("java_type");
			//fldNames[i] = lrRset.getString("src_field");
			fldType.add(lrRset.getInt("java_type"));
			fldNames.add(lrRset.getString("src_field"));
			keyFeildType = lrRset.getString("src_field_type");  //TODO: not a safe way to assume the last one is the PK!!

			avroSchema = avroSchema 
					+ "{\"name\": \"" + lrRset.getString("src_field") + "\", \"type\": \"" + lrRset.getString("avro_type") + "\"} \n" ;
			i++;
		}
		//rest line
		while (lrRset.next()) {
			sqlSelectSource += ", " + lrRset.getString("src_field");
			sqlInsertTarget += ", " + "\"" + lrRset.getString("tgt_field") + "\"";
			//fldType[i] = lrRset.getInt("java_type");
			//fldNames[i] = lrRset.getString("src_field");
			fldType.add(lrRset.getInt("java_type"));
			fldNames.add(lrRset.getString("src_field"));

			avroSchema = avroSchema 
					+ ", {\"name\": \"" + lrRset.getString("src_field") + "\", \"type\": \"" + lrRset.getString("avro_type") + "\"} \n" ;

			i++;
			// System.out.println(i);
		}
		fldCnt=i;
		lrRset.close();
		lrepStmt.close();
		
		sqlSelectSource += " \n from " + tblDetailJSON.get("src_schema") + "." + tblDetailJSON.get("src_table") + " a";
		sqlInsertTarget += ") \n    Values ( ";
		for (i = 1; i <= fldCnt; i++) {
			if (i == 1) {
				sqlInsertTarget += "?";
			} else {
				sqlInsertTarget += ",?";
			}
		}
		sqlInsertTarget += ") ";
		sqlDeleteTarget = "delete " + tblDetailJSON.get("tgt_schema") + "." + tblDetailJSON.get("tgt_table") 
				+ " where " + tblDetailJSON.get("tbl_pk") + "=?";
		
		avroSchema = avroSchema 
				+ "] }";

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public String getGDTTDDL() {
	return"DECLARE GLOBAL TEMPORARY TABLE tmp"+tableID + "(" + tblDetailJSON.get("tbl_pk") + " " + keyFeildType + ") " 
	+" NOT LOGGED";
}
	public String getGDTTIns() {
	return "INSERT INTO tmp" + tableID + " VALUES (?)";
	}
//may not needed later on.
public ArrayList<Integer> getFldJavaType() {
	return fldType;
}
public ArrayList<String> getFldNames() {
	return fldNames;
}
	public String getSQLInsTgt() {
		return sqlInsertTarget;
	}
	public String getSQLSelSrc(boolean fast, boolean relaxed) {
		//The kind of SQLs are:
		// - all data in a table
		// - DCC from a log table
		// - DCC from a DB2 journal
		// ... and META_TABLE and META_SRC_TEMPLATE should have enough info to provide
		//     the right SQL.
		//Intention:
		// String strTemplate = "something from META_SRC_TEMPLATE '%(value)' in column # %(column)";
		// strTemplate = strTemplate.replace("%(value)", x); // 1
		// strTemplate = strTemplate.replace("%(column)", y); // 2
		if (tblDetailJSON.get("temp_id").toString().equals("DJ2K")) {
			return getSrcDCCSQL(fast, relaxed); //TODO: move this func database.
		}else {
			return sqlSelectSource;
		}
	}
	public String getSQLSelSrcViaGDTT() {
		return sqlSelectSource + " where " + tblDetailJSON.get("tbl_pk") 
			+ " (select " + tblDetailJSON.get("tbl_pk") + " from tmp"+tableID +")";
	}
	public String getSQLDelTgt() {
		return sqlDeleteTarget;
	}
//	public String getSQLWhereClause() {
//	return sqlWhereClause;
//}
	private String getSrcDCCSQL(boolean fast, boolean relaxed) {
		long lasDCCSeq = getDCCSeqLastRefresh();
		String extWhere="";
		
		if((lasDCCSeq == -1)||(seqThisRef <= lasDCCSeq))
			return null;   // this is the first time or no data, simply set META_AUX.SEQ_LAST_REF

		if (seqThisRef > lasDCCSeq ) {
			extWhere = " and SEQUENCE_NUMBER <=" + seqThisRef; 
		}
		
		String currStr;
		if(fast)
			currStr="";
		else
			currStr="*CURCHAIN";

		if(relaxed)
			return " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
					+ " FROM table (Display_Journal('" + tblDetailJSON.get("src_schema") + "', '" + tblDetailJSON.get("src_table") + "', " + "   '', '"
					+ currStr + "', " 
					+ "   cast(null as TIMESTAMP), " + "   cast(null as decimal(21,0)), "
					+ "   'R', " 
					+ "   ''," + "   '', '', '*QDDS', ''," 
					+ "   '', '', ''"
					+ ") ) as x where SEQUENCE_NUMBER > " + lasDCCSeq 
					+ extWhere 
					+ " order by 2 asc" ;// something weird with DB2 function: the starting SEQ
														// number seems not takining effect
		else
			return " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
					+ " FROM table (Display_Journal('" + tblDetailJSON.get("src_schema") + "', '" + tblDetailJSON.get("src_table") + "', " + "   '', '"
					+ currStr + "', "
					+ "   cast(null as TIMESTAMP), " // pass-in the start timestamp;
					+ "   cast(" + lasDCCSeq + " as decimal(21,0)), " // starting SEQ #
					+ "   'R', " // JOURNAL CODE: record operation
					+ "   ''," // JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
					+ "   '', '', '*QDDS', ''," // Object library, Object name, Object type, Object member
					+ "   '', '', ''" // User, Job, Program
					+ ") ) as x where SEQUENCE_NUMBER > " + lasDCCSeq 
					+ extWhere
					+ " order by 2 asc";
	}
public String getSrcDCCThisSeqSQL(boolean fast) {
	String currStr;
	if(fast)
		currStr="";
	else
		currStr="*CURCHAIN";
	//return " select max(SEQUENCE_NUMBER) " + " FROM table (Display_Journal('" + lName + "', '" + jName
	return " select max(SEQUENCE_NUMBER) " + " FROM table (Display_Journal('" + tblDetailJSON.get("src_schema") + "', '" + tblDetailJSON.get("src_table")
			+ "', '', '" + currStr + "', " // it looks like possible the journal can be switched and this SQL return no rows
			+ " cast(null as TIMESTAMP), " // pass-in the start timestamp;
			+ " cast(null as decimal(21,0)), " // starting SEQ #
			+ " 'R', " // JOURNAL cat: record operations
			+ " ''," // JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
			+ " '', '', '*QDDS', ''," + "   '', '', ''" // User, Job, Program
			+ ") ) as x ";
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
			ovLogger.info("... need to see why can't retrieve Journal Seq!!!");
		}
	}

	public String getJobID() {
		return jobID;
	}

//	public int getPrcTimeout() {
//		return prcTimeout;
//	}

	public Timestamp getLastAudit() {
		return tsLastAudit;
	}

//	public Timestamp getLastRefresh() {
//		return tsLastRef;
//	}

	public long getDCCSeqLastRefresh() {
		try {
			return Long.valueOf(tblDetailJSON.get("seq_last_ref").toString());
		}catch (NullPointerException e) {
			return -1;
		}
	}

	public int getPoolID() {
		return poolID;
	}


	public String getPK() {
		return tblDetailJSON.get("tbl_pk").toString();
	}

	public String getSQLSelectXForm() {
		return sqlSelectSource;
	}

	public int getCurrState() {
		return (int) tblDetailJSON.get("curr_state");
	}

	public int getTableID() {
		return tableID;
	}

//	public void setRefreshCnt(int i) {
//		refreshCnt = i;
//	}


	public void close() {
		try {
			rRset.close();
			repStmt.close();
			repConn.close();
		} catch (Exception e) {
			ovLogger.warn("TODO: closed already. " + e);
		}

	}

	public String getLabel() {
		return jobID;
	}

	//// from OVSdb.java
	public List<String> getDB2TablesOfJournal(String dbID, String journal) {
		List<String> tList = new ArrayList<String>();
		String strSQL;
		Statement lrepStmt = null;
		ResultSet lrRset=null;

		strSQL = "select src_schema||'.'||src_table from meta_table where src_db_id ='" + dbID
				+ "' and src_dcc_tbl='" + journal + "' and tgt_schema !='*' order by 1";

		// This shortterm solution is only for Oracle databases (as the source)
		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			lrRset = lrepStmt.executeQuery(strSQL);
			while (lrRset.next()) {
				// Retrieve by column name
				String tbl = lrRset.getString(1);
				tList.add(tbl);
			}
		} catch (SQLException se) {
			ovLogger.error("OJDBC driver error: " + se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			ovLogger.error(e);
		} finally {
			// make sure the resources are closed:
			try {
				lrRset.close();
				lrepStmt.close();
			} catch (SQLException se2) {
			}
		}

		return tList;
	}

	/*
	 * 07/24: return list of tbls belongs to a pool
	 */
	public List<Integer> getTblsByPoolID(int poolID) {
		Statement lrepStmt = null;
		ResultSet lrRset;
		List<Integer> tList = new ArrayList<Integer>();
		String strSQL;

		if (poolID < 0)
			strSQL = "select TABLE_ID,CURR_STATE from sync_table order by t_order";
		else
			strSQL = "select TABLE_ID,CURR_STATE from sync_table where pool_id = " + poolID + " order by t_order";

		// This shortterm solution is only for Oracle databases (as the source)
		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			lrRset = lrepStmt.executeQuery(strSQL);
			while (lrRset.next()) {
				// Retrieve by column name
				int id = lrRset.getInt("TABLE_ID");
				tList.add(id);
			}
		} catch (SQLException se) {
			ovLogger.error("OJDBC driver error has occured" + se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			ovLogger.error(e);
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

	public JSONArray getDCCsByPoolID(int poolID) {
		String sql = "select src_db_id, tgt_db_id, src_jurl_name from meta_table where pool_id = " + poolID;

		JSONArray jRslt = SQLtoJSONArray(sql);
		return jRslt;
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

		int tblID = 0;

		try {
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rRset = repStmt.executeQuery("select max(table_id) from vertsnap.sync_table ");

			rRset.next();
			tblID = rRset.getInt(1);
			rRset.close();
			repStmt.close();
			repConn.close();
		} catch (SQLException e) {
			ovLogger.error(e.getMessage());
		}

		return tblID + 1;
	}

	public boolean isNewTblID(int tblID) {
		Statement lrepStmt = null;
		ResultSet lrRset;
		String strSQL;
		boolean rslt = true;

		strSQL = "select TBL_ID from meta_table where tbl_id = " + tblID;

		try {
			lrepStmt = repConn.createStatement();
			lrRset = repStmt.executeQuery(strSQL);
			if (lrRset.next()) {
				rslt = false;
			}
		} catch (SQLException se) {
			ovLogger.error(se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			ovLogger.error(e);
		} finally {
			// make sure the resources are closed:
			try {
				if (lrepStmt != null)
					lrepStmt.close();
			} catch (SQLException se2) {
			}
		}

		return rslt;
	}
/*
	public boolean isDCCJob() {
		if (tblDetailJSON.get("tgt_schema").toString().equals("*"))
			return true;
		else
			return false;
	}
*/
   public String getAvroSchema(){
	   return avroSchema;
	   }

}