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

	private int tableID;
	private String keyDataType;

	//private String sqlSelectSource;
	//private String sqlInsertTarget;
	//private String sqlDeleteTarget;
	
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

	private static final Logger logger = LogManager.getLogger();

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

	public int setupTableForAction(String jID, int tblID, int aId) {
		int rtc;
		
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
		
		if(initTableDetails() == -1 ) {  // actDetailJSON is included in initTableDetails();
			rtc = -1;
			return rtc;
		}
		//verify table level status; if ok, finish the setup for action.
		String currState= tblDetailJSON.get("curr_state").toString();
		switch(aId) {
		case 0:
			if (!(currState.equals("")||currState.equals("0") )) {
				logger.warn("This table is already enabled.");
				rtc = -1;
			}
			rtc = 0;
			break;
		case 1:
		case 2:
			if (!currState.equals("2")) {
				logger.warn("This table is not in sync state.");
				return -1;
			}
			break;
		case 21:  //testing code
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
	
	private int initTableDetails() {
		JSONArray jo;
		String sql = "select tbl_id, temp_id, tbl_pk, src_db_id, src_schema, src_table, tgt_db_id, tgt_schema, tgt_table, \n" + 
					"pool_id, init_dt, init_duration, curr_state, src_dcc_pgm, src_dcc_tbl, dcc_db_id, \n" + 
					"dcc_store, ts_regist, ts_last_ref, seq_last_ref, db_type,src_stmt0, tgt_stmt0 "
					+ " from SYNC_TABLE a, DATA_POINT b " + " where a.src_db_id=b.db_id and tbl_id=" + tableID;
		jo = SQLtoJSONArray(sql);
		if(jo.isEmpty()) {
			logger.error("tableId does not exist.");
			return -1;
		}
		tblDetailJSON = (JSONObject) jo.get(0);
		//If DCC data src is involved:
		Object dccDBIDObj = tblDetailJSON.get("dcc_db_id");
		if((!dccDBIDObj.toString().equals("")) && (!dccDBIDObj.toString().equals("na"))
				) {  //only sync via kafka has it.
			String journalName=tblDetailJSON.get("src_dcc_tbl").toString();
			String[] temp = journalName.split("\\.");
			lName=temp[0]; jName=temp[1];
			
			sql="select tbl_id, src_db_id, tgt_db_id, src_schema, src_table, seq_last_ref, ts_last_ref, curr_state "
					+ " from SYNC_TABLE " + " where src_db_id='" + tblDetailJSON.get("src_db_id") + "' and src_schema='"
					+ lName + "' and src_table='" + jName + "' and tgt_schema='*'";
			jo = SQLtoJSONArray(sql);
			if(jo.isEmpty()) {
				logger.error("error in DCC, e. g. DB2/AS400 journal");
				return -1;
			}
			dccDetailJSON = (JSONObject) jo.get(0);
		}
		
		sql= "select temp_id, act_id, info, stmts from SYNC_TEMPLATE where temp_id='" 
					+ tblDetailJSON.get("temp_id") + "' and act_id=" + actID;
		jo = SQLtoJSONArray(sql);
		if(jo.isEmpty()) {
			logger.error("action not applicable.");
			return -1;
		}
		actDetailJSON = (JSONObject) jo.get(0);

		return 0;
	}

//	public JSONArray getDCCsByPoolID(int poolID) {
//	String sql = "select src_db_id, tgt_db_id, src_jurl_name from SYNC_TABLE where pool_id = " + poolID;
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

	public JSONObject getTableDetails() {
		return tblDetailJSON;
	}
	public JSONObject getActDetails() {
		return actDetailJSON;
	}
	public JSONObject getMiscValues() {
		return miscValues;
	}
	public String getKeyDataType() {
		return keyDataType;
	}
/*	public boolean tblReadyForInit() {
		boolean rtv=true;
		
		String srcDBt=srcDBDetail.get("db_type").toString();
		if(dccDetailJSON != null) {
			try {
			if ( Timestamp.valueOf(dccDetailJSON.get("ts_last_ref").toString()).before(
				Timestamp.valueOf(tblDetailJSON.get("ts_regist").toString())) ) {
					rtv=false;
				}
			}catch(Exception e) {
				logger.error(e);  //in case of null objects	
			}
		}	
		return rtv;
	}*/
	public int begin() {
			logger.warn(actDetailJSON.get("info").toString());
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
		String sql = "update SYNC_TABLE set curr_state = " + st 
				+ " where tbl_id = " + tableID;
		runUpdateSQL(sql);
	}
	public void saveInitStats() {
		//markEndTime();
		int duration = (int) (endMS - startMS) / 1000;
		logger.info(jobID + " duration: " + duration + " sec");

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
		String sql = "update SYNC_TABLE set init_dt = now() "
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
		logger.info(jobID + " duration: " + duration + " sec");

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
		String sql = "update SYNC_TABLE set"
				//+ " curr_sate = " + currState
				+ " ts_last_ref = now(),"
				//+ " seq_last_seq = " + miscValues.get("thisJournalSeq")
				+ " seq_last_ref = " + seqThisRef
				+ " where tbl_id = " + tableID;
		runUpdateSQL(sql);
	}
	public void saveTblInitStats() {
		int duration = (int) (endMS - startMS) / 1000;
		logger.info(jobID + " duration: " + duration + " sec");

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
		String sql = "update SYNC_TABLE set init_dt = now()" 
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
				    + "\"name\": \"" + tblDetailJSON.get("src_schema")+"."+ tblDetailJSON.get("src_table") + "\", \n" 
				    + "\"fields\": [ \n" ;
		
		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		i = 0;
		lrRset = lrepStmt.executeQuery(
			  "select src_field, src_field_type, tgt_field, java_type, avro_type from SYNC_TABLE_FIELD "
			+ " where tbl_id=" + tableID + " order by field_id");

		//first line
		if (lrRset.next()) {
			//fldType[i] = lrRset.getInt("java_type");
			//fldNames[i] = lrRset.getString("src_field");
			fldType.add(lrRset.getInt("java_type"));
			fldNames.add(lrRset.getString("src_field"));

			avroSchema = avroSchema 
					+ "{\"name\": \"" + lrRset.getString("src_field") + "\", \"type\": " + lrRset.getString("avro_type") + "} \n" ;
			i++;
		}
		//rest line (but not the last)
		while (lrRset.next() ) {   
			if( lrRset.isLast()) {                                               //In DB2AS400, a.rrn(a) as DB2RRN is wrong syntaxly;
				if(tblDetailJSON.get("db_type").toString().contains("DB2/AS400")){  // but "a." is needed for Oracle.
				avroSchema = avroSchema 
						+ ", {\"name\": \"DB2RRN\", \"type\": " + lrRset.getString("avro_type") + "} \n" ;
				}if(tblDetailJSON.get("db_type").toString().contains("ORACLE")){
					avroSchema = avroSchema 
							+ ", {\"name\": \"ORARID\", \"type\": " + lrRset.getString("avro_type") + "} \n" ;
				}
				keyDataType = lrRset.getString("src_field_type");  //TODO: not a safe way to assume the last one is the PK!!
			}else {
				keyDataType = lrRset.getString("src_field_type");  //TODO: not a safe way to assume the last one is the PK!!
				avroSchema = avroSchema 
						+ ", {\"name\": \"" + lrRset.getString("src_field") + "\", \"type\": " + lrRset.getString("avro_type") + "} \n" ;
			}
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
		return tblDetailJSON.get("tgt_stmt0").toString();
	}
	//Need to return the right list of SQLs in order to create the needed Resultset:
	/* - for ACT_ID=1: 
	 * 		Alway return the sqlSelectSource, generated in initFieldMetaData() 
	 * - for ACT_ID=2:
	 * 		For Oracle using trig and log table, (TEMP_ID O2V):
	 * 			1. update all the log table records
	 * 			2. add join + where clause to "sqlSelectSource" and return;
	 * 		  and a statement for cleanup(): delete the processed recoded from the log table;
	 * - for ACT_ID=2:
	 * 		For DV2/AS400 journal RRNs to Kafka, (TEMP_ID DJ2K):
	 * 			return the DISPLAY_JOURNAL sql. 
	 * 				( need to options to return different versions, for optimal performance reason)
	 * 		For DV2/AS400 via Kafka, (TEMP_ID D2V_):
	 *      - if count of keys > threshold:
	 * 			1. declare a temp_table
	 * 			2. batch insert the keys into the temp table
	 * 			3. add the temp_table + to the "sqlSelectSource" and return;
	 * 		  and a statement for cleanup(): drop the temp_table;
	 *      - if count of keys < threshold:
	 * 			1. add the keys to where clause to the "sqlSelectSource" and return;
	 */
//	public JSONObject getSrcSQLs(int actId, boolean fast, boolean relaxed) {
//		/*
//		 * The design intention: to be template driving.
//		 *   E. g. 
//		 *   String strTemplate = "something from META_SRC_TEMPLATE '%(value)' in column # %(column)";
//		 *   strTemplate = strTemplate.replace("%(value)", x); // 1
//		 *   strTemplate = strTemplate.replace("%(column)", y); // 2
//		 */
//		if(actId==1) {
//			return getAct1SQLs();
//		}else if (actId==2) {	
//			String tempID=tblDetailJSON.get("temp_id").toString();
//			switch(tempID) {   //TODO: move this func database.
//			case "DJ2K":
//				return getDJ2Kact2SQLs(fast, relaxed); 
//				//break;
//			case "D2V_":
//				return getD2V_act2SQLs(fast, relaxed); 
//			case "O2V":
//			case "O2K":
//				return getO2Vact2SQLs(); 
//			default:
//				logger.error("Invalid template.");
//				return null;
//		}
//		}else {
//			logger.error("Invalid action.");
//			return null;
//		}
//	}
	private JSONObject getAct1SQLs() {
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		pre.add(1, getBareSrcSQL() );
		jo.put("PRE", pre);
		
		return jo;
	}
	//moved to Oracle as getSrcSqlStmts(String temp)
/*	private JSONObject getO2Vact2SQLs() {
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		pre.add("update " + tblDetailJSON.get("src_dcc_tbl") + " set dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" );
		pre.add(getBareSrcSQL() + ", " + tblDetailJSON.get("src_dcc_tbl") 
				+ " b where b.dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') "
				+ " and a.rowid=b."+tblDetailJSON.get("tbl_pk"));
		jo.put("PRE", pre);
		JSONArray aft = new JSONArray();
		aft.add("delete from " + tblDetailJSON.get("src_dcc_tbl") + " where dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" );
		jo.put("AFT", aft);
		
		return jo;
	}
	*/
	private JSONObject getD2V_act2SQLs(boolean fast, boolean relaxed) { 
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		//1. get keys from Kafka
		//2. either --- skip it (too complicated). Keep only the following b1 and b2.
		//     a. compose where clause and add to sqlSelectSource
		//  or b1. declare temp. tbl and batch the keys
		String sql ="DECLARE GLOBAL TEMPORARY TABLE qtemp.DCC"+tableID + "(" + tblDetailJSON.get("tbl_pk") + " " + keyDataType + ") " 
				+" NOT LOGGED"; 
		pre.add(sql);
		pre.add("INSERT INTO qtemp.DCC" + tableID + " VALUES (?)" );
		sql = getBareSrcSQL() + ", qtemp.DCC"+tableID + " b "
				+ " where a..rrn(a)=b." +tblDetailJSON.get("tbl_pk");  //TOTO: may have problem!
		pre.add(sql)
;
		jo.put("PRE", pre);

		return jo;
	}
	/*
	public JSONObject getDJ2Kact2SQLs(boolean fast, boolean relaxed) {  //"public" as an hacker
	//TODO: pushed to DB2Data.byodccSQL(xxxx)
		JSONObject jo = new JSONObject();
		long lasDCCSeq = getDCCSeqLastRefresh();
		String extWhere="";
		
		if((lasDCCSeq == -1)||(seqThisRef <= lasDCCSeq))   
			return null;   // this is the first time or no data, simply set META_AUX.SEQ_LAST_REF

		if (seqThisRef > lasDCCSeq ) {
			extWhere = " and SEQUENCE_NUMBER <=" + seqThisRef; 
		}
		
		String sql;
		String currStr;
		if(fast)
			currStr="";
		else
			currStr="*CURCHAIN";

		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		if(relaxed) {
			sql= " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
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
			pre.add(1, sql );
			jo.put("PRE", pre);

			return jo;
		}else
			sql = " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
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
		pre.add(sql );
		jo.put("PRE", pre);

		return jo;
		
	}
	*/
//	public String getSQLDelTgt() {
//		return sqlDeleteTarget;
//	}

	
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

	public String getBareSrcSQL() {
		//return sqlSelectSource;
		return tblDetailJSON.get("src_stmt0").toString();
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

	//// from OVSdb.java
	public List<String> getDB2TablesOfJournal(String dbID, String journal) {
		List<String> tList = new ArrayList<String>();
		String strSQL;
		Statement lrepStmt = null;
		ResultSet lrRset=null;

		strSQL = "select src_schema||'.'||src_table from SYNC_TABLE where src_db_id ='" + dbID
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
			logger.error("OJDBC driver error: " + se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			logger.error(e);
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

		int tblID = 0;

		try {
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rRset = repStmt.executeQuery("select max(tbl_id) from SYNC_TABLE ");

			rRset.next();
			tblID = rRset.getInt(1);
			rRset.close();
			repStmt.close();
		} catch (SQLException e) {
			logger.error(e);
		}

		return tblID + 1;
	}

	public String getAvroSchema(){
		return avroSchema;
	}
	/**** Registration APIs ****/
	public boolean preRegistCheck(int tblID, String srcDBid, String srcSch, String srcTbl) {
		String sql;
		JSONArray rslt;
		
		sql = "select TBL_ID from SYNC_TABLE where tbl_id = " + tblID;
		rslt = (JSONArray) SQLtoJSONArray(sql);
		if(rslt.size()>0) {
			logger.error("Table ID is already used!");
			return false;
		}
		sql = "select 'exit already!!!', tbl_id from SYNC_TABLE where SRC_DB_ID=" + srcDBid + " and SRC_SCHEMA='"
				+ srcSch + "' and SRC_TABLE='" + srcTbl + "';";
		rslt = (JSONArray) SQLtoJSONArray(sql);
		if(rslt.size()>0) {
			logger.error("Table is already registered!");
			return false;
		}

		return true;
	}
	public void runRegSQL(String sql) {
		runUpdateSQL(sql);
	}
	/*************************************/
}