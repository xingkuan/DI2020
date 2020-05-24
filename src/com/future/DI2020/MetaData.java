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
	Timestamp tsThisRefesh;

	private Connection repConn;
	private Statement repStmt;

	// for now, auxilary is Kafka when replicating from DB2 to Vertica.
	private String auxSchema;
	private String auxTable;

	//
	private ResultSet rRset;
	private int tableID;
	private int currState = 0;

	private int fldCnt;

	private Timestamp tsLastAudit;
	private int auditExp;
	private Timestamp tsLastRefresh;

	private int poolID;
	private long startMS;
	private long endMS;

	private String srcTblAb7;

	private String lName, jName;

	private Timestamp tsThisRefresh;
	private long seqThisRef;

	private static final Logger ovLogger = LogManager.getLogger();

	private static final Metrix metrix = Metrix.getInstance();

	// encapsulate the details into tblDetailJSON;
	private JSONObject tblDetailJSON;
	private JSONObject srcDBDetail;
	private JSONObject tgtDBDetail;
	private JSONObject auxDBDetail;
	private JSONObject miscValues=new JSONObject();
	
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

	public void setupTableJob(String jID, int tblID) {
		jobID = jID;
		tableID = tblID;
		
		tblDetailJSON=null;
		srcDBDetail=null;
		tgtDBDetail=null;
		auxDBDetail=null;
		miscValues=null;

		lName=null;
		jName=null;
		
		initTableDetails();
		initFieldMetaData();

		try {
			srcDBDetail = readDBDetails(tblDetailJSON.get("src_db_id").toString());
			tgtDBDetail = readDBDetails(tblDetailJSON.get("tgt_db_id").toString());
			if(jID.equals("syncTbl")) {
				auxDBDetail = readDBDetails(tblDetailJSON.get("aux_dbid").toString());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void setupAuxJob(String jID, String srcDB, String l, String j, String tgtDB) {
		jobID = jID;
		lName=l;
		jName=j;
		
		tblDetailJSON=null;
		
		srcDBDetail=null;
		auxDBDetail=null;
		//miscValues=null;

		//initFieldMetaData();
		try {
			srcDBDetail = readDBDetails(srcDB);
			auxDBDetail = readDBDetails(tgtDB);
		} catch (SQLException e) {
			ovLogger.error(e);
		}
		initAuxDetails();
	}
	private void initAuxDetails() {
		try {
			String sqlStr="select src_db_id, tgt_db_id, src_schema, src_table, seq_last_ref, ts_last_ref "
					+ " from meta_table " + " where src_db_id='" + srcDBDetail.get("db_id") + "' and src_schema='"
					+ lName + "' and src_table='" + jName + "' and tgt_schema='*'";
			rRset = repStmt.executeQuery(sqlStr);
			//rRset.next();
			tblDetailJSON = (JSONObject) ResultSetToJsonMapper(rRset).get(0);
			rRset.close();
		} catch (SQLException e) {
			ovLogger.error(e.getMessage());
		}
	}

	// return db details as a simpleJSON object, (instead of a Java object, which is
	// too cumbersome).
	public JSONObject readDBDetails(String dbid) throws SQLException {

		JSONObject jo = new JSONObject();
		Statement stmt = null;
		ResultSet rset = null;

		try {
			stmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rset = stmt.executeQuery("select db_id, db_cat, db_type, db_conn, db_driver, db_usr, db_pwd "
					+ " from meta_db " + " where db_id='" + dbid + "'");
			jo = (JSONObject) ResultSetToJsonMapper(rset).get(0);
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

		return jo;
	}

	// return Table details as a simpleJSON object, (instead of a Java object, which
	// is too cumbersome).
	private void initTableDetails() {
		try {
			rRset = repStmt.executeQuery(
					"select tbl_id, tbl_pk, src_db_id, src_schema, src_table, tgt_db_id, tgt_schema, tgt_table, \n" + 
					"pool_id, init_dt, init_duration, curr_state, aux_db_id, aux_prg_type,  src_jurl_name, \n" + 
					"aux_prg_name, aux_chg_topic, ts_regist, ts_last_ref, seq_last_ref "
							+ " from meta_table " + " where tbl_id=" + tableID);
			tblDetailJSON = (JSONObject) ResultSetToJsonMapper(rRset).get(0);
			srcTblAb7=tblDetailJSON.get("src_table").toString().substring(0,6);
		} catch (SQLException e) {
			ovLogger.error(e);
		}finally {
			try {
				rRset.close();
				repStmt.close();
			} catch (SQLException e) {
				ovLogger.error(e);
			}
		}
	}


	private JSONArray ResultSetToJsonMapper(ResultSet rs) throws SQLException {
		JSONArray jArray = new JSONArray();
		JSONObject jsonObject = null;

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		while (rs.next()){
		jsonObject = new JSONObject();
		for (int index = 1; index <= columnCount; index++) {
			String column = rsmd.getColumnName(index);
			Object value = rs.getObject(column);
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

		//return jsonObject;
		return jArray;
	}

	public JSONObject getTableDetails() {
		return tblDetailJSON;
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
		boolean rtv=false;
		try {
		if ( Timestamp.valueOf(tblDetailJSON.get("LAST_REF_TS").toString()).before(
		Timestamp.valueOf(tblDetailJSON.get("TS_REGIST").toString())) ) {
			rtv=true;
		}
		}catch (NullPointerException e) {
			rtv=false;
			ovLogger.error(e);
		}
		return rtv;
	}
	public void markStartTime() {
		Calendar cal = Calendar.getInstance();
		startMS = cal.getTimeInMillis();

	}

	public void markEndTime() {
		Calendar cal = Calendar.getInstance();
		endMS = cal.getTimeInMillis();
	}

	public void saveTblStats() {
		markEndTime();
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
				"JurnalSeq,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");

		// Save to MetaRep:
		java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt = null;
		ResultSet rset = null;
		String sqlStmt = "update meta_table set init_dt = " + ts 
				+ " init_duration = " + duration 
				+ " curr_sate = " + currState
				+ " ts_last_ref = " + ts
				+ " seq_last_seq = " + miscValues.get("thisJournalSeq")
				+ " where tbl_id = " + tableID;

		try {
			stmt = repConn.createStatement();
			rset = stmt.executeQuery(sqlStmt);
		} catch (SQLException e) {
			ovLogger.error(e);
		} finally {
			try {
				rset.close();
				stmt.close();
				repConn.commit();
			} catch (SQLException e) {
				ovLogger.error(e);
			}
		}
	}
	public void saveAuxStats() {
		markEndTime();
		int duration = (int) (endMS - startMS) / 1000;
		ovLogger.info(jobID + " duration: " + duration + " sec");

		// report to InfluxDB:
		metrix.sendMX(
				"duration,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + duration + "\n");
		metrix.sendMX(
				"insCnt,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + totalMsgCnt + "\n");
		metrix.sendMX(
				"JurnalSeq,jobId=" + jobID + ",tblID=" + tableID + "~" + srcTblAb7 + " value=" + miscValues.get("thisJournalSeq") + "\n");

		// Save to MetaRep:
		java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt = null;
		String sqlStmt = "update meta_table set TS_LAST_REF = CURRENT_TIMESTAMP," 
				+ " SEQ_LAST_REF = " + seqThisRef  
				+ " where SRC_SCHEMA = '" + lName + "' and SRC_TABLE='" + jName + "' "
				+ " and SRC_DB_ID = '" + tblDetailJSON.get("src_db_id") + "' and TGT_SCHEMA='*'";

		try {
			stmt = repConn.createStatement();
			int rslt = stmt.executeUpdate(sqlStmt);
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
	
	private String sqlSelectSource;
	private String sqlInsertTarget;
	private String sqlDeleteTarget;
//	private String sqlWhereClause;
	// creates select and insert strings
	private void initFieldMetaData() {
		Statement lrepStmt;
		ResultSet lrRset;
		int i;

		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		i = 0;
		lrRset = lrepStmt.executeQuery(
			  "select tgt_field, src_field, java_type from meta_table_field "
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
		return sqlInsertTarget;
	}
	public String getSQLSelSrc() {
		return sqlSelectSource;
	}
	public String getSQLDelTgt() {
		return sqlDeleteTarget;
	}
//	public String getSQLWhereClause() {
//	return sqlWhereClause;
//}
	public String getSrcAuxSQL(boolean fast, boolean relaxed) {
		long lasAuxSeq = getAuxSeqLastRefresh();
		if(lasAuxSeq == -1)
			return null;   // this is the first time run on this Journal, simply set META_AUX.SEQ_LAST_REF
		else {
		Object thisAuxSeq = miscValues.get("thisJournalSeq");
		String extWhere="";
		if (thisAuxSeq !=null) {
			extWhere = " and SEQUENCE_NUMBER <=" + thisAuxSeq.toString(); 
		}
		
		String currStr;
		if(fast)
			currStr="";
		else
			currStr="*CURCHAIN";

		if(relaxed)
			return " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
					+ " FROM table (Display_Journal('" + lName + "', '" + jName + "', " + "   '', '"
					+ currStr + "', " 
					+ "   cast(null as TIMESTAMP), " + "   cast(null as decimal(21,0)), "
					+ "   'R', " 
					+ "   ''," + "   '', '', '*QDDS', ''," 
					+ "   '', '', ''"
					+ ") ) as x where SEQUENCE_NUMBER > " + lasAuxSeq 
					+ extWhere 
					+ " order by 2 asc" ;// something weird with DB2 function: the starting SEQ
														// number seems not takining effect
		else
			return " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
					+ " FROM table (Display_Journal('" + lName + "', '" + jName + "', " + "   '', '"
					+ currStr + "', "
					+ "   cast(null as TIMESTAMP), " // pass-in the start timestamp;
					+ "   cast(" + lasAuxSeq + " as decimal(21,0)), " // starting SEQ #
					+ "   'R', " // JOURNAL CODE: record operation
					+ "   ''," // JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
					+ "   '', '', '*QDDS', ''," // Object library, Object name, Object type, Object member
					+ "   '', '', ''" // User, Job, Program
					+ ") ) as x where SEQUENCE_NUMBER > " + lasAuxSeq 
					+ extWhere
					+ " order by 2 asc";
		}
	}
public String getSrcAuxThisSeqSQL(boolean fast) {
	String currStr;
	if(fast)
		currStr="";
	else
		currStr="*CURCHAIN";
	return " select max(SEQUENCE_NUMBER) " + " FROM table (Display_Journal('" + lName + "', '" + jName
			+ "', '', '" + currStr + "', " // it looks like possible the journal can be switched and this SQL return no rows
			+ " cast(null as TIMESTAMP), " // pass-in the start timestamp;
			+ " cast(null as decimal(21,0)), " // starting SEQ #
			+ " 'R', " // JOURNAL cat: record operations
			+ " ''," // JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
			+ " '', '', '*QDDS', ''," + "   '', '', ''" // User, Job, Program
			+ ") ) as x ";
}
	
	public void setRefreshTS(Timestamp thisRefreshHostTS) {
		tsThisRefresh = thisRefreshHostTS;
	}
	public void setRefreshSeqThis(long seq) {
		if (seq > 0) {  
			seqThisRef = seq;
		} else {   //should never happen. no?
			seqThisRef = (long) miscValues.get("thisJournalSeq");
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

	public Timestamp getLastRefresh() {
		return tsLastRefresh;
	}

	public long getAuxSeqLastRefresh() {
		try {
			return Long.valueOf(tblDetailJSON.get("SEQ_LAST_REF").toString());
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

	public void setCurrentState(int cs) {
		currState = cs;
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
		} catch (SQLException e) {
			ovLogger.error(jobID + e.getMessage());
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
				+ "' and src_jurl_name='" + journal + "' and tgt_schema !='*' order by 1";

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

	public JSONArray getAuxsByPoolID(int poolID) {
		JSONArray jRslt=null;
		Statement lrepStmt = null;
		ResultSet lrRset;
		String strSQL;
		strSQL = "select src_db_id, tgt_db_id, src_jurl_name from meta_table where pool_id = " + poolID;

		// This shortterm solution is only for Oracle databases (as the source)
		try {
			lrRset = repStmt.executeQuery(strSQL);
			jRslt = ResultSetToJsonMapper(lrRset);
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

		return jRslt;
	}

	public void saveReplicateDB2() {
		int duration = (int) (int) ((endMS - startMS) / 1000);
		try {
			rRset.updateTimestamp("TS_LAST_REF400", tsThisRefresh);
			rRset.updateLong("SEQ_LAST_REF", (long) miscValues.get("thisJournalSeq"));

			rRset.updateRow();
			repConn.commit();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		metrix.sendMX("duration,jobId=" + jobID + " value=" + duration + "\n");
		metrix.sendMX("Seq#,jobId=" + jobID + " value=" + miscValues.get("thisJournalSeq") + "\n");
	}

// ... move to MetaData ?
	public void setThisRefreshHostTS() {
		tsThisRefesh = new Timestamp(System.currentTimeMillis());
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

	public String getAuxPoolID() {
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

	public boolean isAuxJob() {
		if (jName != null)
			return true;
		else
			return false;
	}

}