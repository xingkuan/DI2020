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

	private String sqlSelectSource;
	private String sqlInsertTarget;

	private Timestamp tsLastAudit;
	private int auditExp;
	private Timestamp tsLastRefresh;
	private long seqLastRef;
	private int poolID;
	private int prcTimeout;
	private long startMS;
	private long endMS;

	private int srcDBid;
	private int tgtDBid;

	private boolean tgtUseAlt;
	private String label;
	private String srcTblAb7;

	private String sqlWhereClause;

	private String journalLib, journalName;

	private Timestamp tsThisRefresh;
	private long seqThisRef;

	private static final Logger ovLogger = LogManager.getLogger();

	private static final Metrix metrix = Metrix.getInstance();

	// encapsulate the details into tblDetailJSON;
	private JSONObject tblDetailJSON;
	private JSONObject srcDBDetail;
	private JSONObject tgtDBDetail;
	private JSONObject auxDBDetail;
	
	//may not needed
	//private Map<Integer, Integer> fldType = new HashMap<>();
	int[] fldType; 
	String[] fldNames; 
	
	private int refreshCnt;

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
			ovLogger.error(e.getMessage());
		}
	}

	public void setupForJob(String jID, int tblID) {
		jobID = jID;
		tableID = tblID;
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
			jo = ResultSetToJsonMapper(rset);
		} catch (SQLException e) {
			ovLogger.error(e.getMessage());
		} finally {
			try {
				rset.close();
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
					"aux_prg_name, aux_chg_topic, ts_last_ref, seq_last_ref "
							+ " from meta_table " + " where tbl_id=" + tableID);
			tblDetailJSON = ResultSetToJsonMapper(rRset);
		} catch (SQLException e) {
			ovLogger.error(e.getMessage());
		}finally {
			try {
				rRset.close();
				repStmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private JSONObject ResultSetToJsonMapper(ResultSet rs) throws SQLException {
		// JSONArray jArray = new JSONArray();
		JSONObject jsonObject = null;

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		rs.next();
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
			} else if (value instanceof Byte) {
				jsonObject.put(column, (Byte) value);
			} else if (value instanceof byte[]) {
				jsonObject.put(column, (byte[]) value);
			} else {
				throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
			}
		}

		return jsonObject;
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

//for injecting data into Kafka (for DB2/AS400), instead of table level; read all entries of a Journal (which could be for many tables 
	public boolean initForKafka(int dbID, String jLib, String jName) {
		srcDBid = dbID;
		label = "Inject Kafka DBid: " + dbID;

		journalLib = jLib;
		journalName = jName;

		return true;
	}

	// creates select and insert strings
	private void initFieldMetaData() {
		Statement lrepStmt;
		ResultSet lrRset;
		int i;

		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

//		fldName = new String[fldCnt];
		// System.out.println("field cnt: " + fldCnt);
		i = 0;
		lrRset = lrepStmt.executeQuery(
			  "select tgt_field, src_field, java_type from meta_table_field "
			+ " where tbl_id=" + tableID + " order by field_id");
		//first line
		if (lrRset.next()) {
			sqlSelectSource = "select " + lrRset.getString("src_field");
			sqlInsertTarget = "insert into " + tblDetailJSON.get("tgt_schema") + "." + tblDetailJSON.get("tgt_table")
				+ lrRset.getString("tgt_field")	;
			fldType[i] = lrRset.getInt("java_type");
			fldNames[i] = lrRset.getString("src_field");
			i++;
		}
		//rest line
		while (lrRset.next()) {
			sqlSelectSource += ", " + lrRset.getString("src_field");
			sqlInsertTarget += ", " + "\"" + lrRset.getString("tgt_field") + "\"";
			fldType[i] = lrRset.getInt("java_type");
			fldNames[i] = lrRset.getString("src_field");
			i++;
			// System.out.println(i);
		}
		fldCnt=i;
		lrRset.close();
		
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
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
//may not needed later on.
public int[] getFldJavaType() {
	return fldType;
}
public String[] getFldNames() {
	return fldNames;
}
	public void markStartTime() {
		Calendar cal = Calendar.getInstance();
		startMS = cal.getTimeInMillis();

	}

	public void markEndTime() {
		Calendar cal = Calendar.getInstance();
		endMS = cal.getTimeInMillis();
	}

	public void saveInitStats() {
		int duration = (int) (endMS - startMS) / 1000;
		ovLogger.info(label + " duration: " + duration + " seconds");

		// Save to InfluxDB:
		metrix.sendMX(
				"initDuration,jobId=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value=" + duration + "\n");
		metrix.sendMX(
				"initRows,jobId=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value=" + refreshCnt + "\n");
		metrix.sendMX(
				"JurnalSeq,jobId=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value=" + seqThisRef + "\n");

		// Save to MetaRep:
		java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		try {
			rRset.updateInt("LAST_INIT_DURATION", (int) ((endMS - startMS) / 1000));
			rRset.updateTimestamp("TS_LAST_INIT", tsThisRefresh);
			rRset.updateTimestamp("TS_LAST_REF400", tsThisRefresh);
			rRset.updateLong("SEQ_LAST_REF", seqThisRef);
			rRset.updateTimestamp("TS_LAST_AUDIT", ts);
			rRset.updateInt("AUD_SOURCE_RECORD_CNT", refreshCnt);
			rRset.updateInt("AUD_TARGET_RECORD_CNT", refreshCnt);

			rRset.updateRow();
			repConn.commit();
		} catch (SQLException e) {
			ovLogger.error(label + e.getMessage());
		}
	}

	public void saveRefreshStats(String jobID) {
		int duration = (int) (int) ((endMS - startMS) / 1000);

		metrix.sendMX(
				"syncDuration,jobId=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value=" + duration + "\n");
		metrix.sendMX(
				"syncCount,jobId=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value=" + refreshCnt + "\n");
		metrix.sendMX(
				"JurnalSeq,jobId=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value=" + seqThisRef + "\n");

		try {
			rRset.updateInt("LAST_REFRESH_DURATION", duration);
			// 2020.02.21
			// rRset.updateTimestamp("TS_LAST_REFRESH",hostTS);
			rRset.updateTimestamp("TS_LAST_REF400", tsThisRefresh);
			rRset.updateLong("SEQ_LAST_REF", seqThisRef);
			rRset.updateInt("REFRESH_CNT", refreshCnt);

			rRset.updateRow();
			repConn.commit();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public void saveAudit(int srcRC, int tgtRC) {
		java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		ovLogger.info(label + "source record count: " + srcRC + "     target record count: " + tgtRC);
//TODO: matrix
		try {
			rRset.updateTimestamp("TS_LAST_AUDIT", ts);
			rRset.updateInt("AUD_SOURCE_RECORD_CNT", srcRC);
			rRset.updateInt("AUD_TARGET_RECORD_CNT", tgtRC);
			rRset.updateRow();
			repConn.commit();
			// System.out.println("audit info saved");
		} catch (SQLException e) {
			ovLogger.error(label + e.getMessage());
		}
	}

	public void setRefreshTS(Timestamp thisRefreshHostTS) {
		tsThisRefresh = thisRefreshHostTS;
	}

	public void setRefreshSeqThis(long thisRefreshSeq) {
		if (thisRefreshSeq > 0) {
			seqThisRef = thisRefreshSeq;
		} else {
			seqThisRef = seqLastRef;
			ovLogger.info("...hmm, got a 0 for SEQ# for srcTbl " + tblDetailJSON.get("src_table") + ". The last one: " + seqLastRef);
		}
	}

	public void setRefreshSeqLast(long seq) {

	}

	public String getJobID() {
		return jobID;
	}

	public int getPrcTimeout() {
		return prcTimeout;
	}


	public Timestamp getLastAudit() {
		return tsLastAudit;
	}

	public Timestamp getLastRefresh() {
		return tsLastRefresh;
	}

	public long getSeqLastRefresh() {
		return seqLastRef;
	}

	public int getPoolID() {
		return poolID;
	}


	public String getSQLInsert() {
		return sqlInsertTarget;
	}


	public String getSQLSelect() {
		return sqlSelectSource;
	}

	public String getSQLWhereClause() {
		return sqlWhereClause;
	}


	public String getPK() {
		return tblDetailJSON.get("tbl_pk").toString();
	}

	public String getSQLSelectXForm() {
		return sqlSelectSource;
	}

	public int getCurrState() {
		return currState;
	}

	public void setCurrentState(int cs) {
		try {
			currState = cs;
			rRset.updateInt("CURR_STATE", cs);
			rRset.updateRow();
			repConn.commit();
		} catch (SQLException e) {
			ovLogger.error(label + e.getMessage());
		}
	}

	public int getCurrentState() {
		return currState;
	}

	public int getTableID() {
		return tableID;
	}

	public void setRefreshCnt(int i) {
		refreshCnt = i;
	}

	public int getSrcDBid() {
		return srcDBid;
	}

	public int getTgtDBid() {
		return tgtDBid;
	}

	public void close() {
		try {
			rRset.close();
			repStmt.close();
			repConn.close();
		} catch (SQLException e) {
			ovLogger.error(label + e.getMessage());
		}

	}

	public String getLabel() {
		return label;
	}

	public String getJournalLib() {
		return journalLib;
	}

	public String getJournalName() {
		return journalName;
	}

	//// from OVSdb.java
	public List<String> getDB2TablesOfJournal(String dbID, String journal) {
		List<String> tList = new ArrayList<String>();
		String strSQL;
		Statement lrepStmt = null;
		ResultSet lrRset;

		strSQL = "select source_schema||'.'||source_table from sync_table where source_db_id = " + dbID
				+ " and source_log_table='" + journal + "' order by 1";

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
			try {
				if (repConn != null)
					repConn.close();
			} catch (SQLException se) {
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

	public List<String> getAS400JournalsByPoolID(int poolID) {
		Statement lrepStmt = null;
		ResultSet lrRset;
		List<String> jList = new ArrayList<String>();
		String strSQL;

		strSQL = "select source_db_id||'.'||source_log_table from sync_journal400 where pool_id = " + poolID;

		// This shortterm solution is only for Oracle databases (as the source)
		try {
			lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			lrRset = repStmt.executeQuery(strSQL);
			while (lrRset.next()) {
				// Retrieve by column name
				String jName = lrRset.getString(1);
				jList.add(jName);
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

		return jList;
	}

	public JSONObject getLogDetails() {
		JSONObject jo = null;

		try {
			repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rRset = repStmt.executeQuery("select src_db_id, src_jurl_name, seq_last_ref, last_ref_ts "
					+ " from meta_ext_prg " + " where src_db_id=" + srcDBid + " and src_jurl_name='"
					+ tblDetailJSON.get("src_jurl_name") + "'");
			rRset.next();
			jo = ResultSetToJsonMapper(rRset);
			rRset.close();

		} catch (SQLException e) {
			ovLogger.error(e.getMessage());

		}
		return jo;
	}

	public void saveReplicateDB2() {
		int duration = (int) (int) ((endMS - startMS) / 1000);
		try {
			rRset.updateTimestamp("TS_LAST_REF400", tsThisRefresh);
			rRset.updateLong("SEQ_LAST_REF", seqThisRef);

			rRset.updateRow();
			repConn.commit();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		metrix.sendMX("duration,jobId=" + label + " value=" + duration + "\n");
		metrix.sendMX("Seq#,jobId=" + label + " value=" + seqThisRef + "\n");
	}

// ... move to MetaData ?
	public void setThisRefreshHostTS() {
		tsThisRefesh = new Timestamp(System.currentTimeMillis());
	}

	int totalDelCnt, totalInsCnt, totalErrCnt;

	public void sendMetrix() {
		metrix.sendMX("delCnt,metaData.getJobID()=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value="
				+ totalDelCnt + "\n");
		metrix.sendMX("insCnt,metaData.getJobID()=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value="
				+ totalInsCnt + "\n");
		metrix.sendMX("errCnt,metaData.getJobID()=" + jobID + ",tblID=" + srcTblAb7 + "~" + tableID + " value="
				+ totalErrCnt + "\n");
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

}