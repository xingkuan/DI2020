package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;

class DB2Data400 extends DataPointer {
	private int tableID;

	private String jLibName;
	private String jName;

	private long seqThisFresh = 0;
	private java.sql.Timestamp tsThisRefesh = null;

	private static final Logger ovLogger = LogManager.getLogger();

	public DB2Data400(String dbid) {
		super(dbid);
	}
	
	protected void initializeFrom(DataPointer dt) {
	}

	public boolean miscPrep() {
		// TODO Auto-generated method stub
		return true;
	}

	protected ResultSet getSrcResultSet(String whr) {
		Statement sqlStmt;
		ResultSet sqlRset = null;

		String whereClause="";

		if (!whr.equals(""))	
			whereClause = " where rrn(a) in (" + whr + ")"; 

		String sqlStr = metaData.getSQLSelect() + whereClause;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			sqlRset = sqlStmt.executeQuery(sqlStr);
		} catch (SQLException e) {
			ovLogger.error("   aux recordset not created");
			ovLogger.error(e);
			ovLogger.error(" \n\n\n" + sqlStr + "\n\n\n");
		}

		return sqlRset;
	}

//DB2/AS400 specific
	public long getThisRefreshSeq() {
		setThisRefreshSeq();
		return seqThisFresh;
	}
	

	public ResultSet getAuxResultSet(String rrns) {
		Statement sqlStmt;
		ResultSet sqlRset = null;

		String whereClause;

		if (rrns.equals("")) { // empty where clause is used only for initializing a table
			whereClause = "";
		} else { // otherwise, will be a list of RRN like "1,2,3"
			whereClause = " where rrn(a) in (" + rrns + ")";
		}

		String sqlStr = metaData.getSQLSelect() + " " + whereClause;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			sqlRset = sqlStmt.executeQuery(sqlStr);
		} catch (SQLException e) {
			ovLogger.error("   aux recordset not created");
			ovLogger.error(e);
			ovLogger.error(" \n\n\n" + sqlStr + "\n\n\n");
		}

		return sqlRset;
	}
	//build where clause from meta data
	public ResultSet getAuxResultSet() {
		Statement sqlStmt;
		ResultSet sqlRset = null;

		String whereClause = metaData.getSQLWhereClause();

		String sqlStr = metaData.getSQLSelect() + " " + whereClause;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			sqlRset = sqlStmt.executeQuery(sqlStr);
		} catch (SQLException e) {
			ovLogger.error("   aux recordset not created");
			ovLogger.error(e);
			ovLogger.error(" \n\n\n" + sqlStr + "\n\n\n");
		}

		return sqlRset;
	}
	public ResultSet initSrcLogQuery400() {
		Statement sqlStmt;
		ResultSet sqlRset = null;

		String strLastSeq;
		String strReceiver;

		if (metaData.getSeqLastRefresh() == 0) {
			ovLogger.error("   " + jLibName + "." + jName + " is not initialized.");
			//Should we abort here ??? or call setThisRefreshSeqInitExt() to initialize it? ;
		} else {
			ovLogger.info(
					"initSrcLogQuery(): " + jLibName + "." + jName + " last Seq: " + metaData.getSeqLastRefresh());
			strLastSeq = Long.toString(metaData.getSeqLastRefresh());
			strReceiver = "*CURCHAIN";

			try {
				// String strTS = new
				// SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(tblMeta.getLastRefresh());
				sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				String StrSQLRRN = " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
						+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', " + "   '', '"
						+ strReceiver + "', "
						// + " cast('" + strTS +"' as TIMESTAMP), " //pass-in the start timestamp;
						+ "   cast(null as TIMESTAMP), " // pass-in the start timestamp;
						+ "   cast(" + strLastSeq + " as decimal(21,0)), " // starting SEQ #
						+ "   'R', " // JOURNAL CODE: record operation
						+ "   ''," // JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
						+ "   '', '', '*QDDS', ''," // Object library, Object name, Object type, Object member
						+ "   '', '', ''" // User, Job, Program
						+ ") ) as x where SEQUENCE_NUMBER > " + strLastSeq + " and SEQUENCE_NUMBER <=" + seqThisFresh
						+ " order by 2 asc" // something weird with DB2 function: the starting SEQ number seems not
											// takining effect
				;
				sqlRset = sqlStmt.executeQuery(StrSQLRRN);
				if (sqlRset.isBeforeFirst()) // this check can throw exception, and do the needed below.
					ovLogger.info("   opened src jrnl recordset: " );
			} catch (SQLException e) {
				ovLogger.error("initSrcLogQuery() failure: " + e);
				// 2020.04.12:
				// looks like it is possible that a Journal 0f the last entry can be deleted by
				// this time,--which mayhappen if that journal was never used -- which will
				// result in error.
				// one way is to NOT use -- cast(" + strLastSeq + " as decimal(21,0)), .
				// The code do it here in the hope of doing good thing. But the user should be
				// the one to see if that is appropreate.
				ovLogger.warn(
						"Posssible data loss! needed journal " + jLibName + "." + jName + " must have been deleted.");
				ovLogger.warn("  try differently of " + jLibName + "." + jName + ":");
				try {
					sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
					String StrSQLx = " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
							+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', " + "   '', '"
							+ strReceiver + "', " + "   cast(null as TIMESTAMP), " + "   cast(null as decimal(21,0)), "
							+ "   'R', " + "   ''," + "   '', '', '*QDDS', ''," + "   '', '', ''"
							+ ") ) as x where SEQUENCE_NUMBER > " + strLastSeq + " and SEQUENCE_NUMBER <="
							+ seqThisFresh + " order by 2 asc" // something weird with DB2 function: the starting SEQ
																// number seems not takining effect
					;
					sqlRset = sqlStmt.executeQuery(StrSQLx);
					ovLogger.info("   opened src jrnl recordset on ultimate try: " );
				} catch (SQLException ex) {
					ovLogger.error("   ultimate failure: " + jLibName + "." + jName + " !");
					ovLogger.error("   initSrcLogQuery() failure: " + ex);

				}
			}
		}
		return sqlRset;
	}

	public boolean ready() {
		boolean isReady = true;

		if (metaData.getSeqLastRefresh() == 0) {
			ovLogger.error("   Journal is not replicated to Kafka yet!");
			isReady = false;
		}

		metaData.setCurrentState(1); // set current state to initializing
		metaData.markStartTime();

		return isReady;
	}


//TODO: used only by DB2toKafka. move to KafkaData ?
	public boolean initForKafkaMeta() {
		boolean proceed=false;
		jLibName = metaData.getJournalLib();
		jName = metaData.getJournalName();

		setThisRefreshSeq();
		if (metaData.getSeqLastRefresh() == 0) { // this means the Journal is to be first replicated. INIT run!
			if ((seqThisFresh == 0)) // .. display_journal did not return, perhaps the journal is archived.
				setThisRefreshSeqInitExt(); // try the one with *CURCHAIN

			metaData.setRefreshSeq400This(seqThisFresh); // set last to the current seqNum
			metaData.setRefreshSeq400Last(seqThisFresh); // and this too
		}
		if (seqThisFresh > metaData.getSeqLastRefresh())
			proceed = true;

		return proceed;
	}


	public int getThreshLogCount() {
		Statement sqlStmt;
		ResultSet sqlRset = null;
		// counts and returns the number of records in the source log table

		int lc = 0;
		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			sqlRset = sqlStmt.executeQuery(
					" select count(distinct M_ROW)   from   " + metaData.getTableDetails().get("src_log") );
			sqlRset.next();
			lc = Integer.parseInt(sqlRset.getString(1));
			sqlRset.close();
			sqlStmt.close();
		} catch (SQLException e) {
			// System.out.println(label + " error during threshlogcnt");
//.         ovLogger.log(label + " error during threshlogcnt");
			ovLogger.error( " error during threshlogcnt");
		}
		// System.out.println(label + " theshold log count: " + lc);
//.      ovLogger.log(label + " theshold log count: " + lc);
		ovLogger.info( " theshold log count: " + lc);
		return lc;
	}

	public int getRecordCount() {
		int rtv;
		ResultSet lrRset;

		rtv = 0;
		try {
			Statement sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			lrRset = sqlStmt
					.executeQuery("select count(*) from " + metaData.getTableDetails().get("src_sch").toString() + "." + metaData.getTableDetails().get("src_tbl").toString());
			lrRset.next();
			rtv = Integer.parseInt(lrRset.getString(1));

			lrRset.close();
			sqlStmt.close();
		} catch (SQLException e) {
			ovLogger.error(e);
		}
		return rtv;
	}


	// locate the ending SEQUENCE_NUMBER of this run:
	private void setThisRefreshSeq() {
		Statement sqlStmt;
		ResultSet sqlRset = null;
		ResultSet lrRset;

		String strSQL;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			strSQL = " select max(SEQUENCE_NUMBER) " + " FROM table (Display_Journal('" + jLibName + "', '" + jName
					+ "', " + "   '', '', " // it looks like possible the journal can be switched and this SQL return no
											// row.
					+ "   cast(null as TIMESTAMP), " // pass-in the start timestamp;
					+ "   cast(null as decimal(21,0)), " // starting SEQ #
					+ "   'R', " // JOURNAL cat: record operations
					+ "   ''," // JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
					+ "   '', '', '*QDDS', ''," + "   '', '', ''" // User, Job, Program
					+ ") ) as x ";
			lrRset = sqlStmt.executeQuery(strSQL);
			// could be empty when DB2 just switched log file.
			if (lrRset.next()) {
				seqThisFresh = lrRset.getLong(1);
				metaData.setRefreshSeq400This(seqThisFresh);
			}
			lrRset.close();

			sqlStmt.close();
		} catch (SQLException e) {
			ovLogger.error("   error in setThisRefreshSeq(): " + e);
			//TODO: if empty, shouldn't something be done here?
		}
	}

	private void setThisRefreshSeqInitExt() {
		Statement sqlStmt;
		ResultSet sqlRset = null;
		ResultSet lrRset;

		String strSQL;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			// locate the ending SEQUENCE_NUMBER of this run:
			strSQL = " select max(SEQUENCE_NUMBER) " + " FROM table (Display_Journal('" + jLibName + "', '" + jName
					+ "', " + "   '', '*CURCHAIN', " // it looks like possible the journal can be switched and this SQL
														// return no row.
					+ "   cast(null as TIMESTAMP), " // pass-in the start timestamp;
					+ "   cast(null as decimal(21,0)), " // starting SEQ #
					+ "   'R', " // JOURNAL cat: record operations
					+ "   ''," // JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
					+ "   '', '', '*QDDS', ''," + "   '', '', ''" // User, Job, Program
					+ ") ) as x ";
			lrRset = sqlStmt.executeQuery(strSQL);
			// I guess it could be 0 when DB2 just switched log file.
			if (lrRset.next()) {
				seqThisFresh = lrRset.getLong(1);
				metaData.setRefreshSeq400This(seqThisFresh);
			}
			lrRset.close();

			sqlStmt.close();
		} catch (SQLException e) {
			ovLogger.error("   error in setThisRefreshSeq(): " + e);
		}
	}

	public long getCurrSeq() {
		return seqThisFresh;
	}

	public void commit() throws SQLException {
		dbConn.commit();
	}

	public void rollback() throws SQLException {
		dbConn.rollback();
	}

}