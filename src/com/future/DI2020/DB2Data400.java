package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;


interface FunctionalTry {
    int calculate(int x); 
}


class DB2Data400 extends DataPointer {
//	private int tableID=0;
	private String jName=null;

	private Statement srcSQLStmt = null;
	private ResultSet srcRS = null;
	
	private long seqThisFresh = 0;

	private static final Logger ovLogger = LogManager.getLogger();

	public DB2Data400(String dbid) throws SQLException {
		super(dbid);
	}
	protected void initializeFrom(DataPointer dt) {
		ovLogger.info("   not needed yet");
	}

	public boolean miscPrep() {
		super.miscPrep();
		if(jName != null) { //when jName is set, it must be for sync RRN to Kafka
			initThisRefreshSeq();
		}
		return true;
	}

	public ResultSet getSrcResultSet() {
		return srcRS;
	}

	public boolean crtSrcAuxResultSet() {
		boolean rtv=false;
		String strLastSeq;
		String strReceiver;

		String strSQL = metaData.getSrcAuxSQL(false, false);
		if (strSQL == null) {
			return false;
		}else {
		try {
			// String strTS = new
			// SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(tblMeta.getLastRefresh());
			srcSQLStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			srcRS = srcSQLStmt.executeQuery(strSQL);
			if (srcRS.isBeforeFirst()) {// this check can throw exception, and do the needed below.
				ovLogger.info("   opened src jrnl recordset: ");
				rtv=true;
			}
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
					"Posssible data loss! needed journal " + jName + " must have been deleted.");
			ovLogger.warn("  try differently of " + jName + ":");
			try {
				srcSQLStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				String StrSQL = metaData.getSrcAuxSQL(false, true);
				srcRS = srcSQLStmt.executeQuery(StrSQL);
				rtv=true;
				ovLogger.info("   opened src jrnl recordset on ultimate try: ");
			} catch (SQLException ex) {
				ovLogger.error("   ultimate failure: " + jName + " !");
				ovLogger.error("   initSrcLogQuery() failure: " + ex);
			}
		}
		return rtv;
		}
	}
	
	public void releaseRSandSTMT() {
		try {
			srcSQLStmt.close();
			srcRS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

/*
	protected ResultSet getSrcResultSet(String whr) {
		Statement sqlStmt;
		ResultSet sqlRset = null;

		String whereClause = "";

		if (!whr.equals(""))
			whereClause = " where " + metaData.getTableDetails().get("tbl_pk") +" in (" + whr + ")";

		String sqlStr = metaData.getSQLSelSrc() + whereClause;

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
*/
//DB2/AS400 specific
	public long getThisRefreshSeq() {
		//put it in miscPrep
		//setThisRefreshSeq();
		return seqThisFresh;
	}


	public boolean ready() {
		boolean isReady = true;

		if (metaData.getAuxSeqLastRefresh() == 0) {
			ovLogger.error("   Journal is not replicated to Kafka yet!");
			isReady = false;
		}

		metaData.setCurrentState(1); // set current state to initializing
		metaData.markStartTime();

		return isReady;
	}

	/*
//TODO: used only by DB2toKafka. move to KafkaData ?
	public boolean initForKafkaMeta() {
		boolean proceed = false;
		jName = metaData.getJournalName();

		initThisRefreshSeq();
		if (metaData.getAuxSeqLastRefresh() == 0) { // this means the Journal is to be first replicated. INIT run!
			if ((seqThisFresh == 0)) // .. display_journal did not return, perhaps the journal is archived.
				setThisRefreshSeqInitExt(); // try the one with *CURCHAIN

			metaData.getMiscValues().put("thisJournalSeq", seqThisFresh);
			//metaData.setRefreshSeqLast(seqThisFresh); // and this too
		}
		if (seqThisFresh > metaData.getAuxSeqLastRefresh())
			proceed = true;

		return proceed;
	}
*/
	public int getThreshLogCount() {
		Statement sqlStmt;
		ResultSet sqlRset = null;
		// counts and returns the number of records in the source log table

		int lc = 0;
		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			sqlRset = sqlStmt.executeQuery(
					" select count(distinct M_ROW)   from   " + metaData.getTableDetails().get("src_log"));
			sqlRset.next();
			lc = Integer.parseInt(sqlRset.getString(1));
			sqlRset.close();
			sqlStmt.close();
		} catch (SQLException e) {
			// System.out.println(label + " error during threshlogcnt");
//.         ovLogger.log(label + " error during threshlogcnt");
			ovLogger.error(" error during threshlogcnt");
		}
		// System.out.println(label + " theshold log count: " + lc);
//.      ovLogger.log(label + " theshold log count: " + lc);
		ovLogger.info(" theshold log count: " + lc);
		return lc;
	}

	public int getRecordCount() {
		int rtv;
		ResultSet lrRset;

		rtv = 0;
		try {
			Statement sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			lrRset = sqlStmt.executeQuery("select count(*) from " + metaData.getTableDetails().get("src_sch").toString()
					+ "." + metaData.getTableDetails().get("src_tbl").toString());
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
	private boolean initThisRefreshSeq() {
		if (initThisRefreshSeq(true))
			return true;
		else if (initThisRefreshSeq(false))
			return true;
		return false;
	}
	private boolean initThisRefreshSeq(boolean fast) {
		Statement sqlStmt;
		ResultSet lrRset;
		boolean rtv=false;
		
		String strSQL;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			strSQL = metaData.getSrcAuxThisSeqSQL(fast);
			lrRset = sqlStmt.executeQuery(strSQL);
			// note: could be empty, perhaps when DB2 just switched log file, which will land us in exception
			if (lrRset.next()) {
				seqThisFresh = lrRset.getLong(1);
				metaData.setRefreshSeqThis(seqThisFresh);
				rtv=true;
			}
			lrRset.close();
			sqlStmt.close();
		} catch (SQLException e) {
			if(fast)
				ovLogger.info("   not able to get current Journal seq. Try the expensive way. " + e);
			else
				ovLogger.error("   not able to get current Journal seq. Give up. " + e);
		}
		
		return rtv;
	}


// methods for registration
	public JSONObject genRegSQLs(int tblID, String srcSch, String srcTbl, String tgtSch, String tgtTbl) {
		Statement stmt;
		ResultSet rset = null;
		JSONObject json = new JSONObject();

		String sqlFields = "insert into META_TABLE_FIELD \n"
				+ " (TBL_ID, FIELD_ID, SRC_FIELD, SRC_FIELD_TYPE, TGT_FIELD, TGT_FIELD_TYPE, JAVA_TYPE) \n"  
				+ " values \n";
		String sqlCrtTbl = "create table " + tgtSch + "." + tgtTbl + "\n ( ";

		try {
			stmt = dbConn.createStatement();
			String sqlStmt = "select c.ordinal_position, c.column_name, "
					// not needed columns          + "k.ordinal_position as key_column, k.asc_or_desc as key_order, "
					          + "c.data_type, c.length, c.numeric_scale, c.is_nullable, c.column_text "
					+ "from qsys2.syscolumns c join qsys2.systables t "
					+ "on c.table_schema = t.table_schema and c.table_name = t.table_name "
					+ "left outer join sysibm.sqlstatistics k on c.table_schema = k.table_schem "
					+ " and c.table_name   = k.table_name and c.table_name   = k.index_name "
					+ " and c.column_name  = k.column_name " 
					+ "where c.table_schema = '" + srcSch + "' "
					+ "  and c.table_name   = '" + srcTbl + "' " 
					+ " order by ordinal_position asc";
			rset = stmt.executeQuery(sqlStmt);
			
			String strDataSpec;
			int scal;
			String sDataType, tDataType;
			int xType;
			int fieldCnt = 0;
			while (rset.next()) {
				fieldCnt++;

				sDataType = rset.getString("data_type");

				if (sDataType.equals("VARCHAR")) {
					strDataSpec = "VARCHAR2(" + 2 * rset.getInt("length") + ")"; // simple double it to handle UTF string

					xType = 1;
				} else if (sDataType.equals("DATE")) {
					strDataSpec = "DATE";
					xType = 7;
				} else if (sDataType.equals("TIMESTMP")) {
					strDataSpec = "TIMESTAMP";
					xType = 6;
				} else if (sDataType.equals("NUMERIC")) {
					scal = rset.getInt("numeric_scale");
					if (scal > 0) {
						strDataSpec = "NUMBER(" + rset.getInt("length") + ", " + rset.getInt("numeric_scale") + ")";

						xType = 4; // was 5; but let's make them all DOUBLE
					} else {
						strDataSpec = "NUMBER(" + rset.getInt("length") + ")";

						xType = 1; // or 2
					}
				} else if (sDataType.equals("CHAR")) {
					strDataSpec = "CHAR(" + 2 * rset.getInt("length") + ")"; // simple double it to handle UTF string

					xType = 1;
				} else {
					strDataSpec = sDataType;

					xType = 1;
				}
				sqlCrtTbl = sqlCrtTbl + "\"" + rset.getString("column_name") + "\" " + strDataSpec + ",\n";  //""" is needed because column name can contain space!

				sqlFields = sqlFields 
						+ "(" + tblID + ", " + rset.getInt("ordinal_position") + ", '"  
						+ rset.getString("column_name") + "', '" + sDataType + "', '"
						+ rset.getString("column_name") + "', '" + strDataSpec + "', "
						+ xType + "),\n";
			}
			sqlCrtTbl = sqlCrtTbl + " DB2RRN int ) \n;";

			fieldCnt++;
			sqlFields = sqlFields
					+ "("+ tblID +", " + fieldCnt + ", " 
					+ "'RRN(a) as DB2RRN', 'bigint', "
					+ "'DB2RRN', 'bigint', "
					+ "1) \n;";
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		json.put("crtTbl", sqlCrtTbl);
		json.put("fldSQL", sqlFields);

		return json;
	}

	public boolean regTblMisc(String srcSch, String srcTbl, String srcLog) {
		boolean rslt = false;
		String[] res = srcLog.split("[.]", 0);
		String jLibName = res[0];
		String jName = res[1];

		Statement stmt;
		ResultSet rset = null;

		try {
			String rLib = "", rName = ""; // all receiver?
			// try to read journal of the last 4 hours(I know I'm using the client time;
			// that does not matter)
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.HOUR_OF_DAY, -4);

			//stmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt = dbConn.createStatement();

			String strTS = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(cal.getTime());
			String sqlStmt = " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR"
					+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', " + "   '" + rLib + "', '"
					+ rName + "', " + "   cast('" + strTS + "' as TIMESTAMP), " // pass-in the start timestamp;
					+ "   cast(null as decimal(21,0)), " // starting SEQ #
					+ "   'R', " // JOURNAL CODE:
					+ "   ''," // JOURNAL entry:UP,DL,PT,PX
					+ "   '" + srcSch + "', '" + srcTbl + "', '*QDDS', ''," // Object library, Object name, Object type,
																			// Object member
					+ "   '', '', ''" // User, Job, Program
					+ ") ) as x order by 2 asc";

			rset = stmt.executeQuery(sqlStmt);

			if (rset.next()) {
				// rslt = true;
			}
			rslt = true;

			rset.close();
			stmt.close();
		} catch (SQLException e) {
			rslt = false;
			ovLogger.error(e.getMessage());
		}

		return rslt;
	}
	// ..............

	public void commit() throws SQLException {
		dbConn.commit();
	}

	public void rollback() throws SQLException {
		dbConn.rollback();
	}

	//try Functional programming
public void tryFunctional(String srcSch, String srcTbl, String journal, FunctionalTry s) {
	//FunctionalTry s = (int x)->x*x; 
	int ans = s.calculate(5); 
    System.out.println(ans + srcSch); 
}


}