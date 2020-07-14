package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.vertica.jdbc.VerticaConnection;

import org.apache.logging.log4j.LogManager;


interface FunctionalTry {
    int calculate(int x); 
}


class DB2Data400 extends JDBCData {
//	private int tableID=0;
	public static final Logger logger = LogManager.getLogger();
	
	private long seqThisFresh = 0;

	//public DB2Data400(String dbid) throws SQLException {
	public DB2Data400(JSONObject dbDetailJSON, String role) throws SQLException {
		super(dbDetailJSON, role);
	}
	protected void initializeFrom(DataPoint dt) {
		logger.info("   not needed yet");
	}
	@Override
	public boolean miscPrep() {
		boolean rtc=false;
		super.miscPrep();

		String jTemp=metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("template_id"); 
		if(jTemp.equals("2DCC")) { 
			rtc=initThisRefreshSeq();
		}
		return rtc;
	}
	/************** Synch APIs ****************************/
	public ResultSet getSrcResultSet() {
		return srcRS;
	}
	@Override
	protected JSONObject getSrcSqlStmts(String template) {
	//from metaData private JSONObject getO2Vact2SQLs() {
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		switch(template) {
		case "1DATA":    //no case yet. Having it here as a remind.
		case "1DATA_":    //case: read the whole table
			pre.add( metaData.getBareSrcSQL() );
			jo.put("PRE", pre);
			break;
		case "2DATA":    //no case yet. Having it here as a remind.
			break;
		case "2DATA_":
			String sql ="DECLARE GLOBAL TEMPORARY TABLE qtemp.DCC"+ metaData.getTaskDetails().get("task_id") 
			+ "(" + metaData.getTaskDetails().get("data_pk") + " " + metaData.getKeyDataType() + ") " 
					+" NOT LOGGED"; 
			pre.add(sql);
			pre.add("INSERT INTO qtemp.DCC" + metaData.getTaskDetails().get("task_id") + " VALUES (?)" );
			sql = metaData.getBareSrcSQL() + ", qtemp.DCC"+metaData.getTaskDetails().get("task_id") + " b "
					+ " where a..rrn(a)=b." +metaData.getTaskDetails().get("data_pk");  //TOTO: may have problem!
			pre.add(sql);
			jo.put("PRE", pre);
			break;
		case "2DCC":
			sql=DB2DCCsql(true);
			pre.add(sql );
			jo.put("PRE", pre);
		}
		return jo;
	}
	private String DB2DCCsql(boolean prefered) {
//  spublic JSONObject getDJ2Kact2SQLs(boolean fast, boolean relaxed) {  //"public" as an hacker
		long lasDCCSeq = metaData.getDCCSeqLastRefresh();
		String extWhere="";
		
		if((lasDCCSeq == -1)||(seqThisFresh <= lasDCCSeq))   
			return null;   // this is the first time or no data, simply set META_AUX.SEQ_LAST_REF
	
		if (seqThisFresh > lasDCCSeq ) {
			extWhere = " and SEQUENCE_NUMBER <=" + seqThisFresh; 
		}
		
		String sql;
		String currStr="*CURCHAIN";
	
		if(prefered) {
			sql = " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
					+ " FROM table (Display_Journal('" + metaData.getTaskDetails().get("src_schema") + "', '" 
					+ metaData.getTaskDetails().get("src_table") + "', " + "   '', '"
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
		}else {
			sql= " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
					+ " FROM table (Display_Journal('" + metaData.getTaskDetails().get("src_schema") + "', '" 
					+ metaData.getTaskDetails().get("src_table") + "', " + "   '', '"
					+ currStr + "', " 
					+ "   cast(null as TIMESTAMP), " + "   cast(null as decimal(21,0)), "
					+ "   'R', " 
					+ "   ''," + "   '', '', '*QDDS', ''," 
					+ "   '', '', ''"
					+ ") ) as x where SEQUENCE_NUMBER > " + lasDCCSeq 
					+ extWhere 
					+ " order by 2 asc" ;// something weird with DB2 function: the starting SEQ
										 // number seems not takining effect
		}
		return sql;
	}
	@Override
	//public int crtSrcResultSet(int actId, JSONArray jaSQLs) {
	public int crtSrcResultSet() {
		String template = metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("template_id");

		JSONArray jaSQLs=(JSONArray) getSrcSqlStmts(template).get("PRE");
		String sql;
		for (int i = 0; i < jaSQLs.size()-1; i++) {
			sql = jaSQLs.get(i).toString();
			runUpdateSQL(sql);
		}
		sql=jaSQLs.get(jaSQLs.size()-1).toString();
		if( SQLtoResultSet(sql)<=0 ) {  // DB2AS400 journal, double check with relaxed "display_journal"
			if(template.equals("2DCC")) {
				logger.warn("Failed the 1st trying of initializing src resultset.");
				sql=DB2DCCsql(false);
				if( SQLtoResultSet(sql) <=0 ) {
					logger.warn("Failed the 2nd time for src resultset. Giveup");
					return -1;
				}
			}
		}
		return 0;
	}
	@Override
	public void crtSrcResultSet(List<String >keys) {
		//List<String> msgKeys = dcc.getSrcResultList();
		int cnt=keys.size();
		int tempTblThresh = Integer.parseInt(conf.getConf("tempTblThresh"));
		String sql;
		
		if(cnt < tempTblThresh ) {  //compose a sql with "where 
			String wc = keys.get(0);
			for (int i=1; i< keys.size(); i++) {
				wc=wc+","+keys.get(i);
			}
			sql = metaData.getBareSrcSQL() + " where rrn(a) in (" + wc + ")"; 
			SQLtoResultSet(sql);
		}else {  //TODO: ugly; also works for DB2/AS400 only! (maybe not so bad. This code is DB2Data400.java!)
			JSONObject TJ = metaData.getTaskDetails();
			//use global temp tbl
			sql = "DECLARE GLOBAL TEMPORARY TABLE DCC"+ TJ.get("task_id") + 
					"(" + TJ.get("data_pk") + " bigint) " 
					+" NOT LOGGED"; 
			runUpdateSQL(sql);
			//batch insert into the temp table:
			{
			sql = "INSERT INTO qtemp.DCC" + TJ.get("task_id") + " VALUES (?)" ;
			int[] batchIns = null;
			int i = 0, curRecCnt = 0;
			PreparedStatement insStmt;
			
			try {
					insStmt = dbConn.prepareStatement(sql);
				for (String key: keys) {
					try {
						insStmt.setString(1, key);
					} catch (Exception e) {
						logger.error("   " + e);
						//rtc = -1;
					}
					insStmt.addBatch();
				}
				try {
					batchIns = insStmt.executeBatch();
				} catch (BatchUpdateException e) {
					logger.error("   Error... rolling back");
					logger.error(e.getMessage());
				}
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			}
			sql = metaData.getBareSrcSQL();
			sql = sql + ", qtemp.DCC"+TJ.get("task_id") + " b "
					+ " where rrn(a)=b." +TJ.get("data_pk");
			SQLtoResultSet(sql);
		}
	}
	@Override
	//protected void afterSync(int actId, JSONArray jaSQLs){
	protected void afterSync(){
		String templateId = metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("template_id");

		JSONObject jaSQLs = getSrcSqlStmts(templateId);
		String sql;
		for (int i = 0; i < jaSQLs.size(); i++) {
			sql = jaSQLs.get(i).toString();
			runUpdateSQL(sql);
		}
	}

	private boolean runUpdateSQL(String sql) {
		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt=null; 
		try {
			stmt = dbConn.createStatement();
			int rslt = stmt.executeUpdate(sql);
			stmt.close();
			dbConn.commit();
		} catch (SQLException e) {
			logger.error(e);
		} 
		return true;
	}

	/*
	public boolean crtSrcDCCResultSet() {
		boolean rtv=false;
		String strLastSeq;
		String strReceiver;

		String strSQL = metaData.getSrcDCCSQL(false, false);
		if (strSQL == null) {  //To indicate no need for this step.
			return false;
		}else {
		try {
			// String strTS = new
			// SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(tblMeta.getLastRefresh());
			srcSQLStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			srcRS = srcSQLStmt.executeQuery(strSQL);
			if (srcRS.isBeforeFirst()) {// this check can throw exception, and do the needed below.
				logger.info("   opened src jrnl recordset: ");
				rtv=true;
			}
		} catch (SQLException e) {
			logger.error("initSrcLogQuery() failure: " + e);
			// 2020.04.12:
			// looks like it is possible that a Journal 0f the last entry can be deleted by
			// this time,--which mayhappen if that journal was never used -- which will
			// result in error.
			// one way is to NOT use -- cast(" + strLastSeq + " as decimal(21,0)), .
			// The code do it here in the hope of doing good thing. But the user should be
			// the one to see if that is appropreate.
			logger.warn(
					"Posssible data loss! needed journal " + metaData.getTaskDetails().get("SRC_TABLE") + " must have been deleted.");
			logger.warn("  try differently of " + metaData.getTaskDetails().get("SRC_TABLE") + ":");
			try {
				srcSQLStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				String StrSQL = metaData.getSrcDCCSQL(false, true);
				srcRS = srcSQLStmt.executeQuery(StrSQL);
				rtv=true;
				logger.info("   opened src jrnl recordset on ultimate try: ");
			} catch (SQLException ex) {
				logger.error("   ultimate failure: " + metaData.getTaskDetails().get("SRC_TABLE") + " !");
				logger.error("   initSrcLogQuery() failure: " + ex);
			}
		}
		return rtv;
		}
	}
*/
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
			whereClause = " where " + metaData.getTaskDetails().get("data_pk") +" in (" + whr + ")";

		String sqlStr = metaData.getSQLSelSrc() + whereClause;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			sqlRset = sqlStmt.executeQuery(sqlStr);
		} catch (SQLException e) {
			logger.error("   aux recordset not created");
			logger.error(e);
			logger.error(" \n\n\n" + sqlStr + "\n\n\n");
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


	/*
//TODO: used only by DB2toKafka. move to KafkaData ?
	public boolean initForKafkaMeta() {
		boolean proceed = false;
		jName = metaData.getJournalName();

		initThisRefreshSeq();
		if (metaData.getDCCSeqLastRefresh() == 0) { // this means the Journal is to be first replicated. INIT run!
			if ((seqThisFresh == 0)) // .. display_journal did not return, perhaps the journal is archived.
				setThisRefreshSeqInitExt(); // try the one with *CURCHAIN

			metaData.getMiscValues().put("thisJournalSeq", seqThisFresh);
			//metaData.setRefreshSeqLast(seqThisFresh); // and this too
		}
		if (seqThisFresh > metaData.getDCCSeqLastRefresh())
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
					" select count(distinct M_ROW)   from   " + metaData.getTaskDetails().get("src_log"));
			sqlRset.next();
			lc = Integer.parseInt(sqlRset.getString(1));
			sqlRset.close();
			sqlStmt.close();
		} catch (SQLException e) {
			// System.out.println(label + " error during threshlogcnt");
//.         logger.log(label + " error during threshlogcnt");
			logger.error(" error during threshlogcnt");
		}
		// System.out.println(label + " theshold log count: " + lc);
//.      logger.log(label + " theshold log count: " + lc);
		logger.info(" theshold log count: " + lc);
		return lc;
	}

	public int getDccCnt() {
		return (int) (seqThisFresh - metaData.getDCCSeqLastRefresh());
	}
	// locate the ending SEQUENCE_NUMBER of this run:
	private boolean initThisRefreshSeq() {
		if (initThisRefreshSeq(true))
			return true;
		else if (initThisRefreshSeq(false))
			return true;
		return false;
	}
	/***************DCC ******************/
	private String getSrcDCCThisSeqSQL(boolean fast) {
		String currStr;
		if(fast)
			currStr="";
		else
			currStr="*CURCHAIN";
		//return " select max(SEQUENCE_NUMBER) " + " FROM table (Display_Journal('" + lName + "', '" + jName
		return " select max(SEQUENCE_NUMBER) " + " FROM table (Display_Journal('" 
				+ metaData.getTaskDetails().get("src_schema") + "', '" + metaData.getTaskDetails().get("src_table")
				+ "', '', '" + currStr + "', " // it looks like possible the journal can be switched and this SQL return no rows
				+ " cast(null as TIMESTAMP), " // pass-in the start timestamp;
				+ " cast(null as decimal(21,0)), " // starting SEQ #
				+ " 'R', " // JOURNAL cat: record operations
				+ " ''," // JOURNAL entry: UP,DL,PT,PX,UR,DR,UB
				+ " '', '', '*QDDS', ''," + "   '', '', ''" // User, Job, Program
				+ ") ) as x ";
	}
	private boolean initThisRefreshSeq(boolean prefered) {
		Statement sqlStmt;
		ResultSet lrRset;
		boolean rtv=false;
		
		String strSQL;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			strSQL = getSrcDCCThisSeqSQL(prefered);
			lrRset = sqlStmt.executeQuery(strSQL);
			// note: could be empty, perhaps when DB2 just switched log file, which will land us in exception
			if (lrRset.next()) {
				seqThisFresh = lrRset.getLong(1);
				if(seqThisFresh==0) {
					logger.info("   not able to get current Journal seq. Try the expensive way. " );
					rtv=false;
				}else {
				metaData.setRefreshSeqThis(seqThisFresh);
				rtv=true;
				}
			}
			lrRset.close();
			sqlStmt.close();
		} catch (SQLException e) {
			if(prefered)
				logger.info("   not able to get current Journal seq. Try the expensive way. " + e);
			else
				logger.error("   not able to get current Journal seq. Give up. " + e);
		}
		
		return rtv;
	}
	/*************************************/


	/******** Registration APIs **********/
	@Override
	public boolean regSrcCheck(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		boolean rslt = false;
		String[] res = jurl.split("[.]", 0);
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

			if (rset.next()) {  //Means it is good if no exception.
				// rslt = true;
			}
			rslt = true;

			rset.close();
			stmt.close();
		} catch (SQLException e) {
			rslt = false;
			logger.error("DB2/AS400 regTblCheck() failed.");
			logger.error(e.getMessage());
		}

		return rslt;
	}
	@Override
	public boolean regSrc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		Statement stmt;
		ResultSet rset = null;
		JSONObject json = new JSONObject();

		String[] res = jurl.split("[.]", 0);
		String lName = res[0];
		String jName = res[1];

		String srcSQLstmt="select ";

		String sql = null;
		String sqlFields = "insert into data_field \n"
				+ " (TASK_ID, FIELD_ID, SRC_FIELD, SRC_FIELD_TYPE, SRC_FIELD_LEN, SRC_FIELD_SCALE, JAVA_TYPE, AVRO_Type) \n"  
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
			String sDataType, tDataType, aDataType;
			int xType;
			int fieldCnt = 0;
			while (rset.next()) {
				fieldCnt++;

				srcSQLstmt = srcSQLstmt + "a." + rset.getString("column_name") + ", ";
				
				sDataType = rset.getString("data_type");

				if (sDataType.equals("VARCHAR")) {
					xType = 1;
					aDataType = "[\"string\", \"null\"]";
				} else if (sDataType.equals("DATE")) {
					xType = 7;
					aDataType = "[\"string\", \"null\"], \"logicalType\": \"date\"";
				} else if (sDataType.equals("TIMESTMP")) {
					xType = 6;
					aDataType = "[\"string\",\"null\"], \"logicalType\": \"timestamp-micros\"";
				} else if (sDataType.equals("NUMERIC")) {
					xType = 4; // was 5; but let's make them all DOUBLE
					aDataType = "[\"long\", \"null\"]";
				} else if (sDataType.equals("CHAR")) {
					xType = 1;
					aDataType = "[\"string\", \"null\"]";
				} else {
					xType = 1;
					aDataType = "[\"string\", \"null\"]";
				}

				sql = sqlFields 
						+ "(" + tblID + ", " + rset.getInt("ordinal_position") + ", '"  
						+ rset.getString("column_name") + "', '" + sDataType + "', "
						+ rset.getInt("length") + ", " + rset.getInt("numeric_scale") + ", "
						+ xType + ", '\"type\": " + aDataType + "')";
				metaData.runRegSQL(sql);
			}
			sqlCrtTbl = sqlCrtTbl + " " + PK + " long ) \n;";
			//Last one, the physical PK 
			fieldCnt++;
			sql = sqlFields
					+ "("+ tblID +", " + fieldCnt + ", " 
					+ "'RRN(a) as " + PK + "', 'bigint', "
					+ "20, 0,"
					+ "1, '\"type\": \"long\"')";
			metaData.runRegSQL(sql);

			//setup the src select SQL statement
			srcSQLstmt = srcSQLstmt + "RRN(a) as " + PK 
					+ " from " + srcSch + "." + srcTbl + " a ";
			sql = "update task set src_stmt0='" + srcSQLstmt + "'"
					+ " where task_id="+tblID;
			metaData.runRegSQL(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	return true;
	}
	@Override
	public boolean regSrcDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		//DB2/AS400, instead of using trigger for DCC, it uses external PGM to caprure physical PK (RNN) to the corresponding Kafka topic
		String[] temp = jurl.split("\\.");
		String lName=temp[0];
		String jName=temp[1];

		String sql = "insert into task \n"
				+ "(TASK_ID, TEMPLATE_ID, TASK_CAT, DATA_PK, \n"
				+ "POOL_ID, \n" 
				+ "SRC_DB_ID, SRC_SCHEMA, SRC_TABLE, \n" 
				+ "TGT_DB_ID,TGT_SCHEMA,  TGT_TABLE, \n"
				+ "TS_REGIST) \n" 
				+ "values \n"
				+ "(" + (tblID+1) + ", 'DCC', 'COPY', '" + PK + "', \n"	
				+ " -1, \n"      
				+"'" +  dbID + "', '" + lName + "', '" + jName + "', \n" 
				+ "'" + dccDBid + "', '*', '*', \n"
				+ "now())\n"
				+ "on conflict (src_db_id, src_schema, src_table) do nothing"
				;
		metaData.runRegSQL(sql);

		sql = "insert into data_field \n"
				+ " (TASK_ID, FIELD_ID, SRC_FIELD, TGT_FIELD, SRC_FIELD_TYPE, JAVA_TYPE, AVRO_Type) \n"  
				+ " values \n"
				+ "("+ (tblID+1) +", 1, " 
				+ "'" + PK + "', '" + PK + "','bigint', "
				+ "1, '\"type\": \"string\"')";
		metaData.runRegSQL(sql);

		//setup the src select SQL statement
		String srcSQLstmt="BYODCCSQL";  //Build Your Own DCC SQL. Not much added value in putting it in meta_data.
		sql = "update task set src_stmt0='" + srcSQLstmt + "'"
				+ " where task_id="+(tblID+1);
		metaData.runRegSQL(sql);

		return true;
	}
	@Override
	public boolean regDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		//not used for DB2 so far.
		return true;
	}
	@Override
	public boolean unregisterSrc(int tblID) {
		//nothing to be done
		return true;
	}
	@Override
	public boolean unregisterDcc(int tblID) {
		//nothing to be done
		return true;
	}

	/***************************************************/
private String byodccSQL(boolean fast, boolean relaxed)	{
	long lasDCCSeq = metaData.getDCCSeqLastRefresh();
	String extWhere="";
	
	if((lasDCCSeq == -1)||(seqThisFresh <= lasDCCSeq))   
		return null;   // this is the first time or no data, simply set META_AUX.SEQ_LAST_REF

	if (seqThisFresh > lasDCCSeq ) {
		extWhere = " and SEQUENCE_NUMBER <=" + seqThisFresh; 
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
				+ " FROM table (Display_Journal('" + metaData.getTaskDetails().get("src_schema") + "', '" + metaData.getTaskDetails().get("src_table") + "', " + "   '', '"
				+ currStr + "', " 
				+ "   cast(null as TIMESTAMP), " 
				+ "   cast(null as decimal(21,0)), "
				+ "   'R', " 
				+ "   ''," + "   '', '', '*QDDS', ''," 
				+ "   '', '', ''"
				+ ") ) as x where SEQUENCE_NUMBER > " + lasDCCSeq 
				+ extWhere 
				+ " order by 2 asc" ;// something weird with DB2 function: the starting SEQ
									 // number seems not takining effect
		return sql;
	}else
		sql = " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR, trim(both from SUBSTR(OBJECT,11,10))||'.'||trim(both from SUBSTR(OBJECT,21,10)) as SRCTBL"
				+ " FROM table (Display_Journal('" + metaData.getTaskDetails().get("src_schema") + "', '" + metaData.getTaskDetails().get("src_table") + "', " + "   '', '"
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
		return sql;
	}
	public boolean beginDCC(){
		//TODO: For DCC, need to set task.seq_last_ref;
		initThisRefreshSeq();
		
		return true;
	}

	// ..............

	public void commit(){
		try {
			dbConn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void rollback(){
		try {
			dbConn.rollback();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//try Functional programming
public void tryFunctional(String srcSch, String srcTbl, String journal, FunctionalTry s) {
	//FunctionalTry s = (int x)->x*x; 
	int ans = s.calculate(5); 
    System.out.println(ans + srcSch); 
}


}