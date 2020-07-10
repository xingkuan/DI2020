package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;

class OracleData extends JDBCData{
	public static final Logger logger = LogManager.getLogger();
	
	private Statement srcSQLStmt = null;
	private ResultSet srcRS = null;
	
   //public OracleData(String dbID) throws SQLException {
   public OracleData(JSONObject dbID, String role) throws SQLException {
		super(dbID, role);
   }
//   protected void initializeFrom(DataPoint dt) {
//		logger.info("   not needed yet");
//  }
   @Override
	public boolean miscPrep() {
		boolean rtc=true;
		super.miscPrep();
		//if(jTemp.equals("DJ2K")) { 
		//	rtc=initThisRefreshSeq();
		//}
		return rtc;
	}

	public ResultSet getSrcResultSet() {
		return srcRS;
	}
	//should only be sued when TEMP_ID
	public int getDccCnt() {  //this one is very closely couple with crtSrcResultSet! TODO: any better way for arranging?
		if(!metaData.getTableDetails().get("temp_id").toString().contains("_")){
			logger.warn("It does not look like it should be called!");
		}
		String sql = "update " + metaData.getTableDetails().get("src_dcc_tbl") + " set dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" ;

		int cnt = runUpdateSQL(sql);

		return cnt;
	}
	/********** Synch APIs *********************************/
	@Override
	protected JSONObject getSrcSqlStmts(String template) {
	//from metaData private JSONObject getO2Vact2SQLs() {
		JSONObject jo = new JSONObject();
		JSONArray pre = new JSONArray();
		switch(template) {
			case "1":    //case: read the whole table
				pre.add(1, metaData.getBareSrcSQL() );
				jo.put("PRE", pre);
			case "2TRIG":   //read the changed rows. Original O2V, O2K
				pre.add("update " + metaData.getTableDetails().get("src_dcc_tbl") 
						+ " set dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" );
				pre.add(metaData.getBareSrcSQL() + ", " + metaData.getTableDetails().get("src_dcc_tbl") 
						+ " b where b.dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') "
						+ " and a.rowid=b."+metaData.getTableDetails().get("tbl_pk"));
				jo.put("PRE", pre);
				JSONArray aft = new JSONArray();
				aft.add("delete from " + metaData.getTableDetails().get("src_dcc_tbl") 
						+ " where dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" );
				jo.put("AFT", aft);
				break;
		}
		
		return jo;
	}
	//public int crtSrcResultSet(int actId, JSONArray jaSQLs) {
	@Override
	public int crtSrcResultSet() {
		String template = metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("temp_id");

		JSONArray jaSQLs=(JSONArray) getSrcSqlStmts(template).get("PRE");
		String sql;
		for (int i = 0; i < jaSQLs.size()-1; i++) {
			sql = jaSQLs.get(i).toString();
			runUpdateSQL(sql);
		}
		sql=jaSQLs.get(jaSQLs.size()-1).toString();
		if( SQLtoResultSet(sql)<=0 ) {
			return -1;
		}
		return 0;
	}

	public List<String> getDCCKeyList(){
		List<String> keyList=new ArrayList<String>();

	    Statement stmt1;
 	    ResultSet rs1;
		String sql1 = "select distinct orarid from " 
					+ metaData.getTableDetails().get("src_dcc_tbl") 
					+ " where dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')";
		try {
			stmt1 = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs1 = stmt1.executeQuery(sql1);
			while(rs1.next())
				keyList.add(rs1.getString(1));
			rs1.close();
			stmt1.close();
		} catch (SQLException e) {
			logger.error("   " + e);
		}

		return keyList;
	}
	@Override
	//protected void afterSync(int actId, JSONArray jaSQLs){
	protected void afterSync(){
		String templateId = metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("temp_id");

		JSONObject jaSQLs = getSrcSqlStmts(templateId);

		String sql;
		for (int i = 0; i < jaSQLs.size(); i++) {
			sql = jaSQLs.get(i).toString();
			runUpdateSQL(sql);
		}
	}
   
	/******** Registration APIs **********/
	@Override
	public boolean regSrcCheck(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
        //if log table "jurl" exist, return false;	
		String sql="select TABLE_NAME from dba_tables where OWNER||'.'||TABLE_NAME='" + jurl + "'";
		if(SQLtoResultSet(sql)>0) {
			logger.error("log table " + jurl + " exist already!");
			return false;
		}
		//trigger name "dccPgm" exist, return false
		sql="select TRIGGER_NAME from dba_triggers where owner||'.'||TRIGGER_NAME='"+ dccPgm + "'";
		if(SQLtoResultSet(sql)>0) {
			logger.error("log trigger " + dccPgm + " exist already!");
			return false;
		}
		return true;
	}
	@Override
	protected boolean regSrc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		Statement stmt;
		ResultSet rset = null;
		//JSONObject json = new JSONObject();
		String srcSQLstmt="select ";
		String sql="select ";
		String sqlFields = "insert into SYNC_TABLE_FIELD \n"
				+ " (TBL_ID, FIELD_ID, SRC_FIELD, SRC_FIELD_TYPE, SRC_FIELD_LEN, SRC_FIELD_SCALE, JAVA_TYPE, AVRO_Type) \n"  
				+ " values \n";

		try {
			stmt = dbConn.createStatement();
			String sqlStmt = "select c.column_id, c.column_name, "
					          + "c.data_type, c.data_length, c.data_scale, c.data_precision "
					+ "from dba_tab_columns c "
					+ "where c.owner = upper('" + srcSch + "') "
					+ "  and c.table_name   = upper('" + srcTbl + "') " 
					+ " order by c.column_id asc";
			
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

				if (sDataType.equals("VARCHAR2")) {
					xType = 1;
					aDataType = "[\"string\", \"null\"]";
				} else if (sDataType.equals("DATE")) {
					xType = 7;
					aDataType = "\"int\", \"logicalType\": \"date\"";
				} else if (sDataType.contains("TIMESTAMP")) {
					xType = 6;
					aDataType = "\"string\", \"logicalType\": \"timestamp-micros\"";
				} else if (sDataType.equals("NUMBER")) {
					scal = rset.getInt("data_scale");
					if (scal > 0) {
						xType = 4; // was 5; but let's make them all DOUBLE
						//aDataType = "\"bytes\", \"logicalType\": \"decimal\",\"precision\":" + rset.getInt("data_length")
						//+ "\"scale\": " + scal;
					} else {
						xType = 4; // or 2
						//aDataType = "\"bytes\", \"logicalType\": \"decimal\",\"precision\":" + rset.getInt("data_length")
						//+ "\"scale\": " + 0;
					}
					aDataType = "\"long\"";   //BigDecimal is what getObject returns; but it can't work with AVRO.
				} else if (sDataType.equals("CHAR")) {
					xType = 1;
					aDataType = "[\"string\", \"null\"]";
				} else {
					xType = 1;
					aDataType = "\"string\"";
				}
				
				sql = sqlFields 
						+ "(" + tblID + ", " + rset.getInt("column_id") + ", '"  
						+ rset.getString("column_name") + "', '" + sDataType + "', "
						+ rset.getInt("data_length") + ", " + rset.getInt("data_scale") + ", "
						+ xType + ", '\"type\": " + aDataType + "')";
				metaData.runRegSQL(sql);
			}
			//lastly, add the internal rowID
			fieldCnt++;
			sql = sqlFields
					+ "("+ tblID +", " + fieldCnt + ", " 
					+ "'rowid as " + PK + "', 'varchar(20)', "  //Please keep it lower case!!!
					+ "20, 0, "
					+ "1, 'string') ";
			metaData.runRegSQL(sql);
			//The bare select statement for reading the source.
			srcSQLstmt = srcSQLstmt + "a.rowid as " + PK 
					+ " from " + srcSch + "." + srcTbl + " a ";
			//setup the src select SQL statement
			sql = "update SYNC_TABLE set src_stmt0='" + srcSQLstmt + "'"
					+ " where tbl_id="+tblID;
			metaData.runRegSQL(sql);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		//json.put("repTblFldDML", sqlFields);
		//json.put("repDCCTbl", repDCCTbl);
		//json.put("repDCCTblFld", sqlFieldsDCC);
		//return json;
		return true;
	}
	@Override
	public boolean regSrcDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		String sql="CREATE TABLE " + jurl
				+ " (" + PK + " VARCHAR2(20),  DCC_TS DATE) TABLESPACE DCC_TS";
		if(runUpdateSQL(sql)==-1)
			return false;		
		sql =  "CREATE OR REPLACE TRIGGER " + dccPgm + " \n"  
				+ " AFTER  INSERT OR UPDATE OR DELETE ON " + srcSch+"."+srcTbl + "\n" 
				+ "  FOR EACH ROW\n" 
				+ "    BEGIN  INSERT INTO " + jurl + "(" + PK + ", DCC_TS )\n"  
				+ "     VALUES ( :new.rowid, sysdate   );  END;\n\n"  
				+ "alter trigger " + dccPgm + " disable;\n\n";
		if(runUpdateSQL(sql)==-1)
			return false;		
		return true;
	}
	@Override
	public boolean regDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		//not use for Oracle so far.
		return true;
	}
	@Override
	public boolean unregisterSrc(int tblID) {
		String sql =  "drop TRIGGER " + metaData.getTableDetails().get("src_dcc_pgm");
		runUpdateSQL(sql);		
		
		sql="drop TABLE " + metaData.getTableDetails().get("src_dcc_tbl");
		runUpdateSQL(sql);
		
		return true;
	}
	/**************** starting DCC (that is: enable trigger) *******************************/
	@Override
	public boolean beginDCC(){
	   Statement sqlStmt;
	   try {
		   sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		   String sql="alter trigger "  + metaData.getTableDetails().get("src_dcc_pgm").toString() + " enable";
		   sqlStmt.executeUpdate(sql);
		   sqlStmt.close();
	   } catch (SQLException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   }
		
	   logger.info("   trigger is enabled..");
		
	   return true;
	}
	
	/**************** internal helpers *********************/
	private int runUpdateSQL(String sql) {
		int rslt=0;
		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt=null; 
		try {
			stmt = dbConn.createStatement();
			rslt = stmt.executeUpdate(sql);
			stmt.close();
			dbConn.commit();
		} catch (SQLException e) {
			logger.error(e);
			rslt=-1;
		} 
		return rslt;
	}

   public void commit() {
      try {
		dbConn.commit();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   public void rollback()  {
      try {
		dbConn.rollback();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }

}