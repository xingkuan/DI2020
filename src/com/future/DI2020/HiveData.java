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

class HiveData extends JDBCData{
	public static final Logger logger = LogManager.getLogger();
	
	private Statement srcSQLStmt = null;
	private ResultSet srcRS = null;
	
   //public OracleData(String dbID) throws SQLException {
   public HiveData(JSONObject dbID) throws SQLException {
		super(dbID);
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
		String sql = "update " + metaData.getTaskDetails().get("src_dcc_tbl") + " set dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" ;

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
			case "1DATA":    //case: read the whole table
				pre.add(metaData.getBareSrcSQL() );
				jo.put("PRE", pre);
				break;
			case "2DATA":   //read the changed rows. Original O2V, O2K
			// Not needed as it is done in getDccCnt()	
			//	pre.add("update " + metaData.getTaskDetails().get("src_dcc_tbl") 
			//			+ " set dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" );
				pre.add(metaData.getBareSrcSQL() + ", " + metaData.getTaskDetails().get("src_dcc_tbl") 
						+ " b where b.dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') "
						+ " and a.rowid=b."+metaData.getTaskDetails().get("data_pk"));
				jo.put("PRE", pre);
				JSONArray aft = new JSONArray();
				aft.add("delete from " + metaData.getTaskDetails().get("src_dcc_tbl") 
						+ " where dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" );
				jo.put("AFT", aft);
				break;
		}
		
		return jo;
	}
	//public int crtSrcResultSet(int actId, JSONArray jaSQLs) {
	@Override
	public int crtSrcResultSet() {
		int rtc=0;
		String template = metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("template_id");

		JSONArray jaSQLs=(JSONArray) getSrcSqlStmts(template).get("PRE");
		String sql;
		for (int i = 0; i < jaSQLs.size()-1; i++) {
			sql = jaSQLs.get(i).toString();
			rtc = runUpdateSQL(sql);
		}
		sql=jaSQLs.get(jaSQLs.size()-1).toString();
		if( SQLtoResultSet(sql)<=0 ) {
			return -1;
		}
		return rtc;
	}

	public List<String> getDCCKeyList(){
		List<String> keyList=new ArrayList<String>();

	    Statement stmt1;
 	    ResultSet rs1;
		String sql1 = "select distinct orarid from " 
					+ metaData.getTaskDetails().get("src_dcc_tbl") 
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
		String template = metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("template_id");

		JSONArray jaSQLs=(JSONArray) getSrcSqlStmts(template).get("AFT");

		String sql;
		if(jaSQLs==null)
			return;
		for (int i = 0; i < jaSQLs.size(); i++) {
			sql = jaSQLs.get(i).toString();
			runUpdateSQL(sql);
		}
	}
   
	/******** Registration APIs **********/
	//registration APIs	   
	@Override
	public void disableTask(TaskMeta taskmeta) {
		JSONObject taskDetail = taskmeta.getTaskDetails();
		JSONArray  instrs = (JSONArray ) taskDetail.get("disInstrs");
		
		processInstructions(instrs);
	}

	private void processInstructions(JSONArray instrs) {
		instrs.iterator().forEachRemaining(element -> {
			String stmt = element.get("stmt");
			stmt = parseStmt(stmt);
			runUpdateSQL(instStmt);
	    });
		

	}
	
	@Override
	//for creating the needed objects in the Data point
	public boolean registTask(TaskMeta instructions) {
		JSONObject taskDetail = taskmeta.getTaskDetails();
		JSONArray  instrs = (JSONArray ) taskDetail.get("regInstrs");
		
		processInstructions(instrs);
		
		return true;
	}
	
	@Override
	//for creating the needed objects in the Data point
	public boolean unregistTask(TaskMeta instructions) {
		JSONObject taskDetail = taskmeta.getTaskDetails();
		JSONArray  instrs = (JSONArray ) taskDetail.get("unregInstrs");
		
		processInstructions(instrs);
		
		return true;
	}
	


	/**************** starting DCC (that is: enable trigger) *******************************/
	@Override
	public boolean beginDCC(){
	   Statement sqlStmt;
	   try {
		   sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		   String sql="alter trigger "  + metaData.getTaskDetails().get("src_dcc_pgm").toString() + " enable";
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
	private boolean executeSQL(String sql) {
		boolean rslt;
		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt=null; 
		try {
			stmt = dbConn.createStatement();
			rslt = stmt.execute(sql);
			stmt.close();
			dbConn.commit();
		} catch (SQLException e) {
			logger.error(e);
			rslt=false;
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