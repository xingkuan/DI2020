package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class OracleData extends DataPointer{
   private int tableID;

   private Connection dbConn;
   private boolean dbConnOpen;

   private boolean recover=false;
   private int currState=0;
   private boolean isError=false;
   private int fldCnt;
   private int[] xformType;
   private String[] xformFctn;
   private int logCnt;
   private String label;

   private java.sql.Timestamp tsThisRefesh=null;

   
   private static final Logger ovLogger = LogManager.getLogger();

   public OracleData(String dbID) throws SQLException {
		super(dbID);
   }
   
   public ResultSet getAuxResultSet(String rrns){
	   Statement sqlStmt;
	   ResultSet sqlRset=null;

	   String whereClause;

	   if(rrns.equals("")) {      //empty where clause is used only for initializing a table
		   whereClause = "";
	   }else{    //otherwise, will be a list of RRN like "1,2,3"
		   whereClause = " where rrn(a) in (" + rrns + ")";
	   }
	   
      String sqlStr = metaData.getSQLSelSrc() + " " + whereClause;
      try {
   	   sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
       sqlRset=sqlStmt.executeQuery(sqlStr);
       //TODO: decide where to call the following:
       //sqlRset.close();
       //sqlStmt.close();
      } catch(SQLException e) {
         ovLogger.error("   recordset not created");
         ovLogger.error(e);
         ovLogger.error(" \n\n\n" + sqlStr + "\n\n\n");
      }
      
      return sqlRset;
   }
   public int getThreshLogCount() {
      int lc=0;
	   Statement sqlStmt;
      ResultSet sqlRset;
      
      try {
      	   sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    	  sqlRset  = sqlStmt.executeQuery( " select count(distinct M_ROW)   from   "  
      	       + metaData.getTableDetails().get("src_sch").toString() + "." +  metaData.getTableDetails().get("src_tbl").toString() );
    	  sqlRset.next();
         lc = Integer.parseInt(sqlRset.getString(1));
         sqlRset.close();
         sqlStmt.close();
      } catch(SQLException e) {
         ovLogger.error("   error during threshlogcnt");
      }
      ovLogger.info("   theshold log count: " + lc);
      return lc;
   }

   
   public int getRecordCount(){
      int rtv;
	   Statement sqlStmt;
	      ResultSet sqlRset;
      int i;

      rtv=0;
      try {
    	  sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    	  sqlRset=sqlStmt.executeQuery("select count(*) from " + metaData.getTableDetails().get("src_sch").toString() 
    			  		+ "." + metaData.getTableDetails().get("src_tbl").toString());
    	  sqlRset.next();
            rtv = Integer.parseInt(sqlRset.getString(1));  
            sqlRset.close();
         sqlStmt.close();
      } catch(SQLException e) {
         ovLogger.error("   running sql: "+ e); 
      }
      return rtv;
   }
   public void setTriggerOn() throws SQLException {
	   Statement sqlStmt;
 	  sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	      sqlStmt.executeUpdate("alter trigger "  + metaData.getTableDetails().get("aux_pgm_name").toString() + " enable");    
   }

   public void commit() throws SQLException {
      dbConn.commit();
   }
   public void rollback() throws SQLException {
      dbConn.rollback();
   }

}