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

import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

/*
 class Meta
 
 Meta, singleton class of replication metat data, stored in RDBMS
*/

class MetaData {
	
	private String jobID;
	Timestamp tsThisRefesh;
	
   private  Connection repConn;
   private  Statement repStmt;

   // for now, auxilary is Kafka when replicating from DB2 to Vertica.
   private String auxSchema;
   private String auxTable;

   //encapsulate the details into tblDetailJSON;
	private JSONObject tblDetailJSON;
	//
	private  ResultSet rRset;
   private int tableID;
   private int currState=0;
   private int fldCnt;
   private int refreshCnt;
   private int[] xformType;
   private String[] fldName;
   private String sqlSelectSource;
   private String sqlInsertTarget;
   private String sqlInsertTargetAlt;
   private String sqlCopyTarget;
   private String sqlCopySource;
   private String srcSchema;
   private String srcTable;
   private String tgtSchema;
   private String tgtTable;
   private String tgtTableAlt;
   private String srcTrigger;
   private String srcLogTable;
   private String tgtPK;
   private String oraDelimiter;
   private String vrtDelimiter;
   private int defInitType;
   private int refreshType;
   private int recordCountThreshold;  
   private int minPollInterval; 
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
   
   //private static DBinfo repoDB;
   private JSONObject srcDBDetail;
   private JSONObject tgtDBDetail;
   private JSONObject auxDBDetail;
   private JSONObject tblDetail;

   
   private static MetaData instance = null;  // use lazy instantiation ;
   
   public static MetaData getInstance() {
       if(instance == null) {
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
	      } catch(ClassNotFoundException e){
	       	ovLogger.error("DB Driver error has occured");
	       	ovLogger.error(e);
	      }

	      try {
	         repConn = DriverManager.getConnection(url, uID, uPW);
	         repConn.setAutoCommit(false);
	         repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
	      } catch(SQLException e) {
	    	  ovLogger.error(e.getMessage());
	      }
	   }

   public void setupForJob(String jID, int tblID) {
	   jobID=jID;
	   tableID=tblID;
	   readTableDetails();
	   
	   try {
		srcDBDetail = readDBDetails(tblDetail.get("src_dbid").toString());
		   tgtDBDetail = readDBDetails(tblDetail.get("tgt_dbid").toString());
		   auxDBDetail = readDBDetails(tblDetail.get("aux_dbid").toString());
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }

   //return db details as a simpleJSON object, (instead of a Java object, which is too cumbersome).
   public JSONObject readDBDetails(String dbid) throws SQLException {
      
      JSONObject jo = new JSONObject();
      Statement stmt=null;
      ResultSet rset=null;

      try {
    	    stmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
         rset = stmt.executeQuery("select db_id, db_cat, db_type, db_conn, db_driver, db_usr, db_pwd "
         		+ " from meta_db "  
         		+ " where db_id='" + dbid + "'");   
         jo =  ResultSetToJsonMapper(rset);
       } catch(SQLException e) {
         ovLogger.error(e.getMessage());
      }finally {
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
   //return Table details as a simpleJSON object, (instead of a Java object, which is too cumbersome).
   private JSONObject readTableDetails() {
	  boolean rtv = true;
      java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis()); 
      java.sql.Timestamp ats;
      long ets;
      
      JSONObject jo = new JSONObject();
      //JSONArray jArray = new JSONArray();

      try {
         rRset = repStmt.executeQuery("select SOURCE_SCHEMA, TARGET_SCHEMA, SOURCE_TABLE, TARGET_TABLE, TS_LAST_REF400, SEQ_LAST_REF, "
         		+ "	TS_LAST_AUDIT, AUDIT_EXP, POOL_ID, REFRESH_TYPE, MIN_POLL_INTERVAL, REC_CNT_THRESHOLD, TARGET_TABLE_ALT, "
        	    + "	TS_LAST_INIT,  CURR_STATE, TABLE_WEIGHT, LAST_REFRESH_DURATION, LAST_INIT_DURATION, REFRESH_CNT,  "
         		+ "	AUD_SOURCE_RECORD_CNT, AUD_TARGET_RECORD_CNT, TABLE_ID, SOURCE_TRIGGER_NAME, T_ORDER, ORA_DELIMITER, "
         		+ "	EXP_TYPE, LINESIZE, EXP_TIMEOUT, VERT_DELIMITER, PARTITIONED, PART_BY_CLAUSE, SOURCE_LOG_TABLE, TARGET_PK, "
         		+ "	HEARTBEAT, LAST_INIT_TYPE, DEFAULT_INIT_TYPE, SOURCE_DB_ID, TARGET_DB_ID "
         		+ " from vertsnap.sync_table "  
         		+ " where table_id=" + tableID);   
         jo =  ResultSetToJsonMapper(rRset);
       } catch(SQLException e) {
         ovLogger.error(e.getMessage());
      }
      return jo;
   }
   private JSONObject ResultSetToJsonMapper(ResultSet rs) throws SQLException {
           //JSONArray jArray = new JSONArray();
           JSONObject jsonObject = null;
           
           ResultSetMetaData rsmd = rs.getMetaData();
           int columnCount = rsmd.getColumnCount();
           
           rs.next();
   	        jsonObject = new JSONObject();
   	        for (int index = 1; index <= columnCount; index++) 
   	        {
   	            String column = rsmd.getColumnName(index);
   	            Object value = rs.getObject(column);
   	            if (value == null) 
   	            {
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
	   label="Inject Kafka DBid: " + dbID;
	   
	   journalLib = jLib;
	   journalName = jName;

	   return true;
   }

   
   private void getFieldMetaData() throws SQLException {
      // creates select and insert strings 
      Statement lrepStmt;
      ResultSet lrRset;
      int i;

      lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      
      // get field count
      lrRset = lrepStmt.executeQuery("select count(*) from vertsnap.sync_table_field where table_id=" + tableID);
      //System.out.println("select count(*) from " + OVSTableField + " where table_id=" + tableID);
      if (lrRset.next()) {
         fldCnt=lrRset.getInt(1);
      }
      lrRset.close();

      // get field info
      xformType = new int[fldCnt];
      fldName = new String[fldCnt];
      //System.out.println("field cnt: " + fldCnt);
      i=0;
      lrRset = lrepStmt.executeQuery("select xform_fctn, target_field, source_field, xform_type  from vertsnap.sync_table_field " 
    		  + " where table_id=" + tableID 
    		  + " order by field_id");
      if (lrRset.next()) {                                                                  
         sqlSelectSource = "select \n"    + lrRset.getString("source_field");      
         sqlInsertTarget = "insert into " + tgtSchema + "." + tgtTable ;
         sqlInsertTargetAlt = "insert into " + tgtSchema + "." + tgtTableAlt ;
         sqlInsertTarget +=            "\n( "          + "\"" + lrRset.getString("target_field") + "\"";
         sqlInsertTargetAlt +=            "\n( "       + "\"" + lrRset.getString("target_field") + "\"";
         xformType[i] = lrRset.getInt("xform_type");
         fldName[i] = lrRset.getString("target_field");
         sqlCopySource = "select \n" + lrRset.getString("xform_fctn");
         sqlCopyTarget = "copy " + tgtSchema + "." + tgtTable + " (" +  "\"" + lrRset.getString("target_field") + "\"";
      }
      while (lrRset.next()) {
         sqlSelectSource += " \n , " + lrRset.getString("source_field") ;    
         sqlInsertTarget += " \n , " + "\"" + lrRset.getString("target_field") + "\"";   
         sqlInsertTargetAlt += " \n , " + "\"" + lrRset.getString("target_field") + "\""; 
         sqlCopySource += " || " + oraDelimiter + " || \n" + lrRset.getString("xform_fctn");
         sqlCopyTarget += ", " + "\"" + lrRset.getString("target_field") + "\"";
         i++;
         //System.out.println(i);
         xformType[i] = lrRset.getInt("xform_type");
         fldName[i] = lrRset.getString("target_field");
      }
      lrRset.close();
      sqlCopyTarget += ") FROM LOCAL STDIN DELIMITER " + vrtDelimiter + "DIRECT ENFORCELENGTH";
      sqlCopySource += "\nfrom " + srcSchema + "." + srcTable + " a";
      sqlSelectSource += " \n from " + srcSchema + "." + srcTable + " a";
      sqlInsertTarget += ") \n    Values ( " ;
      sqlInsertTargetAlt += ") \n    Values ( " ;
      for ( i=1; i<=fldCnt; i++ ) {
         if (i==1) {
            sqlInsertTarget += "?";
            sqlInsertTargetAlt += "?";
         } else {
            sqlInsertTarget += ",?";
            sqlInsertTargetAlt += ",?";
         }
      }
      sqlInsertTarget += ") ";
      sqlInsertTargetAlt += ") ";
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
	  int duration =  (int) (endMS - startMS)/1000;
      ovLogger.info(label + " duration: " + duration + " seconds");
      
      //Save to InfluxDB:
      metrix.sendMX("initDuration,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+duration+"\n");
      metrix.sendMX("initRows,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+refreshCnt+"\n");
      metrix.sendMX("JurnalSeq,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+seqThisRef+"\n");

      //Save to MetaRep:
      java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis()); 
      try {                     
         rRset.updateInt("LAST_INIT_DURATION", (int)((endMS - startMS)/1000));
         rRset.updateTimestamp("TS_LAST_INIT",tsThisRefresh);
         rRset.updateTimestamp("TS_LAST_REF400",tsThisRefresh);
         rRset.updateLong("SEQ_LAST_REF",seqThisRef);
         rRset.updateTimestamp("TS_LAST_AUDIT",ts);
         rRset.updateInt("AUD_SOURCE_RECORD_CNT",refreshCnt);     
         rRset.updateInt("AUD_TARGET_RECORD_CNT",refreshCnt);

         rRset.updateRow();
         repConn.commit();   
      } catch (SQLException e) {
         ovLogger.error(label + e.getMessage());
      }
   }
   
   public void saveRefreshStats(String jobID) {
	   int duration = (int)(int)((endMS - startMS)/1000);

	   metrix.sendMX("syncDuration,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+duration+"\n");
	   metrix.sendMX("syncCount,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+refreshCnt+"\n");
	   metrix.sendMX("JurnalSeq,jobId="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value="+seqThisRef+"\n");

      try {                     
         rRset.updateInt("LAST_REFRESH_DURATION", duration);
         //2020.02.21
         //rRset.updateTimestamp("TS_LAST_REFRESH",hostTS);
         rRset.updateTimestamp("TS_LAST_REF400",tsThisRefresh);
         rRset.updateLong("SEQ_LAST_REF",seqThisRef);
         rRset.updateInt("REFRESH_CNT",refreshCnt);
        
         rRset.updateRow();
         repConn.commit();   
      } catch (SQLException e) {
         System.out.println(e.getMessage());
      }
   }

   public void saveAudit(int srcRC, int tgtRC) {
      java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis()); 
      ovLogger.info(label + "source record count: " + srcRC + "     target record count: " + tgtRC );
//TODO: matrix
      try {                     
         rRset.updateTimestamp("TS_LAST_AUDIT",ts);
         rRset.updateInt("AUD_SOURCE_RECORD_CNT",srcRC);
         rRset.updateInt("AUD_TARGET_RECORD_CNT",tgtRC);
         rRset.updateRow();
         repConn.commit();   
         //System.out.println("audit info saved");
      } catch (SQLException e) {
         ovLogger.error(label + e.getMessage());
      }
   }
  
   
   public void setRefreshTS(Timestamp thisRefreshHostTS) {
		tsThisRefresh = thisRefreshHostTS;
	}
	public void setRefreshSeqThis(long thisRefreshSeq) {
		if(thisRefreshSeq>0) {
			seqThisRef=thisRefreshSeq;
		}else {
			seqThisRef=seqLastRef;
			ovLogger.info("...hmm, got a 0 for SEQ# for srcTbl " + srcTable + ". The last one: " + seqLastRef);
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
   public int getRefreshType() {
      return refreshType;
   }
   public int getRecordCountThreshold() {
      return recordCountThreshold;
   }
   public int getMinPollInterval() {
      return minPollInterval;
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
   public String getLogTable() {
      return srcLogTable;
   }
   public String getSrcTrigger() {
      return srcTrigger;
   }
   
    public String getSrcTblAb7() {
	   return srcTblAb7;
   }
   public String getTgtTable() {
      return tgtTable;
   }
   public String getTgtTableAlt() {
      return tgtTableAlt;
   }
   public String getSQLInsert() {
      return sqlInsertTarget;
   }
   public String getSQLInsertAlt() {
      return sqlInsertTargetAlt;
   }
   public void setTgtUseAlt() {
      tgtUseAlt=true;
   }
   public boolean getTgtUseAlt() {
      return tgtUseAlt;
   }
   public String getSQLSelect() {
      return sqlSelectSource;
   }
   public String getSQLWhereClause() {
	      return sqlWhereClause;
	   }
   public String getSQLCopySource() {
      return sqlCopySource;
   }
   public String getSQLCopyTarget() {
      return sqlCopyTarget;
   }
   public String getPK() {
      return tgtPK;
   }
   public String getSQLSelectXForm() {
      return sqlSelectSource;
   }
   public int getCurrState() {
      return currState;
   }
   public void setCurrentState(int cs) {
      try {
         currState=cs;
         rRset.updateInt("CURR_STATE",cs);
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
   public int getDefInitType() {
      return defInitType;
   }

   public int getFldCnt() {
      return fldCnt;
   }
   public int getFldType(int i) {
      return xformType[i];
   }  
   public String getFldName(int i) {
      return fldName[i];
   }  

   public void setRefreshCnt(int i) {
      refreshCnt=i;
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
	      
      strSQL = "select source_schema||'.'||source_table from sync_table where source_db_id = " + dbID + " and source_log_table='" + journal + "' order by 1";
	      
       // This shortterm solution is only for Oracle databases (as the source)
	   try {
		   lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
           lrRset = lrepStmt.executeQuery(strSQL);
           while(lrRset.next()){
               //Retrieve by column name
               String tbl  = lrRset.getString(1);
               tList.add(tbl);
            }
        } catch(SQLException se){
           ovLogger.error("OJDBC driver error has occured" + se);
        }catch(Exception e){
            //Handle errors for Class.forName
        	ovLogger.error(e);
         }finally {
        	// make sure the resources are closed:
        	 try{
        		if(lrepStmt !=null)
        			lrepStmt.close();
        	 }catch(SQLException se2){
        	 }
             try{
                 if(repConn!=null)
                    repConn.close();
             }catch(SQLException se){
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
       
	   if(poolID < 0)
		   strSQL = "select TABLE_ID,CURR_STATE from sync_table order by t_order";
	   else
	      strSQL = "select TABLE_ID,CURR_STATE from sync_table where pool_id = " + poolID + " order by t_order";
	      
       // This shortterm solution is only for Oracle databases (as the source)
	   try {
		   lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
           lrRset = lrepStmt.executeQuery(strSQL);
           while(lrRset.next()){
               //Retrieve by column name
               int id  = lrRset.getInt("TABLE_ID");
               tList.add(id);
            }
        } catch(SQLException se){
           ovLogger.error("OJDBC driver error has occured" + se);
        }catch(Exception e){
            //Handle errors for Class.forName
        	ovLogger.error(e);
         }finally {
        	// make sure the resources are closed:
        	 try{
        		if(lrepStmt !=null)
        			lrepStmt.close();
        	 }catch(SQLException se2){
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

	   strSQL = "select source_db_id||'.'||source_log_table from sync_journal400 where pool_id = " + poolID ;
	      
    // This shortterm solution is only for Oracle databases (as the source)
	   try {
        lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        lrRset = repStmt.executeQuery(strSQL);
        while(lrRset.next()){
            //Retrieve by column name
            String jName  = lrRset.getString(1);
            jList.add(jName);
         }
     } catch(SQLException se){
        ovLogger.error("OJDBC driver error has occured" + se);
     }catch(Exception e){
         //Handle errors for Class.forName
     	ovLogger.error(e);
      }finally {
     	// make sure the resources are closed:
     	 try{
     		if(lrepStmt !=null)
     			lrepStmt.close();
     	 }catch(SQLException se2){
     	 }
      }
	   
	   return jList;
}  
   
   

public JSONObject getLogDetails() {
	JSONObject jo=null;
    try {
       repStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
       rRset = repStmt.executeQuery("select SOURCE_DB_ID, SOURCE_LOG_TABLE, SEQ_LAST_REF, TS_LAST_REF400 "
       		+ " from vertsnap.sync_journal400 " 
       		+ " where SOURCE_DB_ID=" + srcDBid + " and SOURCE_LOG_TABLE='" + srcLogTable +"'");   
       rRset.next();
       jo =  ResultSetToJsonMapper(rRset);
       rRset.close();
       
    } catch(SQLException e) {
       ovLogger.error(e.getMessage());

    }
return jo;
 }

public void saveReplicateDB2() {
    int duration = (int)(int)((endMS - startMS)/1000);
    try {                     
       rRset.updateTimestamp("TS_LAST_REF400",tsThisRefresh);
       rRset.updateLong("SEQ_LAST_REF",seqThisRef);
      
       rRset.updateRow();
       repConn.commit();   
    } catch (SQLException e) {
       System.out.println(e.getMessage());
    }
	   metrix.sendMX("duration,jobId="+label+" value="+duration+"\n");
	   metrix.sendMX("Seq#,jobId="+label+" value="+seqThisRef+"\n");
 }


// ... move to MetaData ?
public void setThisRefreshHostTS(){
	   tsThisRefesh = new Timestamp(System.currentTimeMillis());
}



int totalDelCnt, totalInsCnt, totalErrCnt;
public void sendMetrix() {
     metrix.sendMX("delCnt,metaData.getJobID()="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + totalDelCnt + "\n");
     metrix.sendMX("insCnt,metaData.getJobID()="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + totalInsCnt + "\n");
     metrix.sendMX("errCnt,metaData.getJobID()="+jobID+",tblID="+srcTblAb7+"~"+tableID+" value=" + totalErrCnt + "\n");
}
public void setTotalDelCnt(int v) {
	totalDelCnt=v;
}
public void setTotalInsCnt(int v) {
	totalInsCnt=v;
}
public void setTotalErrCnt(int v) {
	totalErrCnt=v;
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
	     tblID=rRset.getInt(1);      
      rRset.close();
      repStmt.close();
      repConn.close();
   } catch(SQLException e) {
 	  ovLogger.error(e.getMessage());
   }

	   return tblID+1;
}

public boolean isNewTblID(int tblID) {
	   Statement lrepStmt = null;
	   ResultSet lrRset;
 String strSQL;
 boolean rslt = true;
 
 strSQL = "select TABLE_ID from sync_table where table_id = " + tblID;
	      
	   try {
     lrepStmt = repConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
     lrRset = repStmt.executeQuery(strSQL);
     if(lrRset.next()){
     	rslt = false;
      }
  } catch(SQLException se){
     ovLogger.error(se);
  }catch(Exception e){
      //Handle errors for Class.forName
  	ovLogger.error(e);
   }finally {
  	// make sure the resources are closed:
  	 try{
  		if(lrepStmt !=null)
  			lrepStmt.close();
  	 }catch(SQLException se2){
  	 }
   }
	   
	return rslt;
}

   
}    