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

import com.vertica.jdbc.VerticaConnection;

import org.apache.logging.log4j.LogManager;

class JDBCData extends DataPoint{
	protected Connection dbConn;

	protected Statement srcSQLStmt = null;
	protected ResultSet srcRS = null;

	public JDBCData(JSONObject jo, String role) throws SQLException {
		super(jo, role);
		connectDB();  
	}

	private void connectDB() {
		if(dbCat.equals("RDBMS")){
			try {
				Class.forName(driver); 
			} catch(ClassNotFoundException e){
				logger.error("   Driver error has occured");
				logger.error( e);
			}
      
			try {
				dbConn = DriverManager.getConnection(urlString, userID, passPWD);
				dbConn.setAutoCommit(false);
			} catch(SQLException e) {
				logger.error("   cannot connect to db");
				logger.error(e);
			}
		}else {
				logger.info("   If you never see this!");
		}
	}
	
	int batchSize = Integer.parseInt(conf.getConf("batchSize"));
	int[] batchDel = null;
	int[] batchIns = null;
	String[] syncRowIDs;
	int totalSyncCnt, currSyncCnt;
	int fldInx;
	ArrayList<Integer> syncFldType;
	ArrayList<String> syncFldNames;
	PreparedStatement syncInsStmt;

	/**********************Synch APIs************************************/	
	@Override
	public void copyTo(DataPoint tgt) {
		try {
			while(srcRS.next()) {
				tgt.write(srcRS);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);;
		}
		//last batch and whatever
		tgt.write();
	}
	@Override
	public void copyToVia(DataPoint tgtData, DataPoint auxData) {
		int rtc = 2;
		List<String> keys = auxData.getDCCKeyList();

		if(keys.size()>0) {
			//Thread 1: batch delete the records in this target
			Runnable srcTask = () -> { 
				tgtData.dropStaleRowsOfList(keys);
				};
			Thread srcThread=new Thread(srcTask);
			srcThread.start();
			
			//main thread: srcData to select data from the list
			crtSrcResultSet(keys);
			//wait till thread 1 and do batch insert:
			try {
				srcThread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//now the source is ready, the tgt is cleaned:
			copyTo(tgtData);

		}else {
			logger.info("   No changes!");
		}
		//return rtc;
	}
	@Override
	public void write(ResultSet rs) {
		Object o;
		try {
			for (fldInx = 1; fldInx < syncFldType.size(); fldInx++) {  //The last column is the internal record key.
														   //for Oracle ROWID, is a special type, let's treat all as String
														   //for uniformity, so are the others. let's see if that is okay.
				o= rs.getObject(fldInx);
				syncInsStmt.setObject(fldInx, rs.getObject(fldInx));
				//syncInsStmt.setString(syncFldType.size(), rs.getString(syncFldType.size()));  //this two line assumes the last field is
			}
			//the last field (ORAID) need to be casted to String
			syncInsStmt.setString(syncFldType.size(), rs.getString(syncFldType.size()));
			syncInsStmt.addBatch();
			syncRowIDs[currSyncCnt] = rs.getString(syncFldType.size()); //record the PK. Assumed it is always the last one;
																		//   May not a good idea.TODO.
		} catch (SQLException e) {
			//	logger.error("e");
			//	logger.error("    rowid: " + rs.getString(metaData.getPK()));
			//	logger.error("    fieldno: " + i + "  " + syncFldNames.get(i));
			//	rtc = -1;
			logger.error(e);
		}

		// insert batch into target table
		totalSynCnt++;
		currSyncCnt++;

		if (currSyncCnt == batchSize) {
			try {
				batchIns = syncInsStmt.executeBatch();
				currSyncCnt = 0;
				logger.info("   addied batch - " + totalSynCnt);
			} catch (BatchUpdateException e) {
				logger.error("   Batch Error... ");
				logger.error(e);
				for (int i = 1; i <= syncFldType.size(); i++) {
					try {
						logger.error("   " + rs.getString(i));
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						logger.error(e1);
					}
				}
				//int[] iii;
				//iii = e.getUpdateCounts();
				for (int i = 0; i < batchSize; i++) {
					if (batchIns[i] == Statement.EXECUTE_FAILED) {
						logger.info("   " +  syncRowIDs[i]);
						putROWID(syncRowIDs[i]);
						totalErrCnt++;
					}
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e);
			}
		}
	}
	@Override
	public void write() {  // take care of the last batch
		try {
			batchIns = syncInsStmt.executeBatch();
			//commit();  //to be called at the end of sync
			//rtc = 2; //TODO
		} catch (BatchUpdateException e) {
			logger.error("   Error... rolling back");
			logger.error(e.getMessage());
			for (int i = 0; i < batchSize; i++) {
				if (batchIns[i] == Statement.EXECUTE_FAILED) {
					logger.info("   " +  syncRowIDs[i]);
					putROWID(syncRowIDs[i]);
					totalErrCnt++;
				}
			}
		} catch (SQLException e) {
			//rtc = -1; TODO
			// rollback();  //to be called at the end of sync
			logger.error(e.getMessage());
		}
	}

	/*************************************************************************/	
	protected int SQLtoResultSet(String sql) {
		int rv=0;
		try {
			srcSQLStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			srcRS = srcSQLStmt.executeQuery(sql);
			if (srcRS.isBeforeFirst()) {// this check can throw exception, and do the needed below.
				rv=1;
				logger.info("   src recordset ready.");
			}
		} catch (SQLException e) {
			logger.error("   " + e);
			rv = -1;
		}
		//logger.info("   opened src recordset.");
		return rv;
	}

	protected void putROWID(String rowid) {
		try {
			// . FileWriter fstream = new FileWriter(metaData.getInitLogDir() + "/" +
			// metaData.getTgtSchema() + "." + metaData.getTgtTable() + ".row", true);
			FileWriter fstream = new FileWriter(
				logDir + metaData.getTableDetails().get("tgt_sch").toString() + "." + metaData.getTableDetails().get("tgt_tbl").toString()  + ".row", true);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(rowid + "\n");
			out.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	//---------audit APIs
	@Override
	public int getRecordCount(){
		int rtv;
		Statement sqlStmt;
		ResultSet sqlRset;
		int i;

		String sql;
		if(dbRole.equals("SRC")) {
		  sql="select count(*) from " + metaData.getTableDetails().get("src_schema").toString() 
		  		+ "." + metaData.getTableDetails().get("src_table").toString();
		}else if(dbRole.equals("TGT")) {
		  sql="select count(*) from " + metaData.getTableDetails().get("tgt_schema").toString() 
		  		+ "." + metaData.getTableDetails().get("tgt_table").toString();
		}else {
		  logger.error("invalid DB role assignment.");
		  return -1;
		}
			      
		rtv=0;
		try {
		  sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		  sqlRset=sqlStmt.executeQuery(sql);
		  sqlRset.next();
		  rtv = Integer.parseInt(sqlRset.getString(1));  
		  sqlRset.close();
		  sqlStmt.close();
		} catch(SQLException e) {
		  logger.error("   running sql: "+ e); 
		}
		return rtv;
	}

}