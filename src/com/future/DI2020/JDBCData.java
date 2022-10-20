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

//import com.vertica.jdbc.VerticaConnection;

import org.apache.logging.log4j.LogManager;


class JDBCData extends DataPoint{
	protected Connection dbConn;
	
	protected Properties props = new Properties();
	
	protected Statement srcSQLStmt = null;
	protected ResultSet srcRS = null;

	protected JSONObject srcInstr = null, tgtInstr = null;

	public JDBCData(JSONObject jo) throws SQLException {
		super(jo);
		connectDB();  
	}
	
	public void prep(JSONObject instruction) {
		srcInstr = (JSONObject) instruction.get("srcInstr");
		tgtInstr = (JSONObject) instruction.get("tgtInstr");
	}


	private void connectDB() {
		if(dbCat.equals("JDBC")){
			try {
				Class.forName(driver); 
			} catch(ClassNotFoundException e){
				logger.error("   Driver error has occured");
				logger.error( e);
			}
      
			try {
				props.put("user", userID);
				props.put("passWord", passPWD);
				props.put("DirectBatchInsert", true);
				props.put("db", true);
				props.put("schema", true);
				//dbConn.setProperty("DirectBatchInsert", true);
				//dbConn = DriverManager.getConnection(urlString, userID, passPWD, props);
				dbConn = DriverManager.getConnection(urlString, props);
				dbConn.setAutoCommit(false);
			} catch(SQLException e) {
				logger.error("   cannot connect to db");
				logger.error(e);
			}
		}else {
				logger.info("   If you never see this!");
		}
	}
	
	public void clearData() {
		try {
			srcSQLStmt.close();
			srcRS.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		srcInstr = null; 
		tgtInstr = null;
	}

	protected void closeDB() {
		try {
			dbConn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	int batchSize = Integer.parseInt(conf.getConf("batchSize"));
	int[] batchDel = null;
	int[] batchIns = null;
	String[] syncRowIDs;

	int fldInx;
	ArrayList<Integer> syncFldType;
	ArrayList<String> syncFldNames;
	PreparedStatement syncInsStmt;

	/**********************Synch APIs************************************/	
	
	@Override
	public int sync(DataPoint srcData) {
		ResultSet delRS, instRS;
		String delStmt = (String) tgtInstr.get("DELETE");
		String qryStmt = (String) tgtInstr.get("QUERY");
		delRS = srcData.getRS(delStmt);
		instRS = srcData.getRS(qryStmt); //TODO: that depends. It could come from Kafka!
		
		deleteRS(delRS);
		writeRS(instRS);
		
		return 0;
	}
	private int deleteRS(ResultSet rs) {
		String delSQL = (String) tgtInstr.get("deleteInstrunction");
		//...
		return 0;
	}
	private int writeRS(ResultSet rs) {
		Object o;
		int currSyncCnt=0;
		
		try {
			while(srcRS.next()) {
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
								//logErrorROWID(syncRowIDs[i]);
								totalErrCnt++;
							}
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						logger.error(e);
					}
					//go to the next loop
					continue;
				}
				
				//last batch
				batchIns = syncInsStmt.executeBatch();
				//commit();  //to be called at the end of sync
				//rtc = 2; //TODO
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);;
		}

		
		return 0;
	}

/*		
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

*/
@Override
public ResultSet getRS(String sql) {
	ResultSet rs=null;
	int rv;

	try {
		srcSQLStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		rs = srcSQLStmt.executeQuery(sql);
		if (srcRS.isBeforeFirst()) {// this check can throw exception, and do the needed below.
			rv=1;
			logger.info("   src recordset ready.");
		}
	} catch (SQLException e) {
		logger.error("   " + e);
		rv = -1;
	}
	
	return rs;
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

	//---------audit APIs
	@Override
	public int getRecordCount(){
		int rtv;
		Statement sqlStmt;
		ResultSet sqlRset;
		int i;

		String sql="select count(1) from " + tblName;
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
	
	//registration APIs	   

	public JSONObject runDBcmd(String sqlStr, String type) {
		// TODO Auto-generated method stub
		return null;
	}
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


	public String getAVRO(String selectStmt) {
		Statement lrepStmt;
		ResultSet lrRset;
		int i;

		String avroSchema = "{\"namespace\": \"com.future.DI2020.avro\", \n" 
				    + "\"type\": \"record\", \n" 
				    + "\"name\": \"" + tblName + "\", \n" 
				    + "\"fields\": [ \n" ;
		try {
			lrepStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			i = 0;
			lrRset = lrepStmt.executeQuery(selectStmt);
			ResultSetMetaData rsmd = lrRset.getMetaData();
			int columnCount = rsmd.getColumnCount();
			String colName;
			String colType;
		/*
Array: 2003
Big int: -5
Binary: -2
Bit: -7
Blob: 2004
Boolean: 16
Char: 1
Clob: 2005
Date: 91
Datalink70
Decimal: 3
Distinct: 2001
Double: 8
Float: 6
Integer: 4
JavaObject: 2000
Long var char: -16
Nchar: -15
NClob: 2011
Varchar: 12
VarBinary: -3
Tiny int: -6
Time stamt with time zone: 2014
Timestamp: 93
Time: 92
Struct: 2002
SqlXml: 2009
Smallint: 5
Rowid: -8
Refcursor: 2012
Ref: 2006
Real: 7
Nvarchar: -9
Numeric: 2
Null: 0
Smallint: 5
 */
			for (i = 1; i <= columnCount; i++ ) {
				colName = rsmd.getColumnName(i);
				colType = rsmd.getColumnTypeName(i);
	
				avroSchema = avroSchema 
						+ "{\"name\": \"" + colName + "\", " + colType + "} \n" ;
				i++;
			}
	
			lrRset.close();
			lrepStmt.close();
			
			avroSchema = avroSchema 
				+ "] }";

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return avroSchema;
	}

	public String getSrcSTMT(String bareSQL) {
		Statement sqlStmt;
		ResultSet sqlRset;
		  try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = sqlStmt.executeQuery(bareSQL);

	      //Retrieving the ResultSetMetaData object
	      ResultSetMetaData rsmd = rs.getMetaData();

	      //getting the column type
	      int column_size = rsmd.getPrecision(3);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	      
		return null;
	}

	public String getTgtDDL(String bareSQL) {
		// TODO Auto-generated method stub
		return null;
	}


/*
	protected void logErrorROWID(String rowid) {
		try {
			// . FileWriter fstream = new FileWriter(metaData.getInitLogDir() + "/" +
			// metaData.getTgtSchema() + "." + metaData.getTgtTable() + ".row", true);
			FileWriter fstream = new FileWriter(
				logDir + metaData.getTaskDetails().get("tgt_sch").toString() + "." + metaData.getTaskDetails().get("tgt_tbl").toString()  + ".row", true);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(rowid + "\n");
			out.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
*/

}