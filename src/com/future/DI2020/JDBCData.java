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
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

//import com.vertica.jdbc.VerticaConnection;

import org.apache.logging.log4j.LogManager;


class JDBCData extends DataPoint{
	protected Connection dbConn;
	protected Properties props = new Properties();
	
	protected Statement srcSQLStmt = null;
	protected ResultSet srcRS = null;

	public JDBCData(JSONObject jo) throws SQLException {
		super(jo);
		connectDB();  
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
	}

	public void closeDB() {
		try {
			dbConn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	/**********************Synch APIs************************************/	
	int batchSize = Integer.parseInt(conf.getConf("batchSize"));
	int[] batchDel = null;
	int[] batchIns = null;
	String[] syncRowIDs;

	int fldInx;

	//PreparedStatement syncInsStmt;
	private PreparedStatement prepStmt;


	@Override
	public void prepareBatchStmt(String stmt) { //"DELETE from EMPLOYEE where ID=? "
		try {
			prepStmt = dbConn.prepareStatement(stmt);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public JSONObject syncTo(DataPoint tgt) {
		//tgt could be kafka, or ES, or other JDBC
		ResultSet delRS, instRS;

		Timestamp currTS;

		final TaskMeta taskMeta = TaskMeta.getInstance();

		String delStmt = (String)(((JSONObject)taskMeta.getTaskDetails().get("tgtDetail")).get("DELETE"));
		String insStmt = (String)(((JSONObject)taskMeta.getTaskDetails().get("tgtDetail")).get("INSERT"));

		String qryStmt = (String)(((JSONObject)taskMeta.getTaskDetails().get("srcDetail")).get("QUERY"));
		/*TODO getting qryStmt can complicated:
		 * 1. for using trigger only: filter will be a clause of "where id in (select ....
		 * 2. for ids to be provided by, e.g. kafka, that is where getDCCKeys() is designed for:
		 *    if: the item is lest than, e.g 100 => where "id in (...
		 *    else: insert them into a temp table, and the "where id in select ... from temp
		 * on top of that, dccKeys may be return by a DataPoint as a series, because the list maybe too long 
		 *    than may stress the machines memory resource :
		 *      while (true){
		 *      	getDCCkeys()[0]  as the keys
		 *      	if ( getDCCkeys()[1])
		 *      		break;
		 *      }
		 */
		
		delRS = getRS(delStmt);
		instRS = getRS(qryStmt); //TODO: that depends. It could come from Kafka!

		tgt.prepareBatchStmt(delStmt); 	//ideally, that should be abstract into tgt; 
										//but practically, it seems more reusable.
		try {
			int delRScol = delRS.getMetaData().getColumnCount();
			int instRScol = instRS.getMetaData().getColumnCount();

			while(delRS.next()) {
				tgt.upwrite(delRS, delRScol);	//tgt can be JDBCData, can be others
			}
		tgt.upwrite((ResultSet)null, delRScol);	//signal the end
		
		tgt.prepareBatchStmt(insStmt);
		while(instRS.next()) {
			tgt.upwrite(instRS, instRScol);
		}
		tgt.upwrite((ResultSet)null, instRScol);	//signal the end
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		JSONObject rslt = new JSONObject();
		rslt.put("totalInsert", 999);
		rslt.put("totalDel", 999);
		return rslt;
	}
	int currSyncCnt=0, totalSynCnt=0,totalErrCnt=0;
	@Override
	public int upwrite(ResultSet rs, int colCnt) {	//copy data from JDBC to JDBC
		Object o;
		
		try {
			if(null==rs) {			//last batch
				batchIns = prepStmt.executeBatch();
			}else {
				for (fldInx = 1; fldInx < colCnt; fldInx++) {
					//The last column is the internal record key.
					//for Oracle ROWID, is a special type, let's treat all as String
					//for uniformity, so are the others. let's see if that is okay.
					//o= rs.getObject(fldInx);
					prepStmt.setObject(fldInx, rs.getObject(fldInx));
					//syncInsStmt.setString(syncFldType.size(), rs.getString(syncFldType.size()));  //this two line assumes the last field is
				}
				//the last field (ORAID) need to be casted to String
				prepStmt.setString(colCnt, rs.getString(colCnt));
				prepStmt.addBatch();
				syncRowIDs[currSyncCnt] = rs.getString(colCnt); //record the PK. Assumed it is always the last one;
				// insert batch into target table
				totalSynCnt++;
				currSyncCnt++;
	
				if (currSyncCnt == batchSize) {
					batchIns = prepStmt.executeBatch();
					currSyncCnt = 0;
					logger.info("   addied batch - " + totalSynCnt);
				}
			}
		} catch (BatchUpdateException e) {
			logger.error("   Batch Errors... ");
			logger.error(e);
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
			e.printStackTrace();
		}
				
		return 0;

	}

	@Override
	public int upwrite(GenericRecord rs, int colCnt) {	//copy data from kafka to JDBC
/*
		for (Field field : rs.getSchema().getFields()) {
		    String fieldKey = field.name();
		    System.out.println(fieldKey + " : " + rs.get(fieldKey));
		    System.out.println(fieldKey + " : " + rs.get(1));
		}
*/		
		final TaskMeta taskMeta = TaskMeta.getInstance();

		String delSQL = (String)(((JSONObject)taskMeta.getTaskDetails().get("tgtDetail")).get("DELETE"));

		try {
			if(null==rs) {			//last batch
				batchIns = prepStmt.executeBatch();
			}else {
				
				for (fldInx = 1; fldInx < rs.getSchema().getFields().size(); fldInx++) {  //The last column is the internal record key.
					prepStmt.setObject(fldInx, rs.get(fldInx));
					//syncInsStmt.setString(syncFldType.size(), rs.getString(syncFldType.size()));  //this two line assumes the last field is
				}
					//the last field (ORAID) need to be casted to String
				prepStmt.setObject(colCnt, rs.get(colCnt));
				prepStmt.addBatch();
				syncRowIDs[currSyncCnt] = (String) rs.get(rs.getSchema().getFields().size()); //record the PK. Assumed it is always the last one;
				// insert batch into target table
				totalSynCnt++;
				currSyncCnt++;
	
				if (currSyncCnt == batchSize) {
					batchIns = prepStmt.executeBatch();
					currSyncCnt = 0;
					logger.info("   addied batch - " + totalSynCnt);
				}
			}
		} catch (BatchUpdateException e) {
			logger.error("   Batch Errors... ");
			logger.error(e);
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
			e.printStackTrace();
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

	private ResultSet getRS(String sql) {
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
	@Override
	public List<String> getDCCkeys() {
		// TODO Auto-generated method stub
		return null;
	}
	

	/*************************************************************************/	
	//---------audit APIs
	@Override
	public int getRecordCount(){
		int rtv;
		Statement sqlStmt;
		ResultSet sqlRset;
		int i;

		String sql="select count(1) from " + dataDetail.get("SRCTBL");
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

	//registration related APIs	   
	@Override
	public JSONObject runDBcmd(String sqlStr, String type) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getAVRO(String selectStmt) {
		Statement lrepStmt;
		ResultSet lrRset;
		int i;

		String avroSchema = "{\"namespace\": \"com.future.DI2020.avro\", \n" 
				    + "\"type\": \"record\", \n" 
				    + "\"name\": \"" + dataDetail.get("SRCTBL") + "\", \n" 
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