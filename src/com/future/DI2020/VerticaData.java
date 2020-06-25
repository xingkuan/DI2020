package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.text.*;
import java.time.Duration;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;

class VerticaData extends JDBCData {

	private Statement sqlStmt;
	private boolean stmtOpen;
	private ResultSet sRset;

	private static final Logger ovLogger = LogManager.getLogger();

	//public VerticaData(String dbid) throws SQLException {
	public VerticaData(JSONObject dbid, String role) throws SQLException {
		super(dbid, role);
	}

	public void setupSinkData() {
		// TODO Auto-generated method stub

	}

	public boolean miscPrep(String jobTempId) {
		// TODO Auto-generated method stub
		return true;
	}

	// no where clause for initializing
	public int initDataFrom(DataPoint srcData) {
		int rtc=0;
		truncateTbl();
		ResultSet rsltSet = srcData.getSrcResultSet();
		//copyDataFrom(rsltSet);
		syncDataFromV2(rsltSet, 1);  //1 for initializing  
		
		metaData.setTotalInsCnt(totalSynCnt);
		metaData.setTotalErrCnt(totalErrCnt);
		metaData.setTotalDelCnt(totalDelCnt);

		if (totalSynCnt < 0) {
			rtc = -1;
		} else {
			rtc = 2;
		}
		
		return rtc;
	}
	private void truncateTbl() {
		String sql = "truncate table " + metaData.getTableDetails().get("tgt_schema") + "."+ metaData.getTableDetails().get("tgt_table");
		runUpdateSQL(sql);
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
			ovLogger.error(e);
		} 
		return true;
	}

	// when log table is inserted from trigger
	public int syncDataFrom(DataPoint srcData) {
		int rtc=0;
	/* other than the src resultset, also create a KEY list from srcData,
	 * which is used to delete the stalerows in tgt.
	 */
		List<String> keyList = srcData.getDCCKeyList();
		dropStaleRowsOfList(keyList);
		
		// where clause is to be composed from log table
		ResultSet rsltSet = srcData.getSrcResultSet();
		rtc = syncDataFromV2(rsltSet, 2);  //2 for sync

		// TODO: and clear src log tables...
		
		metaData.setTotalInsCnt(totalSynCnt);
		metaData.setTotalErrCnt(totalErrCnt);
		metaData.setTotalDelCnt(totalDelCnt);
		metaData.end(rtc);
//		metaData.setRefreshSeqThis(srcData.getThisJournalSeqNum());
		//to be called from driver pgm
		//metaData.saveInitStats();
//should not be here!!!		
		metaData.saveInitStats();
		// db2KafkaMeta.saveReplicateKafka(); Initialize table has nothing to do with
		// Journal level info. Don't do it here.

		if (totalSynCnt < 0) {
			rtc=-1;
		} else {
			rtc=2;
		}

		srcData.close();
		close();
		
		return rtc;
	}
	public int dropStaleRowsOfList(List<String> keys) {
		int rtc = 2;
		int batchSize = Integer.parseInt(conf.getConf("batchSize"));

		int[] batchDel = null;
		int i = 0, curRecCnt = 0;
		PreparedStatement delStmt;

		try {
			((VerticaConnection) dbConn).setProperty("DirectBatchInsert", true);
			delStmt = dbConn.prepareStatement(metaData.getSQLDelTgt());
			for (String key: keys) {
				try {
					delStmt.setString(1, key);
				} catch (Exception e) {
					ovLogger.error("   " + e.toString());
					ovLogger.error("    The key of problem: " + key);
					//rtc = -1; //will keep going!
				}
				delStmt.addBatch();
				totalDelCnt++;

				if (curRecCnt == batchSize) {
					try {
						batchDel = delStmt.executeBatch();

						curRecCnt = 0;
						ovLogger.info("   batch - " + totalSynCnt);
					} catch (BatchUpdateException e) {
						ovLogger.error("   Batch Error: " + e);
						return -1;  //just error out
					}
				}
			}
			// the last batch
			try {
				batchDel = delStmt.executeBatch();
			} catch (BatchUpdateException e) {
				ovLogger.error("   Batch Error: " + e);
				return -1;  //just error out
			}
			//commit(); //to be called at the end of sync
			rtc = 2;
		} catch (SQLException e) {
			rtc = -1;
			//rollback();  //to be called at the end of sync
			ovLogger.error(e);
		}
		return rtc;
	}

	public int syncDataViaV2(DataPoint srcData, DataPoint auxData) {
		int rtc = 2;
		List<String> keys = auxData.getDCCKeyList();
		/* Drop the idea of using where key in (....) for small list;
		 *   The code becomes too complicated if I do.
		 */
		if(keys.size()>0) {
			//Thread 1: Ask srdData to select data from the list
			Runnable srcTask = () -> { 
				srcData.crtSrcResultSet(keys);
				};
			Thread srcThread=new Thread(srcTask);
			srcThread.start();
			
			//main thread: batch delete the records in this target
			dropStaleRowsOfList(keys);
			//wait till thread 1 and do batch insert:
			try {
				srcThread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//now the source is ready, the tgt is cleaned:
			ResultSet rsltSet = srcData.getSrcResultSet();
			rtc = syncDataFromV2(rsltSet, 2);
			if(rtc<0) {
				ovLogger.error("Error happened. There is the risk of data being out of sync for " + metaData.getTableDetails().get("src_schema") + metaData.getTableDetails().get("src_table"));
			}

		}else {
			ovLogger.info("   No changes!");
		}
		
		return rtc;
	}
	// when log table is from kafka
	// TODO: move the logic into Kafka, at where, messages are read into list and deduplicated.
/*	public int syncDataVia(DataPointer srcData, DataPointer auxData) {
		int giveUp = Integer.parseInt(conf.getConf("kafkaMaxEmptyPolls"));

		int noRecordsCount = 0, cntRRN = 0;
		boolean firstItem = true; 
		int rtc = 2;
		String rrnList = "";
		long lastJournalSeqNum = 0l;

		while (true) {
			// blocking call:
			//TODO: not pretty!!!
			ConsumerRecords<Long, String> records = ((KafkaData)auxData).readMessages();
			// ConsumerRecords<Long, String> records = consumerx.poll(0);
			if (records.count() == 0) {
				noRecordsCount++;
				ovLogger.info("    consumer poll cnt: " + noRecordsCount);
				if (noRecordsCount > giveUp)
					break; // no more records. exit
				else
					continue;
			}

			for (ConsumerRecord<Long, String> record : records) {
				lastJournalSeqNum = record.key();

				// if( lastJournalSeqNum == db2KafkaMeta.getSeqLastRefresh()) { //stop by the
				// SEQ number indicated in sync_journal400 table:
				// break; //break from the for loop
				// } ///let's stop where ever it is. really, it does not matter!

				if (firstItem) {
					rrnList = record.value();
					firstItem = false;
				} else
					rrnList = rrnList + "," + record.value();

				cntRRN++;
			}
			ovLogger.info("    processing to: " + cntRRN);
		//	rtc = replicateRowList(srcData, rrnList);
		//	if (!success)
		//		break;
			// in case there are more in Kafka broker, start the next cycle:
			rrnList = "";
			noRecordsCount = 0;
			firstItem = true;
		}

		metaData.end(rtc);
		metaData.setTotalDelCnt(totalDelCnt);
		metaData.setTotalDelCnt(totalErrCnt);
		metaData.setTotalDelCnt(totalInsCnt);

		return rtc;	
	}
*/
	private int syncDataFromV2(ResultSet rsltSet, int actId) {
		int rtc = 2;
		int batchSize = Integer.parseInt(conf.getConf("batchSize"));

		int[] batchDel = null;
		int[] batchIns = null;
	          // >=0: Successfully executed; The number represents number of affected rows
	    	  // Statement.SUCCESS_NO_INFO: Successfully executed; number of affected rows not available
	          // Statement.EXECUTE_FAILED;
		String[] RowIDs = new String[batchSize];
		int i = 0, curRecCnt = 0;

		ArrayList<Integer> javaType = metaData.getFldJavaType();
		ArrayList<String> fldNames = metaData.getFldNames();

		PreparedStatement insStmt;

		try {
			((VerticaConnection) dbConn).setProperty("DirectBatchInsert", true);

			insStmt = dbConn.prepareStatement(metaData.getSQLInsTgt());

			while (rsltSet.next()) {
				try {
					for (i = 1; i <= javaType.size()-1; i++) {  //The last column is the internal key.
															//for Oracle ROWID, is a special type, let's treat it as String
															//for uniformity, so are the others. let's see if that is okay.
						insStmt.setObject(i, rsltSet.getObject(i));
					}
					insStmt.setString(javaType.size(), rsltSet.getString(javaType.size()));
					//To save a little: the ID field is always the last column!
					//RowIDs[curRecCnt] = srcRset.getString(metaData.getPK());
					RowIDs[curRecCnt] = rsltSet.getString(javaType.size());
				} catch (Exception e) {
					ovLogger.error("initLoadType1 Exception.");
					ovLogger.error("   " + e.toString());
					ovLogger.error("    ****************************");
					ovLogger.error("    rowid: " + rsltSet.getString(metaData.getPK()));
					ovLogger.error("    fieldno: " + i + "  " + fldNames.get(i));
					rtc = -1;
				}
				
				// insert batch into target table
				insStmt.addBatch();
				totalSynCnt++;
				curRecCnt++;

				if (curRecCnt == batchSize) {
					try {
						batchIns = insStmt.executeBatch();

						curRecCnt = 0;
						ovLogger.info("   addied batch - " + totalSynCnt);
					} catch (BatchUpdateException e) {
						ovLogger.error("   Batch Error... ");
						ovLogger.error(e);
						for (i = 1; i <= fldNames.size(); i++) {
							ovLogger.error("   " + rsltSet.getString(i));
						}
						//int[] iii;
						//iii = e.getUpdateCounts();
						for (i = 0; i < batchSize; i++) {
							if (batchIns[i] == Statement.EXECUTE_FAILED) {
								ovLogger.info("   " +  RowIDs[i]);
								putROWID(RowIDs[i]);
								totalErrCnt++;
							}
						}
					}
				}
			}
			// the last batch
			try {
				batchIns = insStmt.executeBatch();
			} catch (BatchUpdateException e) {
				ovLogger.error("   Error... rolling back");
				ovLogger.error(e.getMessage());

				for (i = 0; i < batchSize; i++) {
					if (batchIns[i] == Statement.EXECUTE_FAILED) {
						ovLogger.info("   " +  RowIDs[i]);
						putROWID(RowIDs[i]);
						totalErrCnt++;
					}
				}
			}

			//commit();  //to be called at the end of sync
			rtc = 2;
		} catch (SQLException e) {
			rtc = -1;
			// rollback();  //to be called at the end of sync
			ovLogger.error(e.getMessage());
		}

		return rtc;
	}
	
	
//------------------
	public int getRecordCount() {
		int rtv = 0;

		ResultSet lrRset;
		int i;
		  String sql;
	      if(dbRole.equals("SRC")) {
	    	  sql="select count(*) from " + metaData.getTableDetails().get("src_schema").toString() 
			  		+ "." + metaData.getTableDetails().get("src_table").toString();
	      }else if(dbRole.equals("TGT")) {
	    	  sql="select count(*) from " + metaData.getTableDetails().get("tgt_schema").toString() 
			  		+ "." + metaData.getTableDetails().get("tgt_table").toString();
	      }else {
	    	  ovLogger.error("invalid DB role assignment.");
	    	  return -1;
	      }

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			lrRset = sqlStmt.executeQuery(sql);
			if (lrRset.next()) {
				rtv = Integer.parseInt(lrRset.getString(1));
			}
			lrRset.close();
		} catch (SQLException e) {
			ovLogger.error("  error count tbl rows: " + e);
		}
		return rtv;
	}

	public ResultSet getSrcResultSet() {
		return sRset;
	}

	public void closeSrcResultSet() throws SQLException {
		sRset.close();
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

	public void close() {
		try {
			sqlStmt.close();
			dbConn.close();
		} catch (Exception e) {
			ovLogger.info("   TODO: nothing to close for " + dbID);
		}
		ovLogger.info("   closed src db conn: " + dbID);
	}

	private int deleteRowsBatch(ResultSet rs) throws SQLException {
		int rtc = 0;
		String delSQL = metaData.getSQLDelTgt();
		int batchSize = Integer.parseInt(conf.getConf("batchSize"));

		int[] batchResults = null;
		int i = 0, curRecCnt = 0;

		PreparedStatement tgtPStmt;

//		((VerticaConnection) dbConn).setProperty("DirectBatchInsert", true);
		tgtPStmt = dbConn.prepareStatement(delSQL);
		try {
			while (rs.next()) {
				tgtPStmt.setObject(i, rs.getObject(0));
				tgtPStmt.addBatch();

				if (curRecCnt == batchSize) {
					batchResults = tgtPStmt.executeBatch();
					if (!ckeckBatch(batchResults)) {
						ovLogger.error("   delete batch has problem.");
					}
				curRecCnt = 0;
				ovLogger.info("   delete batch - " + totalSynCnt);
			}
			curRecCnt++;
		}
		// the last batch
		batchResults = tgtPStmt.executeBatch();
		if (!ckeckBatch(batchResults)) {
			ovLogger.error("   delete batch has problem.");
		}
		//commit();  //to be called at the end of sync
		} catch (SQLException e) {
			ovLogger.error(e);
			//rollback();  //to be called at the end of sync
			rtc=-1;
		}
		return rtc;
	}
	//even if found prblem, keeps going, but report in log 
	private boolean ckeckBatch(int[] batch) {
		boolean good=true;
		totalDelCnt=0;
		for (int b: batch) { 
        	if (b>0)
        		totalDelCnt++;
        	else {
        		good=false;
        		//break;
        	}
		}
 		return good;
	}
	private void putROWID(String rowid) {
		try {
			// . FileWriter fstream = new FileWriter(metaData.getInitLogDir() + "/" +
			// metaData.getTgtSchema() + "." + metaData.getTgtTable() + ".row", true);
			FileWriter fstream = new FileWriter(
					logDir + metaData.getTableDetails().get("tgt_sch").toString() + "." + metaData.getTableDetails().get("tgt_tbl").toString()  + ".row", true);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(rowid + "\n");
			out.close();
		} catch (Exception e) {
			ovLogger.error(e.getMessage());
		}
	}
	
}