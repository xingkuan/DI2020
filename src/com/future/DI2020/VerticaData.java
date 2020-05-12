package com.future.DI2020;

import java.io.*;
import java.util.*;
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

class VerticaData extends DataPointer {

	private Connection dbConn;
	private Statement sqlStmt;
	private boolean dbConnOpen;
	private boolean stmtOpen;
	private ResultSet sRset;

	private int connAtmptLim = 5;
	private int AtmptDelay = 5000;

	private long seqThisFresh = 0;
	private java.sql.Timestamp tsThisRefesh = null;

	private static final Logger ovLogger = LogManager.getLogger();

	public VerticaData(String dbid) throws SQLException {
		super(dbid);
	}

	public void setupSinkData() {
		// TODO Auto-generated method stub

	}

	public boolean miscPrep() {
		// TODO Auto-generated method stub
		return true;
	}

	// no where clause for initilizing
	public void initDataFrom(DataPointer srcData) {
		ovLogger.info("    START...");
		ResultSet rsltSet = srcData.getAuxResultSet("");
		copyDataFrom(rsltSet);
		ovLogger.info("    COMPLETE.");
	}

	// when log table is inserted from trigger
	public void syncDataFrom(DataPointer srcData) {
		ovLogger.info("    START...");

		// where clause is to be composed from log table
		ResultSet rsltSet = srcData.getAuxResultSet();
		try {
			dropStaleRowsViaResultSet(rsltSet);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// where clause is to be composed from log table
		rsltSet = srcData.getSrcResultSet();
		copyDataFrom(rsltSet);

		// TODO: and clear src log tables...
		ovLogger.info("    COMPLETE.");
		srcData.close();
		close();
	}

	// when log table is from kafka
	public void syncDataVia(DataPointer srcData, KafkaData auxData) {
		ovLogger.info("    START...");

		int giveUp = Integer.parseInt(conf.getConf("kafkaMaxEmptyPolls"));

		int noRecordsCount = 0, cntRRN = 0;
		boolean firstItem = true, success = false;
		String rrnList = "";
		long lastJournalSeqNum = 0l;

		while (true) {
			// blocking call:
			ConsumerRecords<Long, String> records = auxData.readMessages();
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
			success = replicateRowList(srcData, rrnList);
			if (!success)
				break;
			// in case there are more in Kafka broker, start the next cycle:
			rrnList = "";
			noRecordsCount = 0;
			firstItem = true;
		}

		ovLogger.info("    COMPLETE!");

		metaData.markEndTime();
		metaData.setTotalDelCnt(totalDelCnt);
		metaData.setTotalDelCnt(totalErrCnt);
		metaData.setTotalDelCnt(totalInsCnt);
		metaData.sendMetrix();

		metaData.setRefreshCnt(cntRRN);
		metaData.setRefreshSeqLast(lastJournalSeqNum);

		metaData.saveRefreshStats(metaData.getJobID());

		ovLogger.info("Refreshed tblID: " + metaData.getTableID() + ", Del Cnt: " + totalDelCnt);
		ovLogger.info("Refreshed tblID: " + metaData.getTableID() + ", Ins Cnt: " + totalInsCnt);
		ovLogger.info("Refreshed tblID: " + metaData.getTableID() + ", Err Cnt: " + totalErrCnt);

		if (lastJournalSeqNum > 0)
			metrix.sendMX("JournalSeq,metaData.getJobID()=" + metaData.getJobID() + ",tblID=" + metaData.getSrcTblAb7()
					+ "~" + metaData.getTableID() + " value=" + lastJournalSeqNum + "\n");

		ovLogger.info("tblID: " + metaData.getTableID() + ", " + " - " + metaData.getTableDetails().get("src_sch") + "."
				+ metaData.getTableDetails().get("src_tbl").toString() + " commited");

		if (!success) {
			metaData.setCurrentState(7); // broken - suspended
			ovLogger.info("metaData.getJobID(): " + metaData.getJobID() + ", tblID: " + metaData.getTableID()
					+ "refresh not succesfull");
		} else {
			metaData.setCurrentState(5); // initialized
			ovLogger.info("metaData.getJobID(): " + metaData.getJobID() + ", tblID: " + metaData.getTableID()
					+ " <<<<<  refresh successfull");
		}
		ovLogger.info("    COMPLETE.");
		srcData.close();
		close();
	}

	private boolean copyDataFrom(ResultSet rsltSet) {
		boolean rtc = true;
		int batchSize = Integer.parseInt(conf.getConf("batchSize"));

		int[] batchResults = null;
		String[] RowIDs = new String[batchSize];
		int i = 0, curRecCnt = 0;
		//boolean commFlag = true;

		String ts;
		String sDebug;

		PreparedStatement tgtPStmt;

		ResultSet srcRset = rsltSet;
		try {
			((VerticaConnection) dbConn).setProperty("DirectBatchInsert", true);

			tgtPStmt = dbConn.prepareStatement(metaData.getSQLInsert());

			int tmpInt;
			float tmpFloat;
			double tmpDouble;
			long tmpLong;
			// insert records into batch
			while (srcRset.next()) {
				i = 0;
				// RowIDs[curRecCnt]=srcRset.getString("rowid");
				RowIDs[curRecCnt] = srcRset.getString(metaData.getPK());
				try {
					for (i = 1; i <= metaData.getFldCnt(); i++) {
						switch (metaData.getFldType(i - 1)) {
						case 1: // String
							// String x1 =srcRset.getString(i);
							tgtPStmt.setString(i, srcRset.getString(i));
							break;
						case 2: // int
//	                     tgtPStmt.setInt(i,srcRset.getInt(i));
							tmpInt = srcRset.getInt(i);
							if ((tmpInt == 0) && srcRset.wasNull())
								tgtPStmt.setNull(i, java.sql.Types.INTEGER);
							else
								tgtPStmt.setInt(i, tmpInt);
							// int x2 =srcRset.getInt(i);
							break;
						case 3: // Long
							// tgtPStmt.setLong(i,srcRset.getLong(i));
							// long x3 =srcRset.getLong(i);
							tmpLong = srcRset.getLong(i);
							if ((tmpLong == 0) && srcRset.wasNull())
								tgtPStmt.setNull(i, java.sql.Types.NULL);
							else
								tgtPStmt.setDouble(i, tmpLong);
							break;
						case 4: // Double
							// tgtPStmt.setDouble(i,srcRset.getDouble(i));
							tmpDouble = srcRset.getDouble(i);
							if ((tmpDouble == 0) && srcRset.wasNull())
								tgtPStmt.setNull(i, java.sql.Types.DOUBLE);
							else
								tgtPStmt.setDouble(i, tmpDouble);
							break;
						case 5: // Float
							// tgtPStmt.setFloat(i,srcRset.getFloat(i));
							tmpFloat = srcRset.getFloat(i);
							if ((tmpFloat == 0) && srcRset.wasNull())
								tgtPStmt.setNull(i, java.sql.Types.FLOAT);
							else
								tgtPStmt.setFloat(i, tmpFloat);
							break;
						case 6: // Timestamp
							tgtPStmt.setTimestamp(i, srcRset.getTimestamp(i));
							// Timestamp x6 =srcRset.getTimestamp(i);
							break;
						case 7: // Date
							tgtPStmt.setDate(i, srcRset.getDate(i));
							java.sql.Date x7 = srcRset.getDate(i);
							break;
						case 100: // alternate encoding
							tgtPStmt.setString(i, new String(srcRset.getString(i).getBytes("ISO-8859-15"), "UTF-8"));
							break;
						case 1000: // alternate encoding w debugging
							sDebug = new String(srcRset.getString(i).getBytes("ISO-8859-15"), "UTF-8");
							tgtPStmt.setString(i, sDebug);
							System.out.println(sDebug);
							break;
						case 101: // alternate encoding
							// System.out.println("type 101 - " + i + " |" + srcRset.getString(i) + "|");
							ts = srcRset.getString(i);
							if (ts != null) {
								tgtPStmt.setString(i, new String(srcRset.getString(i).getBytes("US-ASCII"), "UTF-8"));
							} else {
								tgtPStmt.setString(i, "");
							}
							break;
						case 1010: // alternate encoding w debugging
							// System.out.println("type 101 - " + i + " |" + srcRset.getString(i) + "|");
							ts = srcRset.getString(i);
							if (ts != null) {
								sDebug = new String(srcRset.getString(i).getBytes("US-ASCII"), "UTF-8");
								tgtPStmt.setString(i, sDebug);
								System.out.println(sDebug);
							} else {
								tgtPStmt.setString(i, "");
							}
							break;
						case 102: // alternate encoding
							tgtPStmt.setString(i, new String(srcRset.getString(i).getBytes("UTF-16"), "UTF-8"));
							break;
						case 103: // alternate encoding
							ts = srcRset.getString(i);
							if (ts != null) {
								tgtPStmt.setString(i, new String(srcRset.getString(i).getBytes("UTF-8"), "UTF-8"));
							} else {
								tgtPStmt.setString(i, "");
							}
							break;
						case 1030: // alternate encoding w debugging
							sDebug = new String(srcRset.getString(i).getBytes("UTF-8"), "UTF-8");
							tgtPStmt.setString(i, sDebug);
							System.out.println(sDebug);
							break;
						case 104: // alternate encoding
							tgtPStmt.setString(i, new String(srcRset.getString(i).getBytes("ISO-8859-1"), "UTF-8"));
							break;
						case 1040: // alternate encoding w debugging
							sDebug = new String(srcRset.getString(i).getBytes("ISO-8859-1"), "UTF-8");
							tgtPStmt.setString(i, sDebug);
							System.out.println(sDebug);
							break;
						case 105: // alternate encoding
							tgtPStmt.setString(i, new String(srcRset.getString(i).getBytes("ISO-8859-2"), "UTF-8"));
							break;
						case 1050: // alternate encoding w debugging
							sDebug = new String(srcRset.getString(i).getBytes("ISO-8859-2"), "UTF-8");
							tgtPStmt.setString(i, sDebug);
							System.out.println(sDebug);
							break;
						case 106: // alternate encoding
							tgtPStmt.setString(i, new String(srcRset.getString(i).getBytes("ISO-8859-4"), "UTF-8"));
							break;
						case 1060: // alternate encoding w debugging
							sDebug = new String(srcRset.getString(i).getBytes("ISO-8859-4"), "UTF-8");
							tgtPStmt.setString(i, sDebug);
							System.out.println(sDebug);
							break;
						case 107: // alternate encoding
							tgtPStmt.setString(i,
									new String(srcRset.getString(i).getBytes("ISO-8859-1"), "ISO-8859-1"));
							break;
						case 1070: // alternate encoding w debugging
							sDebug = new String(srcRset.getString(i).getBytes("ISO-8859-1"), "ISO-8859-1");
							tgtPStmt.setString(i, sDebug);
							System.out.println(sDebug);
							break;
						// case 125: //alternate encoding - unicode stream
						// tgtPStmt.setUnicodeStream(i,srcRset.getUnicodeStream(i),srcRset.getUnicodeStream(i).available());
						// tgtPStmt.setString(i,new String(srcRset.getString(i).getBytes("ISO-8859-4"),
						// "UTF-8"));
						// break;
						case 999: // set string blank more for testing purposes
							tgtPStmt.setString(i, new String(""));
							break;
						default: // default (String)
							tgtPStmt.setString(i, srcRset.getString(i));
							break;
						}
					}
				} catch (Exception e) {
					ovLogger.error("initLoadType1 Exception. Rollback.");
					ovLogger.error("   " + e.toString());
					ovLogger.error("    ****************************");
					ovLogger.error("    rowid: " + RowIDs[curRecCnt]);
					ovLogger.error("    fieldno: " + i + "  " + metaData.getFldName(i - 1));
					// return -1;
				}
				// insert batch into target table
				tgtPStmt.addBatch();
				totalSynCnt++;
				curRecCnt++;

				if (curRecCnt == batchSize) {
					curRecCnt = 0;
					ovLogger.info("   adding recs (accumulating) - " + totalSynCnt);
					try {
						batchResults = tgtPStmt.executeBatch();
					} catch (BatchUpdateException e) {
						ovLogger.error("   executeBatch Error... ");
						ovLogger.error(e.toString());
						//commFlag = false;
						for (i = 1; i <= metaData.getFldCnt(); i++) {
							ovLogger.error("   " + srcRset.getString(i));
						}
						int[] iii;
						iii = e.getUpdateCounts();
						for (i = 1; i <= batchSize; i++) {
							if (iii[i - 1] == -3) { // JLEE, 07/24: the failed row.
								ovLogger.info("   " + (i - 1) + " : " + iii[i - 1] + " - " + RowIDs[i - 1]);
								putROWID(RowIDs[i - 1]);
								totalErrCnt++;
							}
						}
					}
				}
			}
			// the last batch
			try {
				batchResults = tgtPStmt.executeBatch();
			} catch (BatchUpdateException e) {
				ovLogger.error("   Error... rolling back");
				ovLogger.error(e.getMessage());

				//commFlag = false;
				int[] iii;
				iii = e.getUpdateCounts();
				ovLogger.error("   Number of records in batch: " + curRecCnt);

				for (i = 1; i <= curRecCnt; i++) {
					if (iii[i - 1] == -3) {
						ovLogger.error("   " + (i - 1) + " : " + iii[i - 1] + " - " + RowIDs[i - 1]);
						putROWID(RowIDs[i - 1]);
						totalErrCnt++;
					}
				}
			}

			commit();
		} catch (SQLException e) {
			try {
				rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			ovLogger.error(e.getMessage());
		}

		metaData.setTotalInsCnt(totalSynCnt);
		metaData.setTotalErrCnt(totalErrCnt);
		metaData.setTotalDelCnt(totalErrCnt);
		metaData.markEndTime();
		metaData.saveInitStats();
		metaData.sendMetrix();
		// db2KafkaMeta.saveReplicateKafka(); Initialize table has nothing to do with
		// Journal level info. Don't do it here.

		if (totalSynCnt < 0) {
			metaData.setCurrentState(7); // broken - suspended
			rtc=false;
		} else {
			metaData.setCurrentState(2); // initialized
			rtc=true;
		}
		
		return rtc;
	}

	private boolean replicateRowList(DataPointer srcData, String rrns) {
		boolean success = true;

		try {
			dropStaleRowsOfList(rrns);

			ResultSet rsltSet = srcData.getAuxResultSet(rrns);
			copyDataFrom(rsltSet);

			commit();
		} catch (SQLException e) {
			ovLogger.error(e);
		}

		return success;
	}

	public int getRecordCount() {
		int rtv = 0;

		ResultSet lrRset;
		int i;

		try {
			sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			lrRset = sqlStmt
					.executeQuery("select count(*) from " + metaData.getTableDetails().get("src_sch").toString() + "." + metaData.getTableDetails().get("src_tbl").toString());
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

	public void commit() throws SQLException {
		dbConn.commit();
	}

	public void rollback() throws SQLException {
		dbConn.rollback();
	}

	public void close() {
		try {
			if (stmtOpen) {

				sqlStmt.close();
				stmtOpen = false;
			}
			if (dbConnOpen) {
				dbConn.close();
				dbConnOpen = false;
				ovLogger.info("   closed src db conn: " + dbID);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean dropStaleRowsOfList(String rrnList) throws SQLException {
		int delCnt = 0;
		String DeleteTargetTable = " delete from " + metaData.getTableDetails().get("tgt_sch").toString() + "." + metaData.getTableDetails().get("tgt_tbl").toString() + " where "
				+ metaData.getPK() + " in (" + rrnList + ")";

		delCnt = sqlStmt.executeUpdate(DeleteTargetTable);
		commit();

		totalDelCnt = totalDelCnt + delCnt;

		return true;
	}

	public Long dropStaleRowsViaResultSet(ResultSet srcRset) throws SQLException {
		String delSQL = " delete  from " + metaData.getTableDetails().get("tgt_sch").toString() + "." + metaData.getTableDetails().get("tgt_tbl").toString();
		
		int currRowCnt=0;
		long journalSeqNum=0l;
		
		delSQL += " where   " + metaData.getPK() + " in (";
		if (srcRset.next()) {
			currRowCnt++;
			totalDelCnt++;
			delSQL += "'" + srcRset.getString("RRN") + "'";

			journalSeqNum = srcRset.getLong("SEQNBR");
		}

		delSQL += " ) ";

		int delCnt = sqlStmt.executeUpdate(delSQL);
		totalDelCnt=delCnt;

		srcRset.close();

		return journalSeqNum;
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
	
	
	/*
	 * public int initLoadType2() throws SQLException { // perform load type 2 on
	 * table (this is no longer used) int[] batchResults = null;
	 * 
	 * int curRecCnt; int i = 0; String sqlCopyTgt = metaData.getSQLCopyTarget();
	 * PreparedStatement tgtPStmt; boolean commFlag;
	 * 
	 * errCnt = 0;
	 * 
	 * try { VerticaCopyStream stream = new VerticaCopyStream((VerticaConnection)
	 * tgtConn, sqlCopyTgt); } catch (Exception e) { ovLogger.error(label +
	 * e.getMessage()); }
	 * 
	 * return refreshCnt; }
	 * 
	 * public void truncate() { // truncates target table String sql; try { if
	 * (metaData.getTgtUseAlt()) { sql = "truncate table  " +
	 * metaData.getTgtSchema() + "." + metaData.getTgtTableAlt() + ";"; //
	 * System.out.println(label + " truncating alternate table " + //
	 * metaData.getTgtTableAlt()) ; ovLogger.info(label +
	 * " truncating alternate table " + metaData.getTgtTableAlt()); } else { sql =
	 * "truncate table  " + metaData.getTgtSchema() + "." + metaData.getTgtTable() +
	 * ";"; } tgtStmt.execute(sql); } catch (SQLException e) { //
	 * System.out.println(label + " Truncate table failure"); //
	 * System.out.println(e.getMessage()); ovLogger.error(label +
	 * " Truncate table failure"); ovLogger.error(label + e.getMessage()); } }
	 * 
	 * public void swapTable() { String sql; try { sql = "alter table " +
	 * metaData.getTgtSchema() + "." + metaData.getTgtTable() + ", " +
	 * metaData.getTgtSchema() + "." + metaData.getTgtTableAlt() + ", " +
	 * metaData.getTgtSchema() + "." + metaData.getTgtTableAlt() +
	 * "_gv_tmp rename to " + metaData.getTgtTableAlt() + "_gv_tmp, " +
	 * metaData.getTgtTable() + ", " + metaData.getTgtTableAlt() + ";";
	 * ovLogger.info(" swap table sql: " + sql); tgtStmt.execute(sql);
	 * ovLogger.info(label + " table swapped" + metaData.getTgtTableAlt()); } catch
	 * (SQLException e) { ovLogger.error(label + " swap table failure");
	 * ovLogger.error(label + e.getMessage()); } }
	 */
}