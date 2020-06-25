package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.time.Duration;
import java.sql.*;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;

class runTable {
	private static final Logger ovLogger = LogManager.getLogger();
	private static final Metrix metrix = Metrix.getInstance();
	private static final MetaData metaData = MetaData.getInstance();

	static int tableID;
	static DataPoint srcData;
	static DataPoint tgtData;
	static DataPoint auxData;


	static String jobID ;
	static int jobSub=3;

	private int totalDelCnt = 0, totalInsCnt = 0, totalErrCnt = 0;

	private int kafkaMaxPollRecords;
	private int pollWaitMil;

	/* actId:
	 *     0: enable
	 *     1: initial copy
	 *     2: sync
	 *     9: audit
	 */
	/* test parms:
	 *      tbl 2 2[..]     -- DB2 to Vertica, via Kafka, sync[..]
	 * 		tbl 3 0[..]	    -- DB2 J. to Kafka, enable[..]
	 * 		tbl 5 2[..]	    -- Oracle to Vertica, sync[..]
	 * 		tbl 6 2[..]	    -- Oracle to Kafka, sync[..]
	 */
	public static void main(String[] args) {
		System.out.println(args.length);

		if (args.length != 3) {
			System.out.println("Usage:   runTable <tbl|pool> id aID");
			//return -1;
		}

		String parmCat = args[0];
		int parmId = Integer.parseInt(args[1]);
		int actId = Integer.parseInt(args[2]);
		
		if(parmCat.contentEquals("pool"))
			actOnTables(parmId, actId);
		else if(parmCat.contentEquals("tbl"))
			actOnTable(parmId, actId);
		else 
			System.out.println("Usage:   syncTable <tbl|pool> oId aId");
			
	}
	static void actOnTables(int poolID, int actId) {
		List<Integer> tblList = metaData.getTblsByPoolID(poolID);
		for (int i : tblList) {
            actOnTable(i, actId);
        }
	}
	static void actOnTable(int tID, int actId) {
		ovLogger.info("    BEGIN.");

		//based on jobDetail, do the corresponding...
		switch(actId){
			case 0:  //enable table to be actionable.
				jobID = "enableTbl";
				actType0(tID, actId);
				break;
			case 1:  //initial copying of data from src to tgt
				jobID = "initTbl";
				actType1(tID, actId);
				break;
			case 2:   //sync DCC, and tbl as well 
				jobID = "syncTbl";
				actType2(tID, actId);
				break;
			case 9:   //audit
				actType9(tID, actId);
				break;
			default:
				ovLogger.info("unkown action");
				break;
		}
		ovLogger.info("    End.");
	}

	private static int setupAct(int tblID, int actId) { 
		tableID=tblID;
		if(metaData.setupTableForAction(jobID, tableID, actId)==-1) {
			ovLogger.error("Exit without doing anything.");
			return -1;
		}
		ovLogger.info(jobID + " " + tableID + ": " + metaData.getTableDetails().get("src_table").toString());

		JSONObject tblDetail = metaData.getTableDetails();
		String actTemp = tblDetail.get("temp_id").toString();
		
		switch(actId) {
		case 0:
			srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
			srcData.miscPrep(actTemp);  //parm is to avoid reading max jrnal seq num when not needed
			ovLogger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());
			break;
		case 1:
		case 9:
			srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
			srcData.miscPrep(actTemp);  //parm is to avoid reading max jrnal seq num when not needed
			ovLogger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());

			tgtData = DataPoint.dataPtrCreater(tblDetail.get("tgt_db_id").toString(), "TGT");
			tgtData.miscPrep(actTemp);
			tgtData.setupSink();
			ovLogger.info("   tgt ready: " + metaData.getTableDetails().get("tgt_table").toString());
		}
		
		return 0;
   }
	/* Setup data sources, so job can be. But also try to do the minimum: 
	 * - If {count of DCC} == 0: don't do the rest.
	 * - If {count of DCC} >  0: prepare the needed data sources.
	 * - If {count of DCC} <  0: something is wrong; skip the rest.
	 */
	private static int setupAct2(int tblID, int actId) {  //TODO: too ugly!
		int dccCnt=0;
		
		tableID=tblID;
		if(metaData.setupTableForAction(jobID, tableID, actId)==-1) {
			ovLogger.error("Exit without doing anything.");
			return -1;
		}
		ovLogger.info(jobID + " " + tableID + ": " + metaData.getTableDetails().get("src_table").toString());

		JSONObject tblDetail = metaData.getTableDetails();
		String actTemp = tblDetail.get("temp_id").toString();
		switch(actTemp) {
		case "O2K":   //replicate records to kafka.
		case "O2V":
		case "DJ2K":  //replicate key to kafka
			srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
			srcData.miscPrep(actTemp);  //parm is to avoid reading max jrnal seq num when not needed
			ovLogger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());
			dccCnt = srcData.getDccCnt();
			if(dccCnt==0) {
				ovLogger.info("   no dcc.");
				return 0;  
			}
			break;
		case "D2K_":   //replicate records to kafka, via keys in Kafka
		case "D2V_":
			String auxDBstr = tblDetail.get("dcc_db_id").toString();
			auxData = DataPoint.dataPtrCreater(auxDBstr, "DCC");
			auxData.miscPrep(actTemp);
			dccCnt = auxData.getDccCnt();
			if(dccCnt==0) {
				ovLogger.info("   no dcc.");
				return 0;  
			}
			ovLogger.info("   aux ready: " + metaData.getTableDetails().get("src_table").toString());
			srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
			srcData.miscPrep(actTemp);  //parm is to avoid reading max jrnal seq num when not needed
			ovLogger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());
			break;
		default:
			ovLogger.error("Not a valid template: " + actTemp);
			break;
		}
		

		tgtData = DataPoint.dataPtrCreater(tblDetail.get("tgt_db_id").toString(), "TGT");
		tgtData.miscPrep(actTemp);
		tgtData.setupSink();
		ovLogger.info("   tgt ready: " + metaData.getTableDetails().get("tgt_table").toString());
		
		return dccCnt;
   }
	
	private static void actType0(int tID, int actId) {
		int syncSt=2;

		if(setupAct(tID, 0)==-1) {
			return;   // something is not right. Do nothing.
		}else {
			metaData.begin();
	
			srcData.beginDCC();  //For Oracle (to V), it is enable trigger and curr_state=2;
								 //For DB2/AS400 log (to K), set the seq_last_ref, and curr_state=2;
								 //For DB2/AS400 tbl (to V), curr_state=2
			metaData.end(syncSt);
			metaData.saveSyncStats();
		}
	}
	private static void actType1(int tID, int actId) {
		int syncSt=2;

		if(setupAct(tID, actId)==-1) {
			return;   // something is not right. Do nothing.
		}else {
			metaData.begin();
			//get the PRE and AFT sqls from meta:
			JSONArray preSQLs, aftSQLs;
			JSONObject sqlJO = metaData.getSrcSQLs(actId, false, false);
			if((sqlJO==null)||(sqlJO.isEmpty())){
				ovLogger.error("no SQL found for src resultset.");
				return;
			}
			preSQLs = (JSONArray) sqlJO.get("PRE");
			//aftSQLs = (JSONArray) sqlJO.get("AFT");
			
			srcData.crtSrcResultSet(actId, preSQLs);
			int state = tgtData.initDataFrom(srcData);
			//srcData.cleanup(actId, aftSQLs);
	
			srcData.close();
			tgtData.close();
	
			metaData.end(syncSt);
			metaData.saveSyncStats();
			tearDown();
		}
	}
	private static void actType2(int tID, int actId) {
		int syncSt=2;

		if(setupAct2(tID, actId)<=0) {
			// 1. 0: If count of DCC is 0, don't do anything.
			// 2. -1: If something is wrong,
			// 3. >0: the number of DCC
			return;   // something is not right. Do nothing.
		}else {
			metaData.begin();
			//get the PRE and AFT sqls from meta:
			JSONArray preSQLs, aftSQLs;
			JSONObject sqlJO = metaData.getSrcSQLs(actId, false, false);
			if((sqlJO==null)||(sqlJO.isEmpty())){
				ovLogger.error("no SQL found for src resultset.");
				syncSt=2;
				metaData.end(syncSt);
				return;
			}
			preSQLs = (JSONArray) sqlJO.get("PRE");
			aftSQLs = (JSONArray) sqlJO.get("AFT");
			
			String tempId = metaData.getTableDetails().get("temp_id").toString();
			switch(tempId) {
			case "O2V":    //no aux (kafka in between).
			case "O2K":
			case "D2V":
			case "DJ2K":
				syncSt=srcData.crtSrcResultSet(actId, preSQLs);
				if(syncSt<0) {
					ovLogger.info("    error in source.");
				}else {
					//TODO:  for test. need re-org!
					if(tempId.equals("O2K"))
						syncSt = tgtData.syncAvroDataFrom(srcData);
					else
						syncSt = tgtData.syncDataFrom(srcData);
					//srcData.afterSync(actId, aftSQLs);
				}
				break;
			case "D2V_":   //sync with DCC keys from kafka
				//auxData.crtAuxSrcAsList();  //This is done in setupAct2() already!
				syncSt = tgtData.syncDataViaV2(srcData, auxData);  //crtSrcResultSet() is pushed into this call. 
																   //TODO: ugly code.
				break;
			default:
				ovLogger.error("wrong template ID");
				break;
		}
		if(aftSQLs != null)	
			srcData.afterSync(actId, aftSQLs);	
	
		if(syncSt==2){
			srcData.commit();
			tgtData.commit();
			if(aftSQLs != null)	
				srcData.commit();	
		}else {
			srcData.rollback();
			tgtData.rollback();
			if(aftSQLs != null)	
				srcData.rollback();	
		}
		metaData.end(syncSt);
		metaData.saveSyncStats();
		tearDown();
		}
	}
	private static void actType9(int tID, int actId) {
		int syncSt=2;

		if(setupAct(tID, actId)==-1) {
			return;   // something is not right. Do nothing.
		}else {
			metaData.begin();
		
	      int srcRC=srcData.getRecordCount();
	      int tgtRC=tgtData.getRecordCount();
	
			srcData.close();
			tgtData.close();
	
			metaData.end(syncSt);
			metaData.saveSyncStats();
			tearDown();
		}
	}
	
	private static void tearDown() {
		srcData.close();
		tgtData.close();
		metaData.close();
		ovLogger.info("Completed "+jobID+": " +  metaData.getTableID());
	}

	
	
}