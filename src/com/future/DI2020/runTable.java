package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.time.Duration;
import java.sql.*;

import org.apache.logging.log4j.Logger;
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
	static DataPointer srcData;
	static DataPointer tgtData;
	static DataPointer auxData;


	static String jobID = "syncTbl";
	static int jobSub=3;

	private int totalDelCnt = 0, totalInsCnt = 0, totalErrCnt = 0;

	private int kafkaMaxPollRecords;
	private int pollWaitMil;

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

		//based on jobDetail, do the corresponding...
		switch(actId){
			case 0:  //enable table to be actionable.
				ovLogger.info("move the enableTbl.java here");
				actType0(tID, actId);
				break;
			case 1:  //initial copying of data from src to tgt
				//ovLogger.info("move initTable.java here");
				actType1(tID, actId);
				break;
			case 2:   //sync DCC 
				ovLogger.info("move runDCC.java here");
				actType2(tID, actId);
				break;
			case 3:  //cann't it be handled in case 2?
				actType3(tID, actId);
				break;
			case 4:   //cann't it be handled in case 2?
				actType4(tID, actId);
				break;
			default:
				ovLogger.info("unkown action");
				break;
		}
	}

	private static int setup(int tblID, int actId) {
		tableID=tblID;
		if(metaData.setupTableAct(jobID, tableID, actId)==-1) {
			ovLogger.error("Did not do anything.");
			return -1;
		}
		ovLogger.info(jobID + " " + tableID + ":" + metaData.getTableDetails().get("src_table").toString());

		JSONObject tblDetail = metaData.getTableDetails();

		//TODO: no need to access JOURNAL! modify to avoid reading max journal seq num!!!
		srcData = DataPointer.dataPtrCreater(tblDetail.get("src_db_id").toString());
		srcData.miscPrep(tblDetail.get("temp_id").toString());
		ovLogger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());

		tgtData = DataPointer.dataPtrCreater(tblDetail.get("tgt_db_id").toString());
		tgtData.miscPrep(tblDetail.get("temp_id").toString());
		tgtData.setupSink();
		ovLogger.info("   tgt ready: " + metaData.getTableDetails().get("tgt_table").toString());
		
		String auxDBstr = tblDetail.get("dcc_db_id").toString();
		if((!auxDBstr.equals("")) && (!auxDBstr.equals("na"))) {
			auxData = DataPointer.dataPtrCreater(auxDBstr);
			auxData.miscPrep(tblDetail.get("temp_id").toString());
			ovLogger.info("   aux ready: " + metaData.getTableDetails().get("src_table").toString());
		}
		return 0;
   }
	
	private static void actType0(int tID, int actId) {
		int syncSt=2;

		if(setup(tID, actId)==-1) {
			return;   // something is not right. Do nothing.
		}else {
			ovLogger.info("    BEGIN.");
			metaData.begin();
	
			srcData.beginDCC();  //For Oracle (to V), it is enable trigger and curr_state=2;
								 //For DB2/AS400 log (to K), set the seq_last_ref, and curr_state=2;
								 //For DB2/AS400 tbl (to V), curr_state=2
			metaData.end(syncSt);
			metaData.saveSyncStats();

			ovLogger.info("    END.");
		}
	}
	private static void actType1(int tID, int actId) {
		int syncSt=2;

		if(setup(tID, actId)==-1) {
			return;   // something is not right. Do nothing.
		}else {
			ovLogger.info("    BEGIN.");
			metaData.begin();
		
			srcData.crtSrcResultSet("");
			int state = tgtData.initDataFrom(srcData);
	
			srcData.close();
			tgtData.close();
	
			metaData.end(syncSt);
			metaData.saveSyncStats();
			tearDown();
		
			ovLogger.info("    END.");
		}
	}
	private static void actType2(int tID, int actId) {
		int syncSt=2;

		if(setup(tID, actId)==-1) {
			return;   // something is not right. Do nothing.
		}else {
			ovLogger.info("    BEGIN.");
			metaData.begin();
		
			int state=srcData.crtSrcResultSet("");
			if(state==-2) {
				ovLogger.info("    no change.");
			}else {
				state = tgtData.syncDataFrom(srcData);
			}
			srcData.close();
			tgtData.close();
	
			metaData.end(syncSt);
			metaData.saveSyncStats();
			tearDown();
		
			ovLogger.info("    END.");
		}
	}
	private static void actType3(int tID, int actId) {
		int syncSt=2;

		ovLogger.info("    BEGIN.");
		setup(tID, actId);
		metaData.begin();
	
		syncSt = tgtData.syncDataFrom(srcData);

		srcData.close();
		tgtData.close();
	
		metaData.end(syncSt);
		metaData.saveSyncStats();
		tearDown();
	
		ovLogger.info("    END.");
	}
	private static void actType4(int tID, int actId) {
		int syncSt=2;

		ovLogger.info("    BEGIN.");
		setup(tID, actId);
		metaData.begin();
	
		auxData.crtAuxSrcAsList();
		syncSt = tgtData.syncDataViaV2(srcData, auxData);

		srcData.close();
		tgtData.close();
		auxData.close();
	
		metaData.end(syncSt);
		metaData.saveSyncStats();
		tearDown();
	
		ovLogger.info("    END.");
	}
	
	private static void tearDown() {
		srcData.close();
		tgtData.close();
		metaData.close();
		ovLogger.info("Completed "+jobID+": " +  metaData.getTableID());
	}

	
	
}