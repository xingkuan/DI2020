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
	private static final Logger logger = LogManager.getLogger();
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
	 *     0: enable   ;    templates: {"0Enab"}
	 *     1: initial copy; templates: {"1Init"}
	 *     2: sync     ;    templates: {"2Data", "2DCC", "2DATA_"} 
	 *     9: audit    ;    templates: {"9Audit"}
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
		int syncSt = 2; //the desired table state: "2"
		
		logger.info("    BEGIN.");
		JSONObject tblDetail = metaData.getTableDetails();

		if(metaData.setupTableForAction(jobID, tableID, actId)==-1) {
			logger.error("Exit without doing anything.");
			return ;
		}
		logger.info(jobID + " " + tableID + ": " + metaData.getTableDetails().get("src_table").toString());
		
		metaData.begin();
		//based on jobDetail, do the corresponding...
		switch(actId){
			case 0:  //enable table to be actionable.
				jobID = "enableTbl";
				srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
				srcData.miscPrep();  
				logger.info("   src ready: " + metaData.getTableDetails().get("src_table"));

									 //For Oracle (to V), it is enable trigger and curr_state=2;
				srcData.beginDCC();	 //For DB2/AS400 log (to K), set the seq_last_ref, and curr_state=2;
									 //For DB2/AS400 tbl (to V), curr_state=2
				break;
			case 1:  //initial copying of data from src to tgt
				jobID = "initTbl";
				//String tempId="1";
				srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
				srcData.miscPrep();  
				logger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());

				tgtData = DataPoint.dataPtrCreater(tblDetail.get("tgt_db_id").toString(), "TGT");
				tgtData.miscPrep();
				tgtData.setupSink();

				srcData.crtSrcResultSet();
				tgtData.setupSink();
				srcData.copyTo(tgtData);
				//*******
				//srcData.cleanup(actId, aftSQLs);
				break;
			case 2:   //sync DCC, and tbl as well 
				jobID = "syncTbl";

				String tempId = metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("temp_id");
				switch(tempId) {
					case "2DCC":
					case "2DATA":
						srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
						srcData.miscPrep();  //parm is to avoid reading max jrnal seq num when not needed
						logger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());
						int dccCnt = srcData.getDccCnt();
						if(dccCnt==0) {
							logger.info("   no dcc.");
							return ;  
						}

						syncSt=srcData.crtSrcResultSet();
						if(syncSt<0) {
							logger.info("    error in source.");
						}else {
							tgtData.setupSink();
							srcData.copyTo(tgtData);
						}
						break;
					case "2DATA_":
						String auxDBstr = tblDetail.get("dcc_db_id").toString();
						auxData = DataPoint.dataPtrCreater(auxDBstr, "AUX");
						auxData.miscPrep();
						dccCnt = auxData.getDccCnt();
						if(dccCnt==0) {
							logger.info("   no dcc.");
							return ;  
						}
						logger.info("   aux ready: " + metaData.getTableDetails().get("src_table").toString());
						srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
						srcData.miscPrep();  //parm is to avoid reading max jrnal seq num when not needed
						logger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());

						tgtData.setupSink();
						srcData.copyToVia(tgtData,auxData);  
						break;
					default:
						logger.error("wrong template ID");
						break;
				}
				srcData.afterSync();
				tgtData.afterSync();
				
				break;
			case 9:   //audit
				//actType9(tID, actId);
				srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
				srcData.miscPrep();  
				logger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());

				tgtData = DataPoint.dataPtrCreater(tblDetail.get("tgt_db_id").toString(), "TGT");
				tgtData.miscPrep();
				tgtData.setupSink();

				int srcRC=srcData.getRecordCount();
				int tgtRC=tgtData.getRecordCount();
				break;
			default:
				logger.info("unkown action");
				break;
		}
		metaData.end(syncSt);
		metaData.saveSyncStats();
		tearDown();

		logger.info("    End.");
	}
	private static void tearDown() {
		if(!(srcData==null))
			srcData.close();
		if(!(tgtData==null))
			tgtData.close();
		if(!(auxData==null))
			auxData.close();
		metaData.close();
		logger.info("Completed "+jobID+": " +  metaData.getTableID());
	}

	
	
}