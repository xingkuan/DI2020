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

/*
  class: initTable
*/

class initTask {
	private static String jobID = "initTbl ";

	private static final Logger ovLogger = LogManager.getLogger();
//	private static final Metrix metrix = Metrix.getInstance();
	private static final TaskMeta taskMeta = TaskMeta.getInstance();

	private static final DataPointMgr dataMgr = DataPointMgr.getInstance();
//	private int totalDelCnt = 0, totalInsCnt = 0, totalErrCnt = 0;

	static DataPoint srcData;
	static DataPoint tgtData;

	public static void main(String[] args) {
		//System.out.println(args.length);

		if (args.length != 1) {
			System.out.println("Usage: initTask tskID");
			//return -1;
		}

		int tskId = Integer.parseInt(args[0]);
		initializeTgtFromSrc(tskId);  
		//return 0;
	}

	private static boolean initializeTgtFromSrc(int taskId) {
		int actId = 0;
		jobID = jobID + taskId ;

		taskMeta.setupTask(jobID, taskId, actId);

		setup(taskId);  

		int ok = taskMeta.beginTask();
		if(ok == 1) {
			//ResultSet rsltSet = srcData.getData();
			int state = tgtData.initDataFrom(srcData);
			
			srcData.miscPrep();  
			srcData.beginDCC();	 //For DB2/AS400 log (to K), set the seq_last_ref, and curr_state=2;
								 //For DB2/AS400 tbl (to V), curr_state=2
			
			srcData.clearState();
			tgtData.clearState();
			
			taskMeta.setTaskState(9);
			taskMeta.endTask();
			
			tearDown();
		}else {
			ovLogger.info("    Table not in the right state.");
		}
		ovLogger.info("    COMPLETE.");

		return true;
	}


	// setup metaData, source and target
	private static boolean setup(int taskID) {
		int actId = 0;
		JSONObject tskDetail = taskMeta.getTaskDetails();

		if(taskMeta.isTaskReadyFor(actId)){
			ovLogger.info(jobID + " " + taskID + ":" + tskDetail.get("src_table").toString());
	
			JSONObject srcInstr =  taskMeta.getInstrs((String) tskDetail.get("src_db_id"));
			JSONObject tgtInstr =  taskMeta.getInstrs((String) tskDetail.get("tgt_db_id"));

			srcData = dataMgr.getDB(tskDetail.get("src_db_id").toString());
			srcData.prep(srcInstr);
			ovLogger.info("   src ready: " + tskDetail.get("src_table").toString());
	
			tgtData = dataMgr.getDB(tskDetail.get("tgt_db_id").toString());
			tgtData.prep(tgtInstr);
			tgtData.setupSink();
			ovLogger.info("   tgt ready: " + tskDetail.get("tgt_table").toString());
			return true;
		}else{
			ovLogger.info(jobID + ":" + tskDetail.get("src_table").toString() 
					+ ": log file not ready!");
			return false;
		}
	}
	private static void tearDown() {
		srcData.closeDB();
		tgtData.closeDB();
		taskMeta.close();
		ovLogger.info("Completed "+jobID+": " +  taskMeta.getTableID());
	}
}