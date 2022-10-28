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

	private static int initializeTgtFromSrc(int taskId) {
		int actId = 0;
		int ok;
		
		jobID = jobID + taskId ;

		ok=taskMeta.setupTask(jobID, taskId, actId);
		if(ok==-1) {
			return -1;
		}
		ok=setup(taskId);  
		if(ok==-1) {
			return -1;
		}

		//ResultSet rsltSet = srcData.getData();
		JSONObject rslt = srcData.syncTo(tgtData);

		srcData.clearState();
		tgtData.clearState();
			
		taskMeta.setTaskState(9);
		taskMeta.endTask();
			
		tearDown();

		ovLogger.info("    COMPLETE.");

		return 0;
	}


	// setup metaData, source and target
	private static int setup(int taskID) {
		int actId = 0;
		JSONObject tskDetail = taskMeta.getTaskDetails();

		if(taskMeta.isTaskReadyFor(actId)){
			ovLogger.info(jobID + " " + taskID + ":" + tskDetail.get("src_table").toString());

			JSONObject srcDetailJSON = (JSONObject) tskDetail.get("SRC");
			JSONObject tgtDetailJSON = (JSONObject) tskDetail.get("TGT");
			
			JSONArray srcInstr =  (JSONArray) ((JSONObject)(tskDetail.get("src_db_id"))).get("init");
			JSONArray tgtInstr =  (JSONArray) ((JSONObject)(tskDetail.get("tgt_db_id"))).get("init");

			srcData = dataMgr.getDB(tskDetail.get("src_db_id").toString());
			String sqlStr, type;
			JSONObject rslt;
			Iterator<JSONObject> it = srcInstr.iterator();
			while (it.hasNext()) {
				sqlStr= (String) it.next().get("stmt");
				type= (String) it.next().get("type");
				System.out.println(sqlStr );

				rslt = srcData.runDBcmd(sqlStr, type);
			}
			ovLogger.info("   src ready: " + tskDetail.get("src_table").toString());
	
			tgtData = dataMgr.getDB(tskDetail.get("tgt_db_id").toString());
			it = tgtInstr.iterator();
			while (it.hasNext()) {
				sqlStr= (String) it.next().get("stmt");
				type= (String) it.next().get("type");
				System.out.println(sqlStr );

				rslt = tgtData.runDBcmd(sqlStr, type);
			}
			ovLogger.info("   tgt ready: " + tskDetail.get("tgt_table").toString());
			return 0;
		}else{
			ovLogger.info(jobID + ":" + tskDetail.get("src_table").toString() 
					+ ": log file not ready!");
			return -1;
		}
	}
	private static void tearDown() {
		srcData.closeDB();
		tgtData.closeDB();
		taskMeta.close();
		ovLogger.info("Completed "+jobID+": " +  taskMeta.getTableID());
	}
}