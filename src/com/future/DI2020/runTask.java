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

class runTask {
	private static final Logger logger = LogManager.getLogger();

	private static final TaskMeta metaData = TaskMeta.getInstance();
	private DataPointMgr dataMgr = DataPointMgr.getInstance();
	
	//static int tableID;
	static DataPoint srcData;
	static DataPoint tgtData;
	static DataPoint auxData;

	JSONObject taskDetail;

	static String jobID ;
	static int jobSub=3;

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
	public void main(String[] args) {
		System.out.println(args.length);

		if (args.length != 3) {
			System.out.println("Usage:   runTask <tbl|pool> id aID");
			//return -1;
		}

		String parmCat = args[0];
		int parmId = Integer.parseInt(args[1]);
		int actId = Integer.parseInt(args[2]);
		
		if(parmCat.contentEquals("pool"))
			actOnTasks(parmId, actId);
		else if(parmCat.contentEquals("tbl"))
			actOnTask(parmId, actId);
		else 
			System.out.println("Usage:   syncTable <tbl|pool> oId aId");
			
	}
	
	void actOnTasks(int poolID, int actId) {
		List<Integer> tblList = metaData.getTblsByPoolID(poolID);
		for (int i : tblList) {
            actOnTask(i, actId);
        }
	}

	void actOnTask(int tID, int actId) {
		jobID = jobID + tID;
		metaData.setJobName(jobID);

		int syncSt = 2; //the desired table state: "2"

		setup(tID);
		
		metaData.beginTask();

		/*
		logger.info("BEGIN: " + metaData.getTaskDetails().get("src_table").toString());

		int dccCnt = srcData.getDccCnt();
		if(dccCnt==0) {
			logger.info("   no dcc.");
			break ;  
		}

		int cnt=srcData.crtSrcResultSet();
		if(cnt<0) {
			logger.info("    error in source.");
		}else {
			tgtData.sync(srcData);
		}
		srcData.afterSync();
		tgtData.afterSync();

		if(srcData!=null)
			srcData.commit();
		if(tgtData!=null)
			tgtData.commit();
*/
		tgtData.sync(srcData);   //20220927TODO this step should take instructions from meta and do it

		metaData.setTaskState(9);
		metaData.endTask();
		
		logger.info("END.");
	}
	
	  private void setup(int taskId) {
		int actId = 1;  	//data pumping
		String srcTbl,  tgtTbl;
			
		metaData.setupTaskForAction(jobID, taskId, actId);
		if(metaData.isTaskReadyFor(actId)){
			taskDetail = metaData.getTaskDetails();
				
			srcTbl = taskDetail.get("src_tbl").toString();
			tgtTbl = taskDetail.get("tgt_tbl").toString();
	
			jobID = jobID + taskId + " " + srcTbl + " "+ tgtTbl;
	
			srcData = DataPointMgr.getDB(taskDetail.get("src_db_id").toString());

			JSONObject srcInstr = (JSONObject) taskDetail.get("srcInstruction");
			srcData.prep(srcInstr);
	
			tgtData = DataPointMgr.getDB(taskDetail.get("tgt_db_id").toString());
			JSONObject tgtInstr = (JSONObject) taskDetail.get("srcInstruction");
			tgtData.prep(tgtInstr);
	
			tgtData = DataPointMgr.getDB(taskDetail.get("tgt_db_id").toString());
		}
		logger.info("Completed "+jobID+": " +  metaData.getTableID());
	}
	
	private void endTask() {
		jobID="audit ";
		taskDetail = null;
		
		srcData.clearData();
		tgtData.clearData();
		
		dataMgr.returnDB(taskDetail.get("src_db_id").toString(), srcData);
		dataMgr.returnDB(taskDetail.get("tgt_db_id").toString(), tgtData);
	}

	
	
}