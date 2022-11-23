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

	private static final TaskMeta taskMeta = TaskMeta.getInstance();
	private DataPointMgr dataMgr = DataPointMgr.getInstance();
	
	//static int tableID;
	static DataPoint srcData;
	static DataPoint tgtData;
	static DataPoint dccData;

	JSONObject srcDetailJSON;
	JSONObject tgtDetailJSON;
	JSONObject dccDetailJSON;

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
		List<Integer> tblList = taskMeta.getTblsByPoolID(poolID);
		for (int i : tblList) {
            actOnTask(i, actId);
        }
	}

	void actOnTask(int tID, int actId) {
		jobID = jobID + tID;
		int syncSt = 2; //the desired table state: "2"

		setupTask(jobID, tID);
		
		JSONObject matrix = srcData.syncTo(tgtData);   //20220927TODO this step should take instructions from meta and do it
		
		taskMeta.setTaskState(9);
		taskMeta.endTask();
		
		logger.info("END.");
	}
	
	  private void setupTask(String jobID, int taskId) {
		int actId = 1;  	//data pumping
		DataPointMgr dataMgr=DataPointMgr.getInstance();	
		taskMeta.setupTask(jobID, taskId, actId);
		Map tskDetail = taskMeta.getTaskDetails();

		if(taskMeta.isTaskReadyFor(actId)){
			srcDetailJSON = (JSONObject) tskDetail.get("SRC");
			tgtDetailJSON = (JSONObject) tskDetail.get("TGT");
			dccDetailJSON = (JSONObject) tskDetail.get("DCC");
				
			jobID = jobID + taskId + " " + ((JSONObject)(srcDetailJSON.get("dbid"))).get("TBL") ;
	
			srcData = dataMgr.getDB(srcDetailJSON.get("dbid").toString());
			tgtData = dataMgr.getDB(tgtDetailJSON.get("dbid").toString());

			if(dccDetailJSON != null) { //data keys from, eg kafka
				dccData = dataMgr.getDB(dccDetailJSON.get("dbid").toString());
				dccData.setDetail(dccDetailJSON);
			}
			srcData.setDetail(srcDetailJSON);
			tgtData.setDetail(tgtDetailJSON);
		}
		logger.info("Completed "+jobID+": " +  taskMeta.getTableID());
	}
	
	private void endTask() {
		Map tskDetail = taskMeta.getTaskDetails();

		srcData.clearState();
		tgtData.clearState();
		
		dataMgr.returnDB(tskDetail.get("src_db_id").toString(), srcData);
		dataMgr.returnDB(tskDetail.get("tgt_db_id").toString(), tgtData);
		
		if(null!=dccData) {
			dccData.clearState();
			dataMgr.returnDB(tskDetail.get("ccd_db_id").toString(), dccData);
		}

		jobID="audit ";
	}

	
	
}