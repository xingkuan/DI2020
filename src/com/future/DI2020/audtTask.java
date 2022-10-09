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

class audtTask {
	private static final Logger ovLogger = LogManager.getLogger();
	private static final Matrix matrix = Matrix.getInstance();
	private static final TaskMeta metaData = TaskMeta.getInstance();

	private DataPointMgr dataMgr = DataPointMgr.getInstance();
	
	JSONObject taskDetail = metaData.getTaskDetails();
	
   DataPoint srcData;
   DataPoint tgtData;

   String jobID="audit ";
   
   //202209.27 if need to iterate through a list of tasks, simply call the audit(tId) ...
   public void audit(int taskId) {
	   int srcRC=0;
	   int tgtRC=0;
	   int rowDiff=0;

	   jobID= jobID + taskId;
	   int actId = 9;
		metaData.setupTask(jobID, taskId, actId);

		setup(taskId);
		
		metaData.beginTask();

		ovLogger.info("BEGIN: " + jobID);
		srcRC=srcData.getRecordCount();
		tgtRC=tgtData.getRecordCount();
		ovLogger.info("END: " + jobID);

		rowDiff = srcRC - tgtRC;

		// report to InfluxDB:
		matrix.sendMX(
				"audit,jobId=" + jobID+",srcCnt=" + srcRC 
						+ ",tgtCnt=" + tgtRC 
						+ ",diffCnt="+rowDiff +"\n");
		ovLogger.info("closing tgt. tblID: " + metaData.getTableID() );
		
		metaData.endTask();

		end();
   }
   
   //setup the source and target
   private void setup(int taskId) {
		int actId = 9;  	//auditing
		JSONObject tskDetail;
		String srcTbl,  tgtTbl;
		
		tskDetail = metaData.getTaskDetails();
		
		srcTbl = taskDetail.get("src_tbl").toString();
		tgtTbl = taskDetail.get("tgt_tbl").toString();

		jobID = jobID + taskId + " " + srcTbl + " "+ tgtTbl;
		
		srcData = dataMgr.getDB(tskDetail.get("src_db_id").toString());
		tgtData = dataMgr.getDB(tskDetail.get("tgt_db_id").toString());
		
   }

   private void end() {
		jobID="audit ";
		taskDetail = null;
	   
		srcData.clearData();
		tgtData.clearData();
		
		dataMgr.returnDB(taskDetail.get("src_db_id").toString(), srcData);
		dataMgr.returnDB(taskDetail.get("tgt_db_id").toString(), tgtData);

  }
}