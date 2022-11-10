package com.future.DI2020;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

class xformApp
{
	private static final Logger logger = LogManager.getLogger();
	private static final Matrix metrix = Matrix.getInstance();
	private static final TaskMeta metaData = TaskMeta.getInstance();

	static DataPoint srcData;
	static DataPoint tgtData;

	static String jobID ;

   /*
    * goal: Read data from Kafka that needs only simple Transformation;
    *       and pump into ElasticSearch.
    *       The reason for ES is because of its TS nature and Kibana that 
    *       can answer a lot of business needs 
    */
   public static void main (String args[]) {   
		System.out.println(args.length);

		if (args.length != 2) {
			System.out.println("Usage:   xformData <tbl|pool> id");
			//return -1;
		}

		String parmCat = args[0];
		int parmId = Integer.parseInt(args[1]);
		
		if(parmCat.contentEquals("pool"))
			transforms(parmId);
		else if(parmCat.contentEquals("tbl"))
			transform(parmId);
		else 
			System.out.println("Usage:   syncTable <tbl|pool> oId aId");
			
	}
   
	static void transforms(int poolID) {
		List<Integer> tblList = metaData.getTblsByPoolID(poolID);
		for (int i : tblList) {
           transform(i);
       }
	   return ;
   }

	static void transform(int tblId) {
		jobID = "xform";
		TaskMeta metaData = TaskMeta.getInstance();
		DataPointMgr dataMgr = DataPointMgr.getInstance();
		
		metaData.setupTask(jobID, tblId, 11);  // actId for dev activities.
		
		JSONObject tskDetail = metaData.getTaskDetails();
		
		xformEngine xformEng = new xformEngine();
		xformEng.setupScripts();

		srcData = (KafkaData) dataMgr.getDB(tskDetail.get("src_db_id").toString());
		//srcData.testConsumer();
		tgtData = (ESData) dataMgr.getDB(tskDetail.get("tgt_db_id").toString());
		//tgtData.test();
		srcData.setupXformEngine(xformEng);
		//tgtData.setupSink();
		//int cnt=srcData.xformInto(tgtData);
		//srcData.test();
		//if(cnt>0)
			srcData.syncTo(tgtData);
		
		srcData.closeDB();
		tgtData.closeDB();
		
		return;
	}
   
}