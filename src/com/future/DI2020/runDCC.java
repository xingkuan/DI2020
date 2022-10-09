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

class runDCC {
	private static final Logger ovLogger = LogManager.getLogger();
	private static final Matrix metrix = Matrix.getInstance();
	private static final TaskMeta metaData = TaskMeta.getInstance();

	static DataPointMgr srcData;
	static DataPointMgr dccData;

	static String jobID = "runDCC";

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage:   syncTable <tbl|pool> id");
			//return -1;
		}

		String parmCat = args[0];
		int parmId = Integer.parseInt(args[1]);
		
		if(parmCat.contentEquals("pool"))
			syncByPool(parmId);
		else if(parmCat.contentEquals("tbl"))
			syncDCC1(parmId);
		else 
			System.out.println("Usage:   runDCC <tbl|pool> id");
	}
	static void syncByPool(int pID) {
		JSONArray jList = metaData.getDCCsByPoolID(pID);
		jList.forEach(j ->{JSONObject jo = (JSONObject)j;
            //syncDCC1(jo.get("SRC_DB_ID").toString(), jo.get("SRC_SCHEMA").toString(), jo.get("SRC_TABLE").toString(), jo.get("TGT_DB_ID").toString());
            syncDCC1(Integer.parseInt(jo.get("TBL_ID").toString()));
        });
	}
	
	//static void syncDCC1(String srcDB, String lName, String jName, String dccDB) {
	static void syncDCC1(int tblID) {
		int syncSt=2;
		setup(tblID);
		ovLogger.info("    START.");
		int ok = metaData.begin();
		if(ok == 1) {
			int proceed = srcData.crtSrcResultSet();
			if(proceed>0) {
				ResultSet rsltSet = srcData.getSrcResultSet();
				syncSt = dccData.syncDataFrom(srcData);
			}
			metaData.end(syncSt);
			metaData.saveInitStats();
	
			ovLogger.info(jobID + " - " + metaData.getTaskDetails().get("src_table"));
			tearDown();
		} else {
			   ovLogger.info("Table not in sync mode: " + jName + ".");
				//return false;
		}
		ovLogger.info("    COMPLETE.");
	}
	
	// setup the source and target
	//private static void setup(String srcDB, String lName, String jName, String dccDB) {
	private static void setup(int tblID) {
		//metaData.setupDCCJob(jobID, srcDB, lName, jName, dccDB);
		metaData.setupTableJob(jobID, tblID);
		ovLogger.info(jobID + " " + metaData.getTaskDetails().get("src_db_id") + "." 
					+ metaData.getTaskDetails().get("src_schema") + "."
					+ metaData.getTaskDetails().get("src_table"));

		//JSONObject dccDetail = metaData.getDCCDetails();

		srcData = DataPointMgr.dataPtrCreater(srcDB);
		srcData.miscPrep();
		ovLogger.info("   src ready.");

		dccData = DataPointMgr.dataPtrCreater(dccDB);
		dccData.miscPrep();
		dccData.setupSink();
		ovLogger.info("   tgt ready");

   }
	private static void tearDown() {
		srcData.close();
		dccData.close();
		metaData.close();
		ovLogger.info("Completed "+jobID+": " +  metaData.getTableID());
	}
	
}