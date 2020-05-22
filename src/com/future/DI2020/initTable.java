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

class initTable {
	private static String jobID = "initTbl";

	private static final Logger ovLogger = LogManager.getLogger();
	private static final Metrix metrix = Metrix.getInstance();
	private static final MetaData metaData = MetaData.getInstance();

//	private int totalDelCnt = 0, totalInsCnt = 0, totalErrCnt = 0;

	static DataPointer srcData;
	static DataPointer tgtData;

	public static void main(String[] args) {
		System.out.println(args.length);

		if (args.length != 1) {
			System.out.println("Usage:   initTable tblID");
			//return -1;
		}

		int tId = Integer.parseInt(args[0]);
		initializeTgtFromSrc(tId);
		//return 0;
	}

	private static boolean initializeTgtFromSrc(int tId) {
		if(setup(tId)) {
			metaData.markStartTime();
	
			((VerticaData) tgtData).initDataFrom(srcData);
	
			metaData.saveStats();
			metaData.sendMetrix();
			tearDown();
			return true;
		}else {
			return false;
		}
	}


	// setup metaData, source and target
	private static boolean setup(int tID) {
		metaData.setupTableJob(jobID, tID);
		if(metaData.tblReadyForInit()){
			ovLogger.info(jobID + " " + tID + ":" + metaData.getTableDetails().get("src_table").toString());
	
			JSONObject tblDetail = metaData.getTableDetails();
	
			srcData = DataPointer.dataPtrCreater(tblDetail.get("src_db_id").toString());
			srcData.miscPrep();
			srcData.crtSrcResultSet("");
			ovLogger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());
	
			tgtData = DataPointer.dataPtrCreater(tblDetail.get("tgt_db_id").toString());
			tgtData.miscPrep();
			tgtData.setupSink();
			ovLogger.info("   tgt ready: " + metaData.getTableDetails().get("tgt_table").toString());
			return true;
		}else{
			ovLogger.info(jobID + " " + tID + ":" + metaData.getTableDetails().get("src_table").toString() 
					+ ": log file not ready!");
			return false;
		}
	}
	private static void tearDown() {
		srcData.close();
		tgtData.close();
		metaData.close();
		ovLogger.info("Completed "+jobID+": " +  metaData.getTableID());
	}
}