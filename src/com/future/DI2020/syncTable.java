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

class syncTable {
	private static final Logger ovLogger = LogManager.getLogger();
	private static final Metrix metrix = Metrix.getInstance();
	private static final MetaData metaData = MetaData.getInstance();

	static int tableID;
	static DataPointer srcData;
	static DataPointer tgtData;
	static DataPointer auxData;


	static String jobID = "syncTbl";

	private int totalDelCnt = 0, totalInsCnt = 0, totalErrCnt = 0;

	private int kafkaMaxPollRecords;
	private int pollWaitMil;

	public static void main(String[] args) {
		System.out.println(args.length);

		if (args.length != 2) {
			System.out.println("Usage:   syncTable <tbl|pool> id");
			//return -1;
		}

		String parmCat = args[0];
		int parmId = Integer.parseInt(args[1]);
		
		if(parmCat.contentEquals("pool"))
			syncByPool(parmId);
		else if(parmCat.contentEquals("tbl"))
			syncTable(parmId);
		else 
			System.out.println("Usage:   syncTable <tbl|pool> id");
			
	}
	static void syncByPool(int pID) {
		List<Integer> tblList = metaData.getTblsByPoolID(pID);
		for (int i : tblList) {
            syncTable(i);
        }
	}
	static void syncTable(int tID) {
		int syncSt=2;
		setup(tID);

		ovLogger.info("    START...");
		int ok = metaData.begin(2);
		if(ok == 1) {
			if (auxData == null)
				syncSt = tgtData.syncDataFrom(srcData);
			else
				syncSt = tgtData.syncDataVia(srcData, auxData);

			srcData.close();
			tgtData.close();
			if (auxData != null)
				auxData.close();

			metaData.end(syncSt);
			metaData.saveSyncStats();
			tearDown();
		} else {
		   ovLogger.info("Table not in sync mode: " + tableID + " - " + metaData.getTableDetails().get("src_tbl") + ".");
			//return false;
		}
		ovLogger.info("    COMPLETE.");
	}


	
	// setup the source and target
	private static void setup(int tblID) {
		tableID=tblID;
		metaData.setupTableJob(jobID, tableID);
		ovLogger.info(jobID + " " + tableID + ":" + metaData.getTableDetails().get("src_table").toString());

		JSONObject tblDetail = metaData.getTableDetails();

		srcData = DataPointer.dataPtrCreater(tblDetail.get("src_db_id").toString());
		srcData.miscPrep();
		ovLogger.info("   src ready: " + metaData.getTableDetails().get("src_table").toString());

		tgtData = DataPointer.dataPtrCreater(tblDetail.get("tgt_db_id").toString());
		tgtData.miscPrep();
		tgtData.setupSink();
		ovLogger.info("   tgt ready: " + metaData.getTableDetails().get("tgt_table").toString());

   }
	private static void tearDown() {
		srcData.close();
		tgtData.close();
		metaData.close();
		ovLogger.info("Completed "+jobID+": " +  metaData.getTableID());
	}

	
	
}