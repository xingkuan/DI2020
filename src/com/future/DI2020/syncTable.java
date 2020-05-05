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

	int tableID;
	DataPointer srcData;
	DataPointer tgtData;
	DataPointer auxData;

	String jobID = "syncTbl";

	private int totalDelCnt = 0, totalInsCnt = 0, totalErrCnt = 0;

	private int kafkaMaxPollRecords;
	private int pollWaitMil;

	// setup the source and target
	private void setup() {
		ovLogger.info(jobID + " " + tableID );
		metaData.setupForJob(jobID, tableID);
		
		JSONObject tblDetail = metaData.getTableDetails();
		   
	   srcData = DataPointer.dataPtrCreater(tblDetail.get("srcDBID").toString());
	   tgtData = DataPointer.dataPtrCreater(tblDetail.get("tgtDBID").toString());

	   if( tblDetail.get("auxType") != null)
		   auxData = DataPointer.dataPtrCreater(tblDetail.get("auxDBID").toString());
   }

	public boolean refreshTable(int tblID) throws Exception {
		tableID = tblID;
		setup();
		if (metaData.getCurrState() == 2 || metaData.getCurrState() == 5) {
			setup();
			metaData.markStartTime();

			if (auxData == null)
				tgtData.syncDataFrom(srcData);
			else
				tgtData.syncDataVia(srcData, auxData);

			srcData.close();
			tgtData.close();
			if (auxData != null)
				auxData.close();

			ovLogger.info(jobID + " - " + metaData.getTableDetails().get("src_table"));
			return true;
		} else
		   ovLogger.info("Not in sync mode: " + tableID + " - " + metaData.getTableDetails().get("src_tbl") + ".");
			return false;
	}
}