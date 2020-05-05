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
	private static String jobID="initTbl";

	private static final Logger ovLogger = LogManager.getLogger();
	private static final Metrix metrix = Metrix.getInstance();
	private static final MetaData metaData = MetaData.getInstance();

	int tableID;

//	private int totalDelCnt = 0, totalInsCnt = 0, totalErrCnt = 0;

	DataPointer srcData;
	DataPointer tgtData;

	// setup the source and target
	private void setup() {
		ovLogger.info(jobID + " " + tableID + metaData.getTableDetails().get("src_tbl").toString());
		metaData.setupForJob(jobID, tableID);
		
		JSONObject tblDetail = metaData.getTableDetails();

		srcData = DataPointer.dataPtrCreater(tblDetail.get("srcDBID").toString());
		ovLogger.info("   connected to source: " );

		tgtData = DataPointer.dataPtrCreater(tblDetail.get("srcDBID").toString());
		ovLogger.info("   connected to target");
	}

	public boolean initializeTable(int tID) {
		tableID = tID;

		setup();
		initializeTgtFromSrc();

		return true;
	}


	private boolean initializeTgtFromSrc() {
		boolean rtv = true;

		srcData.miscPrep();
		ovLogger.info("      src ready: " + metaData.getTableDetails().get("src_tbl").toString() );

		tgtData.miscPrep();
		tgtData.setupSinkData();
		ovLogger.info("      tgt ready: " + metaData.getTgtTable() );

		metaData.markStartTime();

		((VerticaData) tgtData).initDataFrom(srcData);

		return rtv;
	}

/*
	public boolean reInitializeTable(int tblID) {


		return true;
	}

	public boolean tblInitType2() {
		if (tblMeta.getCurrState() == 0) {
			// initialize table
			ovLogger.info("JobID: " + jobID + ", tblID: " + tblMeta.getTableID() + " init type 2");
			tblMeta.setCurrentState(1); // set current state to initializing

			tblMeta.markStartTime();
			try {
				tblTgt.truncate();
				tblSrc.setTriggerOn();
			} catch (SQLException e) {

			}
		} else {
			ovLogger.error("JobID: " + jobID + ", tblID: " + tblMeta.getTableID()
					+ " Cannot initialize... not in correct state");
		}
		return true;
	}

	public void tblLoadSwap() {
		tblMeta.setTgtUseAlt();
		if (tblInitType1()) {
			ovLogger.info("tblID: " + tblMeta.getTableID() + " Init successful. JobID: " + jobID);
		} else {
			ovLogger.info("tblID: " + tblMeta.getTableID() + " Init failed. JobID: " + jobID);
		}
		tblTgt.swapTable();
	}
*/
	public void close() {
		ovLogger.info("closing tgt. tblID: " + metaData.getTableID());
	}
}