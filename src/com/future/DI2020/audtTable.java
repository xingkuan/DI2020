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

class audtTable {
	   private static final Logger ovLogger = LogManager.getLogger();
	   private static final Metrix metrix = Metrix.getInstance();
	   private static final MetaData metaData = MetaData.getInstance();

   int tableID;
   DataPointer srcData;
   DataPointer tgtData;

   String jobID;
   
   private int totalDelCnt=0, totalInsCnt=0, totalErrCnt=0;
   
   private int giveUp;
   private int kafkaMaxPollRecords;
   private int pollWaitMil;

   //setup the source and target
   private void setup() {
		ovLogger.info(jobID + " " + tableID);
		metaData.setupTableAct(jobID, tableID, 9);  //9: audit
		ovLogger.info("   " + metaData.getTableDetails().get("src_tbl").toString());
		
		JSONObject tblDetail = metaData.getTableDetails();

		srcData = DataPointer.dataPtrCreater(tblDetail.get("srcDBID").toString(), "SRC");
		ovLogger.info("   connected to source: " );

		tgtData = DataPointer.dataPtrCreater(tblDetail.get("srcDBID").toString(), "TGT");
		ovLogger.info("   connected to target");
   }

	
   public void audit(int tblID) {
	   int srcRC;
	   int tgtRC;
	   int rowDiff;

		tableID = tblID;

		setup();
	   
       srcRC=srcData.getRecordCount();
       tgtRC=tgtData.getRecordCount();

       rowDiff = srcRC - tgtRC;
//move to metaData
       /*
      metrix.sendMX("rowDiff,jobId="+jobID+",tblID="+metaData.getSrcTblAb7()+"~"+metaData.getTableID()+" value=" + rowDiff + "\n");
      metrix.sendMX("rowSrc,jobId="+jobID+",tblID="+metaData.getSrcTblAb7()+"~"+metaData.getTableID()+" value=" + srcRC + "\n");
      metrix.sendMX("rowTgt,jobId="+jobID+",tblID="+metaData.getSrcTblAb7()+"~"+metaData.getTableID()+" value=" + tgtRC + "\n");
 */
//TODO: remove it .. it is here for immediate Prod needs. 08/02/2017
      //writeAudit(srcRC, tgtRC, rowDiff);
   }
   
   /*
   Conf conf = Conf.getInstance();
	protected final String logDir = conf.getConf("logDir");
   private void writeAudit(int srcRC, int tgtRC, int diffRC){
	   try{
	   FileWriter fstream = new FileWriter(logDir + "/vSyncAudit.log", true);
	   BufferedWriter out = new BufferedWriter(fstream);
	   out.write("TableID: " + metaData.getTableID() + ", TableName: " + metaData.getSrcSchema() + '.' + metaData.getSrcTable() 
	       + ", srcCnt: " + srcRC 
	       + ", tgtCnt: " + tgtRC
	       + ", diffCnt: " + diffRC
	       + "\r\n");
	   out.close();
   } catch (Exception e){
       System.out.println("Error writing audit file: " + e.getMessage());
    }
   }
   */

   public void close() {
      ovLogger.info("closing tgt. tblID: " + metaData.getTableID() );
  }
}