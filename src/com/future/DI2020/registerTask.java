package com.future.DI2020;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import com.ibm.as400.access.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

public class registerTask {
	private static final Logger logger = LogManager.getLogger();
	private static final MetaData metaData = MetaData.getInstance();

	private static int taskID;

	static DataPoint srcDB, tgtDB, dccDB;

	public static void main(String[] args) throws IOException {
		System.out.println(args.length);

		if (args.length == 13) {
			taskID = 0; // have to be set later.
		} else if (args.length == 14) {
			taskID = Integer.parseInt(args[13]);
		} else {
			System.out.println(
					"Usage:   RegisterTbl PK TEMPID DB2D JOHNLEE2 TEST1 dccPgm journal VERTX Tsch TEST1 kafka1 topic 0 99");
			System.out.println(
					"   or:   RegisterTbl rrn D2V_ sdbID ssch stbl EXT jrnl tdbid tsch ttbl dccDB topic poolID tblID");
			return;
			/*eg   (tID:   ): DB2RRN DATA_ DB2D JOHNLEE2 TESTTBL2 EXT JOHNLEE2.QSQJRN VERTX TEST TESTTBL2 KAFKA1 JOHNLEE2.TESTTBL2DCC 9 2
			 *eg 2 (tID:  2): ORARID DATA ORA1 VERTSNAP TESTO VERTSNAP.TESTO_DCCTRG VERTSNAP.TESTO_DCCLOG VERTX TEST TESTO na na 9 
			 *eg 1 (tID:  1): ORARID DATA ORA1 VERTSNAP TESTOK VERTSNAP.TESTOK_DCCTRG VERTSNAP.TESTOK_DCCLOG KAFKA1 TEST TESTOK na na 9 1 
			 *eg 3 (tID:3,4): DB2RRN DATA_ DB2D JOHNLEE2 TESTTBL2 EXT JOHNLEE2.QSQJRN KAFKA1 TEST AVROTESTTBL2 KAFKADCC JOHNLEE2.TESTTBL2 9
			 *eg   MM510LIB.INVAUD, ITHAB1JRN.B1JRNE
			 *  1. DB2RRN D2K_ DB2T MM510LIB INVAUD EXT ITHAB1JRN.B1JRNE KAFKA1 TEST AVROINVAUD KAFKA1 DCCINVAUD c:/users/johnlee/ 9 2
			 *  2. data from kafka to ES
			 */  
		}
//		DB2RRN DB2D JOHNLEE2 TESTTBL2 JOHNLEE2.QSQJRN VERTX TEST TESTTBL2 KAFKA1 JOHNLEE2.TESTTBL2 c:/users/johnlee/ 9 2
		String strPK = args[0];
		String strTempId = args[1];
		String srcDBid = args[2];
		String srcSch = args[3];
		String srcTbl = args[4];
		String dccPgm = args[5];
		String dccLog = args[6];
		String tgtDBid = args[7];
		String tgtSch = args[8];
		String tgtTbl = args[9];
		String dccDBid = args[10];
		String dccTopic = args[11];

		String taskCat = "COPY";
		
		System.out.println(Arrays.toString(args));

		int poolID = Integer.parseInt(args[12]);
		//Note: should have table poolID and DCC poolID. For now, DCC poolID is set to -1. 

		if (taskID == 0) {
			taskID = metaData.getNextTblID();
		}
		
		if(!metaData.preRegistCheck(taskID, srcDBid, srcSch, srcTbl, dccDBid)) {
			//Stop; do nothing.
			return;
		}
		srcDB = DataPoint.dataPtrCreater(srcDBid, "SRC");
		if(!srcDB.regSrcCheck(taskID, strPK, srcSch, srcTbl, dccPgm, dccLog, tgtSch, tgtTbl, dccDBid)) {
			//Stop; do nothing.
			return;
		}
		
		String sqlStr;
		sqlStr = "insert into task \n" 
				+ "(TASK_ID, TEMPLATE_ID, TASK_CAT, DATA_PK, \n"
				+ "SRC_DB_ID, SRC_SCHEMA, SRC_TABLE, \n" 
				+ "TGT_DB_ID,TGT_SCHEMA,  TGT_TABLE, \n"
				+ "POOL_ID, CURR_STATE, \n" 
				+ "SRC_DCC_PGM, SRC_DCC_TBL, \n"
				+ "DCC_DB_ID, DCC_STORE, \n" 
				+ "TS_REGIST) \n" 
				+ "values \n"
				+ "(" + taskID + ", '" + strTempId +"', '" + taskCat + "', '" + strPK + "', \n" 
				+ "'" + srcDBid + "', '" + srcSch + "', '" + srcTbl + "', \n" 
				+ "'" + tgtDBid + "', '" + tgtSch + "', '" + tgtTbl + "', \n"
				+ poolID + ", 0, \n"
				+ "'" + dccPgm + "', '" + dccLog + "', \n" 
				+ "'" + dccDBid + "', '" + srcSch + "." + srcTbl + "', \n"
				+ "now()) \n;";
		metaData.runRegSQL(sqlStr);

		boolean rslt;
		//ask srcDB to populate table_field, and whatever
		rslt = srcDB.regSrc(taskID, strPK, srcSch, srcTbl, dccPgm, dccLog, tgtSch, tgtTbl, dccDBid);
		rslt = srcDB.regSrcDcc(taskID, strPK, srcSch, srcTbl, dccPgm, dccLog, tgtSch, tgtTbl, dccDBid);

		//Ask tgtDB to do whatever is needed
		//sqlStr = json.get("tgtTblDDL").toString();
		tgtDB = DataPoint.dataPtrCreater(tgtDBid, "TGT");
		tgtDB.regTgt(taskID, strPK, srcSch, srcTbl, dccPgm, dccLog, tgtSch, tgtTbl, dccDBid);
		//Kafka, ES, JDBC ...
		// eg Kafka, run the following:

		
		//If sync via Kafka, we also ask the dccDB to create the needed topic, and register in meta DB.
		//Object tempO = json.get("repDCCDML");
		if(!dccDBid.equals("na")) {
			dccDB = DataPoint.dataPtrCreater(dccDBid, "DCC");
			//sqlStr = json.get("repDCCDML").toString();
			//dccDB.regSetup(sqlStr);
			dccDB.regDcc(taskID, strPK, srcSch, srcTbl, dccPgm, dccLog, tgtSch, tgtTbl, dccDBid);
		}
				
	}

	

}