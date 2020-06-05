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

public class RegisterTbl {
	private static final Logger ovLogger = LogManager.getLogger();
	private static final MetaData metaData = MetaData.getInstance();

	private static int tblID;

	static DataPointer srcDB;

	static String srcSQLs = "srcDDL.sql";
	static String crtTblSQL = "tgtTblDDL.sql";
	static String repTblIns = "metaTblDML.sql";
	static String repFldIns = "metaFldDML.sql";
	static String dccDML = "metaDCCDML.sql";
	static String hadRegister = "hadRegistered.sql";
	static String kafka = "kafkaTopic.sh";
	static String db2Journal = "metaDCCDML.sql";

	static String outPath;

	public static void main(String[] args) throws IOException {
		System.out.println(args.length);

		if (args.length == 14) {
			tblID = 0; // have to be set later.
		} else if (args.length == 15) {
			tblID = Integer.parseInt(args[14]);
		} else {
			System.out.println(
					"Usage:   RegisterTbl PK TEMPID DB2D JOHNLEE2 TEST1 dccPgm journal VERTX Tsch TEST1 kafka1 topic c:/users/johnlee 0 99");
			System.out.println(
					"   or:   RegisterTbl rrn D2V_ sdbID ssch stbl EXT jrnl tdbid tsch ttbl dccDB topic opath poolID tblID");
			return;
			//eg 1: DB2RRN D2V_ DB2D JOHNLEE2 TESTTBL2 EXT JOHNLEE2.QSQJRN VERTX TEST TESTTBL2 KAFKA1 JOHNLEE2.TESTTBL2 c:/users/johnlee/ 9 2
			//eg 2: ORARID O2V ORA1 VERTSNAP TESTO VERTSNAP.TESTO_DCCTRG VERTSNAP.TESTO_DCCLOG VERTX TEST TESTO na na c:/users/johnlee/ 9 
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
		outPath = args[12];
		int poolID = Integer.parseInt(args[13]);
		//Note: should have table poolID and DCC poolID. For now, DCC poolID is set to -1. 

		System.out.println(Arrays.toString(args));
		System.out.println(outPath);
		RegisterTbl regTbl = new RegisterTbl();

		srcDB = DataPointer.dataPtrCreater(srcDBid);

		if(dccLog.equals("EXT") ) {    //TODO: So far, that is modeled after DB2/AS400 journal only. May not be that good idea!
			if(!srcDB.regTblMisc(srcSch, srcTbl, dccLog)) {
			System.out.println("Is the journal for " + srcSch + "." + srcTbl + ": " + dccLog + " right?");
			return;
			}
		}

		if (tblID == 0) {
			tblID = regTbl.getNextTblID();
		}
		String sqlStr;
		FileWriter fWriter;

		// make sure tableID is not used
		if (metaData.isNewTblID(tblID)) {
			try {
				sqlStr = "select 'exit already!!!', tbl_id from meta_table where SRC_DB_ID=" + srcDBid + " and SRC_SCHEMA='"
						+ srcSch + "' and SRC_TABLE='" + srcTbl + "';";
				fWriter = new FileWriter(new File(outPath + hadRegister));
				fWriter.write(sqlStr);
				fWriter.close();
				
				sqlStr = "insert into META_TABLE \n" 
						+ "(TBL_ID, TEMP_ID, TBL_PK, \n"
						+ "SRC_DB_ID, SRC_SCHEMA, SRC_TABLE, \n" 
						+ "TGT_DB_ID,TGT_SCHEMA,  TGT_TABLE, \n"
						+ "POOL_ID, CURR_STATE, \n" 
						+ "SRC_DCC_PGM, SRC_DCC_TBL, \n"
						+ "DCC_DB_ID, DCC_STORE, \n" 
						+ "TS_REGIST) \n" 
						+ "values \n"
						+ "(" + tblID + ", '" + strTempId +"', '" + strPK + "', \n" 
						+ "'" + srcDBid + "', '" + srcSch + "', '" + srcTbl + "', \n" 
						+ "'" + tgtDBid + "', '" + tgtSch + "', '" + tgtTbl + "', \n"
						+ poolID + ", 0, \n"
						+ "'" + dccPgm + "', '" + dccLog + "', \n" 
						+ "'" + dccDBid + "', '" + srcSch + "." + srcTbl + "', \n"
						+ "now()) \n;";
				fWriter = new FileWriter(new File(outPath + repTblIns));
				fWriter.write(sqlStr);
				fWriter.close();

				JSONObject json = srcDB.genRegSQLs(tblID, strPK, srcSch, srcTbl, dccPgm, dccLog, tgtSch, tgtTbl, dccDBid);
				// ...
				sqlStr = json.get("repTblFldDML").toString();
				fWriter = new FileWriter(new File(outPath + repFldIns));
				fWriter.write(sqlStr);
				fWriter.close();
				sqlStr = json.get("tgtTblDDL").toString();
				fWriter = new FileWriter(new File(outPath + crtTblSQL));
				fWriter.write(sqlStr);
				fWriter.close();
				//
				Object tempO = json.get("repDCCDML");
				if(tempO!=null) {
					fWriter = new FileWriter(new File(outPath + crtTblSQL));
					sqlStr = json.get("repDCCDML").toString();
					fWriter.write(sqlStr);
					fWriter.close();
				}
				//
				tempO = json.get("srcSQLs");
				if(tempO!=null) {
					sqlStr = tempO.toString();
					fWriter = new FileWriter(new File(outPath + srcSQLs));
					fWriter.write(sqlStr);
					fWriter.close();
				}
				sqlStr = "/opt/kafka/bin/kafka-topics.sh --zookeeper usir1xrvkfk02:2181 --delete --topic " + srcSch + "_"
						+ srcTbl + "\n\n" + "/opt/kafka/bin/kafka-topics.sh --create " + "--zookeeper usir1xrvkfk02:2181 "
						+ "--replication-factor 2 " + "--partitions 2 " + "--config retention.ms=86400000 " + "--topic "
						+ srcSch + "." + srcTbl + " \n";
				fWriter = new FileWriter(new File(outPath + kafka));
				fWriter.write(sqlStr);
				fWriter.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("TableID " + tblID + " has been used already!");
		}
	}

	private int getNextTblID() {
		return metaData.getNextTblID();
	}

}