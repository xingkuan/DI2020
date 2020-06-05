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

		if (args.length == 12) {
			tblID = 0; // have to be set later.
		} else if (args.length == 13) {
			tblID = Integer.parseInt(args[12]);
		} else {
			System.out.println(
					"Usage:   RegisterTbl DB2D JOHNLEE2 TEST1 journal VERTX Tsch TEST1 kafka1 topic c:/users/johnlee 0 99");
			System.out.println(
					"   or:   RegisterTbl sdbID ssch stbl jrnl tdbid tsch ttbl dccDB topic opath poolID tblID");
			return;
		}
//		DB2RRN DB2D JOHNLEE2 TESTTBL2 JOHNLEE2.QSQJRN VERTX TEST TESTTBL2 KAFKA1 JOHNLEE2.TESTTBL2 c:/users/johnlee/ 9 2
		String strPK = args[0];
		String srcDBid = args[1];
		String srcSch = args[2];
		String srcTbl = args[3];
		String jrnlName = args[4];
		String tgtDBid = args[5];
		String tgtSch = args[6];
		String tgtTbl = args[7];
		String dccDBid = args[8];
		String dccTopic = args[9];
		outPath = args[10];
		int poolID = Integer.parseInt(args[11]);
		//Note: should have table poolID and DCC poolID. For now, DCC poolID is set to -1. 

		System.out.println(Arrays.toString(args));
		System.out.println(outPath);
		RegisterTbl regTbl = new RegisterTbl();

		srcDB = DataPointer.dataPtrCreater(srcDBid);

		// TODO: appliable only to DB2/AS400. ... check if the Journal can be accessed
		boolean isJournalOK = srcDB.regTblMisc(srcSch, srcTbl, jrnlName);
		if (isJournalOK) {
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
							+ "(" + tblID + ", 'D2V_', '" + strPK + "', \n" 
							+ "'" + srcDBid + "', '" + srcSch + "', '" + srcTbl + "', \n" 
							+ "'" + tgtDBid + "', '" + tgtSch + "', '" + tgtTbl + "', \n"
							+ poolID + ", 0, \n"
							+ "'EXT', '" + jrnlName + "', \n" 
							+ "'" + dccDBid + "', '" + srcSch + "." + srcTbl + "', \n"
							+ "now()) \n;";
					fWriter = new FileWriter(new File(outPath + repTblIns));
					fWriter.write(sqlStr);
					fWriter.close();

					JSONObject json = srcDB.genRegSQLs(tblID, "DB2RRN", srcSch, srcTbl, jrnlName, tgtSch, tgtTbl, dccDBid);
					// ...
					sqlStr = json.get("fldSQL").toString();
					fWriter = new FileWriter(new File(outPath + repFldIns));
					fWriter.write(sqlStr);
					fWriter.close();
					sqlStr = json.get("crtTbl").toString();
					fWriter = new FileWriter(new File(outPath + crtTblSQL));
					fWriter.write(sqlStr);
					fWriter.close();
					sqlStr = json.get("repDCCTbl").toString();
					fWriter = new FileWriter(new File(outPath + crtTblSQL));
					fWriter.write(sqlStr);
					fWriter.close();
					//
					fWriter = new FileWriter(new File(outPath + dccDML));
					sqlStr = json.get("repDCCTbl").toString();
					fWriter.write(sqlStr);
					sqlStr = json.get("repDCCTblFld").toString();
					fWriter.write(sqlStr);
					fWriter.close();

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
		} else {
			System.out.println("Is the journal for " + srcSch + "." + srcTbl + ": " + jrnlName + " right?");
		}
	}

	private int getNextTblID() {
		return metaData.getNextTblID();
	}

}