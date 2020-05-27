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

	static String crtTblSQL = "verticaDDL.sql";
	static String repTblIns = "repoTblDML.sql";
	static String repFldIns = "repoColsDML.sql";
	static String hadRegister = "hadRegistered.sql";
	static String kafka = "kafkaTopic.sh";
	static String db2Journal = "repoAuxDML.sql";

	static String outPath;

	public static void main(String[] args) throws IOException {
		System.out.println(args.length);

		if (args.length == 11) {
			tblID = 0; // have to be set later.
		} else if (args.length == 12) {
			tblID = Integer.parseInt(args[11]);
		} else {
			System.out.println(
					"Usage:   RegisterTbl DB2D JOHNLEE2 TEST1 journal VERTX Tsch TEST1 kafka1 topic c:/users/johnlee 0 99");
			System.out.println(
					"   or:   RegisterTbl sdbID ssch stbl jrnl tdbid tsch ttbl auxDB topic opath poolID tblID");
			return;
		}

		String srcDBid = args[0];
		String srcSch = args[1];
		String srcTbl = args[2];
		String jrnlName = args[3];
		String tgtDBid = args[4];
		String tgtSch = args[5];
		String tgtTbl = args[6];
		String auxDBid = args[7];
		String auxTopic = args[8];
		outPath = args[9];
		int poolID = Integer.parseInt(args[10]);

		System.out.println(Arrays.toString(args));
		System.out.println(outPath);
		RegisterTbl regTbl = new RegisterTbl();

		srcDB = DataPointer.dataPtrCreater(srcDBid);

		// check if the Journal can be accessed
		boolean isJournalOK = srcDB.regTblMisc(srcSch, srcTbl, jrnlName);
		if (isJournalOK) {
			if (tblID == 0) {
				tblID = regTbl.getNextTblID();
			}

			// make sure tableID is not used
			if (metaData.isNewTblID(tblID)) {
				regTbl.genRepTblDML(srcDBid, srcSch, srcTbl, auxDBid, jrnlName, tgtDBid, tgtSch, tgtTbl, poolID);
				regTbl.genRegSQLs(srcDBid, srcSch, srcTbl, auxDBid, jrnlName, tgtDBid, tgtSch, tgtTbl, poolID);
				regTbl.genMisc(srcDBid, srcSch, srcTbl, auxDBid, jrnlName, tgtDBid, tgtSch, tgtTbl, poolID);

			} else {
				System.out.println("TableID " + tblID + " has been used already!");
			}
		} else {
			System.out.println("Is the journal for " + srcSch + "." + srcTbl + ": " + jrnlName + " right?");
		}
	}

	private boolean genRepTblDML(String srcDBid, String srcSch, String srcTbl, String auxDBid, String journal, String tgtDBid, String tgtSch, String tgtTbl, int poolID) {
		FileWriter repoInsTbl;

		String sqlRepoDML1 = "insert into META_TABLE \n" 
				+ "(TBL_ID, TBL_PK, \n"
				+ "SRC_DB_ID, SRC_SCHEMA, SRC_TABLE, \n" 
				+ "TGT_DB_ID,TGT_SCHEMA,  TGT_TABLE, \n"
				+ "POOL_ID, INIT_DT, INIT_DURATION, \n" 
				+ "CURR_STATE, \n" 
				+ "AUX_DB_ID, AUX_PRG_TYPE, \n"
				+ "SRC_JURL_NAME, AUX_PRG_NAME, AUX_CHG_TOPIC, \n" 
				+ "TS_REGIST, TS_LAST_REF, SEQ_LAST_REF) \n" + "values \n"
				+ "(" + tblID + ", 'DB2RRN', '" + srcDBid + "', '" + srcSch + "', '" + srcTbl + "', \n" 
				+ "'" + tgtDBid + "', '" + tgtSch + "', '" + tgtTbl + "', \n"
				+ " 0, null, null, \n"
				+ " 0, \n" 
				+ "'" + auxDBid + "', 'Ext Java', \n" 
				+ "'" + journal + "', 'Java', 'topic', \n"
				+ "CURRENT_TIMESTAMP, null, null) \n;";
		try {
			repoInsTbl = new FileWriter(new File(outPath + repTblIns));
			repoInsTbl.write(sqlRepoDML1);
			repoInsTbl.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	private boolean genRegSQLs(String srcDBid, String srcSch, String srcTbl, String auxDBid, String journal, String tgtDBid, String tgtSch, String tgtTbl, int poolID) {
		FileWriter verticaDDL;
		FileWriter repoInsCols;

		JSONObject json = ((DB2Data400) srcDB).genRegSQLs(tblID, srcSch, srcTbl, tgtSch, tgtTbl);
		String sqlStr;

		try {
			verticaDDL = new FileWriter(new File(outPath + crtTblSQL));
			repoInsCols = new FileWriter(new File(outPath + repFldIns));

			sqlStr = json.get("crtTbl").toString();
			verticaDDL.write(sqlStr);
			verticaDDL.close();
			
			sqlStr = json.get("fldSQL").toString();
			repoInsCols.write(sqlStr);
			repoInsCols.close();
			
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	private boolean genMisc(String srcDBid, String srcSch, String srcTbl, String auxDBid, String journal, String tgtDBid, String tgtSch,
			String tgtTbl, int poolID) {
		FileWriter hadRegistered;
		FileWriter kafkaTopic;

		
		try {
			String strText;
			hadRegistered = new FileWriter(new File(outPath + hadRegister));
			strText = "select 'exit already!!!', tbl_id from meta_table where SRC_DB_ID=" + srcDBid + " and SRC_SCHEMA='"
					+ srcSch + "' and SRC_TABLE='" + srcTbl + "';";
			hadRegistered.write(strText);
			hadRegistered.close();

			// generate command for create kafka topic
			kafkaTopic = new FileWriter(new File(outPath + kafka));
			strText = "/opt/kafka/bin/kafka-topics.sh --zookeeper usir1xrvkfk02:2181 --delete --topic " + srcSch + "."
					+ srcTbl + "\n\n" + "/opt/kafka/bin/kafka-topics.sh --create " + "--zookeeper usir1xrvkfk02:2181 "
					+ "--replication-factor 2 " + "--partitions 2 " + "--config retention.ms=86400000 " + "--topic "
					+ srcSch + "." + srcTbl + " \n";
			kafkaTopic.write(strText);
			kafkaTopic.close();
			
			FileWriter repoJournalRow = new FileWriter(new File(outPath + db2Journal));
			String[] res = journal.split("[.]", 0);
			String lName = res[0];
			String jName = res[1];
			String jRow = "insert into META_TABLE \n"
					+ "(TBL_ID, TBL_PK, \n"
					+ "SRC_DB_ID, SRC_SCHEMA, SRC_TABLE, \n" 
					+ "TGT_DB_ID,TGT_SCHEMA,  TGT_TABLE, \n"
					+ "POOL_ID, INIT_DT, INIT_DURATION, \n" 
					+ "CURR_STATE, \n" 
					+ "AUX_DB_ID, AUX_PRG_TYPE, \n"
					+ "SRC_JURL_NAME, AUX_PRG_NAME, AUX_CHG_TOPIC, \n" 
					+ "TS_REGIST, TS_LAST_REF, SEQ_LAST_REF) \n" + "values \n"
					+ "(" + (tblID+1) + ", null, '"	+ srcDBid + "', '" + lName + "', '" + jName + "', \n" 
					+ "'" + auxDBid + "', '*', '*', \n"
					+ " -1, null, null, \n"
					+ " 2, \n"     //For aux entry, no state of 0. 
					+ "'', '', \n" 
					+ "'', 'Java', 'topic', \n"
					+ "CURRENT_TIMESTAMP, null, null) \n"
					+ "on conflict (src_db_id, src_schema, src_table) do nothing;";
			repoJournalRow.write(jRow);
			repoJournalRow.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	private int getNextTblID() {
		return metaData.getNextTblID();
	}

}