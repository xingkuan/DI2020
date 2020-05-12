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

	static FileWriter verticaDDL;
	static FileWriter repoInsTbl;
	static FileWriter repoInsCols;

	static FileWriter hadRegistered;
	static FileWriter kafkaTopic;

	static DataPointer srcDB;

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
		String auxDB = args[7];
		String auxTopic = args[8];
		String outPath = args[9];
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
				/*
				 * File dir = new File (outPath); verticaDDL = new FileWriter(new File(dir,
				 * "verticaDDL.sql")); repoInsTbl = new FileWriter(new File(dir,
				 * "repoTblDML.sql")); repoInsCols = new FileWriter(new File(dir,
				 * "repoColsDML.sql"));
				 */
				verticaDDL = new FileWriter(new File(outPath + "verticaDDL.sql"));
				repoInsTbl = new FileWriter(new File(outPath + "repoTblDML.sql"));
				repoInsCols = new FileWriter(new File(outPath + "repoColsDML.sql"));

				try {
					regTbl.genDDL(srcDBid, srcSch, srcTbl, jrnlName, tgtDBid, tgtSch, tgtTbl, poolID);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				verticaDDL.close();
				repoInsTbl.close();
				repoInsCols.close();

				// generate comands for checking existence
				String strText;
				hadRegistered = new FileWriter(new File(outPath + "hadRegistered.sql"));
				strText = "select 'exit already!!!' from sync_table where source_db_id=" + srcDBid
						+ " and source_schema='" + srcSch + "' and source_table='" + srcTbl + "';";
				hadRegistered.write(strText);
				hadRegistered.close();

				// generate command for create kafka topic
				kafkaTopic = new FileWriter(new File(outPath + "kafkaTopic.sh"));
				strText = "/opt/kafka/bin/kafka-topics.sh --zookeeper usir1xrvkfk02:2181 --delete --topic " + srcSch
						+ "." + srcTbl + "\n\n" + "/opt/kafka/bin/kafka-topics.sh --create "
						+ "--zookeeper usir1xrvkfk02:2181 " + "--replication-factor 2 " + "--partitions 2 "
						+ "--config retention.ms=86400000 " + "--topic " + srcSch + "." + srcTbl + " \n";
				kafkaTopic.write(strText);
				kafkaTopic.close();

				FileWriter repoJournalRow = new FileWriter(new File(outPath + "repoJ400row.sql"));
				String jRow = "merge into VERTSNAP.sync_journal400 a \n"
						+ "using (select distinct source_db_id, source_log_table from VERTSNAP.sync_table where pool_id = "
						+ poolID + " ) b \n"
						+ "on (a.source_db_id = b.source_db_id and a.source_log_table=b.source_log_table) \n"
						+ "when not matched then \n" + "  insert (a.source_db_id, a.source_log_table) \n"
						+ "  values (b.source_db_id, b.source_log_table) \n";
				repoJournalRow.write(jRow);
				repoJournalRow.close();

			} else {
				System.out.println("TableID " + tblID + " has been used already!");
			}
		} else {
			System.out.println("Is the journal for " + srcSch + "." + srcTbl + ": " + jrnlName + " right?");
		}
	}

	private int genDDL(String srcDBid, String srcSch, String srcTbl, String journal, String tgtDBid, String tgtSch,
			String tgtTbl, int poolID) throws SQLException {
		srcDB = DataPointer.dataPtrCreater(srcDBid);

		ResultSet rset = srcDB.getFieldMeta(srcSch, srcTbl, journal);

		// For sync_table
		String sqlRepoDML1 = "insert into didb.META_TABLE \n" + " (TBL_ID, TBL_PK, \n"
				+ "  SRC_DB_ID, SRC_SCHEMA, SRC_TABLE, \n" + "  TGT_DB_ID,TGT_SCHEMA,  TGT_TABLE, \n"
				+ "  POOL_ID, INIT_DT, INIT_DURATION, \n" + "  CURR_STATE, \n" + "  AUX_DB_ID, AUX_PRG_TYPE, \n"
				+ "  SRC_JURL_NAME, AUX_PRG_NAME, AUX_CHG_TOPIC, \n" + "  TS_LAST_REF, SEQ_LAST_REF \n" + "values \n"
				+ "  (" + tblID + ", 'DB2RRN', " + "  '" + srcDBid + "', '" + srcSch + "', '" + "', '" + srcTbl
				+ "', \n" + "  '" + tgtDBid + "', '" + tgtSch + "', '" + "', '" + tgtTbl + "', \n" + "  0, , , \n"
				+ "  0, \n" + " 'xx', 'xx', \n" + " '" + journal + "', 'prg', 'topic', \n" + " , ,) \n;";
		// System.out.println(sqlRepoDML1);
		try {
			repoInsTbl.write(sqlRepoDML1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// for sync_table_field:
		String sqlRepoDMLTemp = "insert into SYNC_TABLE_FIELD "
				+ "(FIELD_ID, TABLE_ID, SOURCE_FIELD, TARGET_FIELD, XFORM_FCTN, XFORM_TYPE ) " + "values (" + "";
		String sqlRepoDMLfield = "";
		// prepare target DDL(Vertica):
		String sqlDDLv = "create table " + tgtSch + "." + tgtTbl + "\n ( ";
		String strDataSpec;
		int scal;
		String sDataType, tDataType;
		String xForm = "";
		int xType;
		int fieldCnt = 0;
		while (rset.next()) {
			sqlRepoDMLfield = "";
			fieldCnt++;

			sDataType = rset.getString("data_type");

			if (sDataType.equals("VARCHAR")) {
				strDataSpec = "VARCHAR2(" + 2 * rset.getInt("length") + ")"; // simple double it to handle UTF string

				xType = 1;
				xForm = "nvl(" + rset.getString("column_name") + ", NULL)";
			} else if (sDataType.equals("DATE")) {
				strDataSpec = "DATE";
				xType = 7;
				xForm = "nvl(to_char(" + rset.getString("column_name") + ",''dd-mon-yyyy''), NULL)";
			} else if (sDataType.equals("TIMESTMP")) {
				strDataSpec = "TIMESTAMP";
				xType = 6;
				xForm = "nvl(to_char(" + rset.getString("column_name") + ",''dd-mon-yyyy hh24:mi:ss''), NULL)";
			} else if (sDataType.equals("NUMERIC")) {
				scal = rset.getInt("numeric_scale");
				if (scal > 0) {
					strDataSpec = "NUMBER(" + rset.getInt("length") + ", " + rset.getInt("numeric_scale") + ")";

					xType = 4; // was 5; but let's make them all DOUBLE
					xForm = "nvl(to_char(" + rset.getString("column_name") + "), NULL)";
				} else {
					strDataSpec = "NUMBER(" + rset.getInt("length") + ")";

					xType = 1; // or 2
					// xForm = ;
				}
			} else if (sDataType.equals("CHAR")) {
				strDataSpec = "CHAR(" + 2 * rset.getInt("length") + ")"; // simple double it to handle UTF string

				xType = 1;
				xForm = "nvl(" + rset.getString("column_name") + "), NULL)";
			} else {
				strDataSpec = sDataType;

				xType = 1;
				xForm = "nvl(to_char(" + rset.getString("column_name") + "), NULL)";
			}
			sqlDDLv = sqlDDLv + "\"" + rset.getString("column_name") + "\" " + strDataSpec + ",\n";

			sqlRepoDMLfield = sqlRepoDMLTemp + rset.getInt("ordinal_position") + ", " + tblID + ", '"
					+ rset.getString("column_name") + "', '" + rset.getString("column_name") + "', '" + xForm + "', "
					+ xType + ") ;\n";

			// System.out.println(sqlRepoDMLfield);
			try {
				repoInsCols.write(sqlRepoDMLfield);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		sqlDDLv = sqlDDLv + " DB2RRN int ) \n;";
		// System.out.println(sqlDDLv);
		try {
			verticaDDL.write(sqlDDLv);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		fieldCnt++;
		sqlRepoDMLfield = sqlRepoDMLTemp + fieldCnt + ", " + tblID + ", 'RRN(a) as DB2RRN', 'DB2RRN', "
				+ " 'nvl(rrn(a), NULL)', 1) \n;";
		// System.out.println(sqlRepoDMLfield);
		try {
			repoInsCols.write(sqlRepoDMLfield);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rset.close();

		return 0;
	}

	private int getNextTblID() {
		return metaData.getNextTblID();
	}


}