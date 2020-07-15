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

public class unRegistTask {
	private static final Logger logger = LogManager.getLogger();
	private static final MetaData metaData = MetaData.getInstance();

	private static int taskID;

	static DataPoint srcDB, tgtDB, dccDB;

	public static void main(String[] args) throws IOException {
		System.out.println(Arrays.toString(args));
		//System.out.println(args.length);

		boolean unregister = false;
		if (args.length == 1) {
			unregister = false;
		}else if (args.length == 2) {
			String opt = args[1];
			if(opt.equals("-c"))
				unregister = false;
			else if (opt.equals("-x"))
				unregister = true;
			else {
				System.out.println("Usage: unregistTbl tblId [-c|-x]");
				return;
			}
		}else{
			System.out.println("Usage: unregistTbl tblId [-c|-x]");
			return;
		}

		taskID = Integer.parseInt(args[0]);
		metaData.setupTaskForAction("Unregist task", taskID, -1);

		if(!unregister) {  //Just checking ...
			System.out.println("To really do it:   unregistTask " + taskID + " -x");
			return;
		}else {   //really unregister it
			JSONObject tblDetail = metaData.getTaskDetails();
			//Check if this task has any dependency. If yes,(like DB2 journal DCC), return.
			if(metaData.taskHasDependency(tblDetail.get("src_db_id").toString(),
					tblDetail.get("src_schema").toString(),
					tblDetail.get("src_table").toString())){
				logger.error("I don't think you want to remove " + tblDetail.get("src_schema")
				+ "." + tblDetail.get("src_table"));
				return;
			}
			DataPoint srcData = DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
			srcData.unregisterSrc(taskID);
			srcData.close();
			DataPoint tgtData = DataPoint.dataPtrCreater(tblDetail.get("tgt_db_id").toString(), "TGT");
			tgtData.unregisterTgt(taskID);
			tgtData.close();
			String dccDBid = tblDetail.get("dcc_db_id").toString();
			/*TODO: should not do this
			if(!( (dccDBid !=null) || (dccDBid.equals("na"))) ) {
				DataPoint auxData = DataPoint.dataPtrCreater(dccDBid, "DCC");
				auxData.unregisterDcc(taskID);
				auxData.close();
			}
			*/
			String sqlStr;
			sqlStr = "delete from data_field where task_id = " + taskID;
			metaData.runRegSQL(sqlStr);
			sqlStr = "delete from task where task_id = " + taskID; 
			metaData.runRegSQL(sqlStr);
			metaData.close();
		}

		return;
	}
}
