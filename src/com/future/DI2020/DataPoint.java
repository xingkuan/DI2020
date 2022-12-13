package com.future.DI2020;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

public class DataPoint {
	protected String dbID;
	protected String urlString;
	protected String driver;
	protected String userID;
	protected String passPWD;
	protected String dbType, dbCat, dbEngine;

	public static final Logger logger = LogManager.getLogger();
	protected static final Conf conf = Conf.getInstance();
	protected static final Matrix metrix = Matrix.getInstance();
	protected static final String logDir = conf.getConf("logDir");

	
	protected JSONObject dataDetail;
	
	public DataPoint(JSONObject jo) {
		//JSONObject jo = metaData.readDBDetails(dbid);
//		dbRole = role;
		
		dbID=jo.get("db_id").toString();
		//this.dbID=dbid;
		urlString=jo.get("db_conn").toString();
		driver=jo.get("db_driver").toString();
		userID=jo.get("db_usr").toString();
		passPWD=jo.get("db_pwd").toString();
		dbType=jo.get("db_role").toString();
		dbCat=jo.get("db_cat").toString();
		dbEngine=jo.get("db_engine").toString();
	}
	
	public void setDetail(JSONObject dtl) {
		dataDetail=dtl;
	}

	public void closeDB() {
	}
	
	public void commit() {
	}

	public JSONObject syncTo(DataPoint srcData) {
		// TODO Auto-generated method stub
		return null;
	}

	public void clearState() {
		//tblName="";
	}

	public int runDBcmd(String sqlStr, String type) {
		// TODO Auto-generated method stub
		return -1;
	}

	public Map<String, String> getRegSTMTs(String bareSQL) {
		// TODO Auto-generated method stub
		return null;
	}

	public int getRecordCount() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	//future staff
	private xformEngine xEngine;
	public void setupXformEngine(xformEngine xformEng) {
		xEngine=xformEng;
	}
	protected int xformInto(DataPoint tgtData) {
		return 0;
	}

	public int upwrite(GenericRecord rs, int colCnt) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int upwrite(ResultSet rs, int colCnt) {
		// TODO Auto-generated method stub
		return 0;
	}

	public List<String> getDCCkeys() { 
		// TODO Auto-generated method stub
		return null;
	}

	public void prepareBatchStmt(String delStmt) {
		// TODO Auto-generated method stub
		
	}

}
