package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;

class JDBCData extends DataPoint{
	protected Connection dbConn;

	public JDBCData(JSONObject jo, String role) throws SQLException {
		super(jo, role);
		connectDB();  
	}

	private void connectDB() {
		if(dbCat.equals("RDBMS")){
			try {
				Class.forName(driver); 
			} catch(ClassNotFoundException e){
				ovLogger.error("   Driver error has occured");
				ovLogger.error( e);
			}
      
			try {
				dbConn = DriverManager.getConnection(urlString, userID, passPWD);
				dbConn.setAutoCommit(false);
			} catch(SQLException e) {
				ovLogger.error("   cannot connect to db");
				ovLogger.error(e);
			}
		}else {
				ovLogger.info("   If you never see this!");
		}
	}
	
}