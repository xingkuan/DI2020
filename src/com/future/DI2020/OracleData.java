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

class OracleData extends JDBCData{
	public static final Logger logger = LogManager.getLogger();
	
	private Statement srcSQLStmt = null;
	private ResultSet srcRS = null;
	
   //public OracleData(String dbID) throws SQLException {
   public OracleData(JSONObject dbID, String role) throws SQLException {
		super(dbID, role);
   }
   protected void initializeFrom(DataPoint dt) {
		logger.info("   not needed yet");
   }
	public boolean miscPrep(String jTemp) {
		boolean rtc=true;
		super.miscPrep(jTemp);
		//if(jTemp.equals("DJ2K")) { 
		//	rtc=initThisRefreshSeq();
		//}
		return rtc;
	}

	public ResultSet getSrcResultSet() {
		return srcRS;
	}
	//should only be sued when TEMP_ID
	public int getDccCnt() {  //this one is very closely couple with crtSrcResultSet! TODO: any better way for arranging?
		if(!metaData.getTableDetails().get("temp_id").toString().contains("_")){
			logger.warn("It does not look like it should be called!");
		}
		String sql = "update " + metaData.getTableDetails().get("src_dcc_tbl") + " set dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')" ;

		int cnt = runUpdateSQL(sql);

		return cnt;
	}
	public int crtSrcResultSet(int actId, JSONArray jaSQLs) {
		String sql;
		for (int i = 0; i < jaSQLs.size()-1; i++) {
			sql = jaSQLs.get(i).toString();
			runUpdateSQL(sql);
		}
		sql=jaSQLs.get(jaSQLs.size()-1).toString();
		if( !SQLtoResultSet(sql) ) {
			return -1;
		}
		return 0;
	}

	public List<String> getDCCKeyList(){
		List<String> keyList=new ArrayList<String>();

	    Statement stmt1;
 	    ResultSet rs1;
		String sql1 = "select distinct orarid from " 
					+ metaData.getTableDetails().get("src_dcc_tbl") 
					+ " where dcc_ts = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')";
		try {
			stmt1 = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs1 = stmt1.executeQuery(sql1);
			while(rs1.next())
				keyList.add(rs1.getString(1));
			rs1.close();
			stmt1.close();
		} catch (SQLException e) {
			logger.error("   " + e);
		}

		return keyList;
	}

   
	/******** Registration APIs **********/
	@Override
	public boolean regTblCheck(String srcSch, String srcTbl, String srcLog) {
		//do nothing for Oracle trig based.
		return true;
	}
	@Override
	public JSONObject genRegSQLs(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
		Statement stmt;
		ResultSet rset = null;
		JSONObject json = new JSONObject();
		//jurl is not used.
		//String[] res = jurl.split("[.]", 0);
		//String lName = res[0];
		//String jName = res[1];

		String sqlFields = "insert into META_TABLE_FIELD \n"
				+ " (TBL_ID, FIELD_ID, SRC_FIELD, SRC_FIELD_TYPE, TGT_FIELD, TGT_FIELD_TYPE, JAVA_TYPE, AVRO_Type) \n"  
				+ " values \n";
		String sqlCrtTbl = "create table " + tgtSch + "." + tgtTbl + "\n ( ";

		try {
			stmt = dbConn.createStatement();
			String sqlStmt = "select c.column_id, c.column_name, "
					          + "c.data_type, c.data_length, c.data_scale, c.data_precision "
					+ "from dba_tab_columns c "
					+ "where c.owner = upper('" + srcSch + "') "
					+ "  and c.table_name   = upper('" + srcTbl + "') " 
					+ " order by c.column_id asc";
			
			rset = stmt.executeQuery(sqlStmt);
			
			String strDataSpec;
			int scal;
			String sDataType, tDataType, aDataType;
			int xType;
			int fieldCnt = 0;
			while (rset.next()) {
				fieldCnt++;

				sDataType = rset.getString("data_type");

				if (sDataType.equals("VARCHAR2")) {
					strDataSpec = "VARCHAR(" + 2*rset.getInt("data_length") + ")"; // simple double it to handle UTF string
					xType = 1;
					aDataType = "[\"string\", \"null\"]";
				} else if (sDataType.equals("DATE")) {
					strDataSpec = "DATE";
					xType = 7;
					aDataType = "\"int\", \"logicalType\": \"date\"";
				} else if (sDataType.contains("TIMESTAMP")) {
					strDataSpec = "TIMESTAMP";
					xType = 6;
					aDataType = "\"int\", \"logicalType\": \"timestamp-micros\"";
				} else if (sDataType.equals("NUMBER")) {
					scal = rset.getInt("data_scale");
					if (scal > 0) {
						strDataSpec = "NUMBER(" + rset.getInt("data_length") + ", " + rset.getInt("data_scale") + ")";
						xType = 4; // was 5; but let's make them all DOUBLE
						//aDataType = "\"bytes\", \"logicalType\": \"decimal\",\"precision\":" + rset.getInt("data_length")
						//+ "\"scale\": " + scal;
					} else {
						strDataSpec = "NUMBER(" + rset.getInt("data_length") + ")";
						xType = 4; // or 2
						//aDataType = "\"bytes\", \"logicalType\": \"decimal\",\"precision\":" + rset.getInt("data_length")
						//+ "\"scale\": " + 0;
					}
					aDataType = "\"long\"";   //BigDecimal is what getObject returns; but it can't work with AVRO.
				} else if (sDataType.equals("CHAR")) {
					strDataSpec = "CHAR(" + 2 * rset.getInt("data_length") + ")"; // simple double it to handle UTF string
					xType = 1;
					aDataType = "[\"string\", \"null\"]";
				} else {
					strDataSpec = sDataType;
					xType = 1;
					aDataType = "\"string\"";
				}
				sqlCrtTbl = sqlCrtTbl + "\"" + rset.getString("column_name") + "\" " + strDataSpec + ",\n";  //""" is needed because column name can contain space!

				sqlFields = sqlFields 
						+ "(" + tblID + ", " + rset.getInt("column_id") + ", '"  
						+ rset.getString("column_name") + "', '" + sDataType + "', '"
						+ rset.getString("column_name") + "', '" + strDataSpec + "', "
						+ xType + ", '\"type\": " + aDataType + "'),\n";
			}
			sqlCrtTbl = sqlCrtTbl + " " + PK + " varchar(20) ) \n;";

			fieldCnt++;
			sqlFields = sqlFields
					+ "("+ tblID +", " + fieldCnt + ", " 
					+ "'rowid as " + PK + "', 'varchar(20)', "
					+ "'" + PK + "', 'varchar(20)', "
					+ "1, 'string') \n;";
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		json.put("tgtTblDDL", sqlCrtTbl);
		json.put("repTblFldDML", sqlFields);
		//json.put("repDCCTbl", repDCCTbl);
		//json.put("repDCCTblFld", sqlFieldsDCC);
		String srcSQLs="CREATE TABLE " + jurl
				+ " (" + PK + " VARCHAR2(20),  DCC_TS DATE) TABLESPACE DCC_TABLESPACE\n\n;"
				+ "CREATE OR REPLACE TRIGGER " + dccPgm + " \n"  
				+ " AFTER  INSERT OR UPDATE OR DELETE ON " + srcSch+"."+srcTbl + "\n" 
				+ "  FOR EACH ROW\n" 
				+ "    BEGIN  INSERT INTO " + jurl + "(" + PK + ", DCC_TS )\n"  
				+ "     VALUES ( :new.rowid, sysdate   );  END;\n\n"  
				+ "alter trigger " + dccPgm + " disable;\n\n";
		json.put("srcSQLs", srcSQLs);

		return json;
	}
	/***************************************************/
	
	public boolean beginDCC(){
	   Statement sqlStmt;
	   try {
		   sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		   String sql="alter trigger "  + metaData.getTableDetails().get("src_dcc_pgm").toString() + " enable";
		   sqlStmt.executeUpdate(sql);
		   sqlStmt.close();
	   } catch (SQLException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   }
		
	   logger.info("   trigger is enabled..");
		
	   return true;
	}
	protected void afterSync(int actId, JSONArray jaSQLs){
		String sql;
		for (int i = 0; i < jaSQLs.size(); i++) {
			sql = jaSQLs.get(i).toString();
			runUpdateSQL(sql);
		}
	}

	private int runUpdateSQL(String sql) {
		int rslt=0;
		// Save to MetaRep:
		//java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
		Statement stmt=null; 
		try {
			stmt = dbConn.createStatement();
			rslt = stmt.executeUpdate(sql);
			stmt.close();
			dbConn.commit();
		} catch (SQLException e) {
			logger.error(e);
		} 
		return rslt;
	}

   public void commit() {
      try {
		dbConn.commit();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   public void rollback()  {
      try {
		dbConn.rollback();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }

}