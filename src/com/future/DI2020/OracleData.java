package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;

class OracleData extends DataPointer{
	private Statement srcSQLStmt = null;
	private ResultSet srcRS = null;
	
   public OracleData(String dbID) throws SQLException {
		super(dbID);
   }
   protected void initializeFrom(DataPointer dt) {
		ovLogger.info("   not needed yet");
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
	public int crtSrcResultSet(String str) {
		if(str.equals("")) {  //DB2 has some complication.
			//But for Oracle (trig based, it should be simpler. and parm "str" should always be ""
			//TODO: move this complexity to DB. ---that is: to be Data Driving!
			String sql = metaData.getSQLSelSrc(false, false);
			if((sql==null)||(sql.equals(""))){
				return -2;
			}
			
			if( !SQLtoResultSet(sql) ) {
			}
		}else {
			//TODO
		}
		return 0;
	}
	private boolean SQLtoResultSet(String sql) {
		try {
			// String strTS = new
			// SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(tblMeta.getLastRefresh());
			srcSQLStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			srcRS = srcSQLStmt.executeQuery(sql);
			if (srcRS.isBeforeFirst()) {// this check can throw exception, and do the needed below.
				ovLogger.info("   opened src recordset: ");
			}
		} catch (SQLException e) {
			ovLogger.error("   " + e);
			return false;
		}
		return true;
	}

   public int getRecordCount(){
      int rtv;
	   Statement sqlStmt;
	      ResultSet sqlRset;
      int i;

      rtv=0;
      try {
    	  sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    	  sqlRset=sqlStmt.executeQuery("select count(*) from " + metaData.getTableDetails().get("src_sch").toString() 
    			  		+ "." + metaData.getTableDetails().get("src_tbl").toString());
    	  sqlRset.next();
            rtv = Integer.parseInt(sqlRset.getString(1));  
            sqlRset.close();
         sqlStmt.close();
      } catch(SQLException e) {
         ovLogger.error("   running sql: "+ e); 
      }
      return rtv;
   }
   
   
// methods for registration
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
					aDataType = "string";
				} else if (sDataType.equals("DATE")) {
					strDataSpec = "DATE";
					xType = 7;
					aDataType = "date";
				} else if (sDataType.equals("TIMESTMP")) {
					strDataSpec = "TIMESTAMP";
					xType = 6;
					aDataType = "timestamp_micros";
				} else if (sDataType.equals("NUMBER")) {
					scal = rset.getInt("data_scale");
					if (scal > 0) {
						strDataSpec = "NUMBER(" + rset.getInt("data_length") + ", " + rset.getInt("data_scale") + ")";
						xType = 4; // was 5; but let's make them all DOUBLE
					} else {
						strDataSpec = "NUMBER(" + rset.getInt("data_length") + ")";
						xType = 1; // or 2
					}
					aDataType = "dbl";
				} else if (sDataType.equals("CHAR")) {
					strDataSpec = "CHAR(" + 2 * rset.getInt("data_length") + ")"; // simple double it to handle UTF string
					xType = 1;
					aDataType = "string";
				} else {
					strDataSpec = sDataType;
					xType = 1;
					aDataType = "string";
				}
				sqlCrtTbl = sqlCrtTbl + "\"" + rset.getString("column_name") + "\" " + strDataSpec + ",\n";  //""" is needed because column name can contain space!

				sqlFields = sqlFields 
						+ "(" + tblID + ", " + rset.getInt("column_id") + ", '"  
						+ rset.getString("column_name") + "', '" + sDataType + "', '"
						+ rset.getString("column_name") + "', '" + strDataSpec + "', "
						+ xType + ", '" + aDataType + "'),\n";
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

	// ..............
	
	public boolean beginDCC(){
	   Statement sqlStmt;
	   try {
		   sqlStmt = dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		   sqlStmt.executeUpdate("alter trigger "  + metaData.getTableDetails().get("aux_pgm_name").toString() + " enable");
		   sqlStmt.close();
	   } catch (SQLException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   }
		
	   ovLogger.info("   trigger is enabled..");
		
	   return true;
	}

   public void commit() throws SQLException {
      dbConn.commit();
   }
   public void rollback() throws SQLException {
      dbConn.rollback();
   }

}