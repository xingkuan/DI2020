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


public class RegisterTbl400 {
   private static final Logger ovLogger = LogManager.getLogger();


   private static int tblID;
   
   static FileWriter verticaDDL ;
   static FileWriter repoInsTbl ;
   static FileWriter repoInsCols;
   
   static FileWriter hadRegistered;
   static FileWriter kafkaTopic;


   private Connection srcConn;
   private Statement srcStmt;
   private ResultSet sRset;

	private static final MetaData metaData = MetaData.getInstance();

   private int genDDL(String srcDBid, String srcSch, String srcTbl, String journal, String tgtDBid, String tgtSch, String tgtTbl, int poolID, int refType) throws IOException {
      JSONObject srcDBDetail=metaData.readDBDetails(srcDBid);
      
      try {
         //Class.forName("com.ibm.as400.access.AS400JDBCDriver"); 
         Class.forName(srcDBDetail.get("db_driver").toString()); 
      } catch(ClassNotFoundException e){
         ovLogger.error("Driver error has occured");
         e.printStackTrace();
         return 2;
      }
       
      try {
          //ovLogger.info("conn attempt ");
          srcConn = DriverManager.getConnection(srcDBDetail.get("db_url").toString(), 
        		  srcDBDetail.get("db_user").toString(), 
        		  srcDBDetail.get("passwd").toString());
          srcConn.setAutoCommit(false);
          srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
          ////
          String sqlStmt = "select " + 
          		"       c.ordinal_position," + 
          		"       c.column_name," + 
          		"       k.ordinal_position as key_column," + 
          		"       k.asc_or_desc      as key_order," + 
          		"       c.data_type, c.length, c.numeric_scale, c.is_nullable, c.column_text" + 
          		"  from qsys2.syscolumns   c" + 
          		"  join qsys2.systables    t" + 
          		"    on c.table_schema = t.table_schema" + 
          		"   and c.table_name   = t.table_name" + 
          		"  left outer join sysibm.sqlstatistics k" + 
          		"    on c.table_schema = k.table_schem" + 
          		"   and c.table_name   = k.table_name" + 
          		"   and c.table_name   = k.index_name " + 
          		"   and c.column_name  = k.column_name" + 
          		" where c.table_schema = '" + srcSch + "' "  +
          		"   and c.table_name   = '" + srcTbl + "' " +
          		" order by ordinal_position asc"
          ;		
          sRset=srcStmt.executeQuery(sqlStmt );

          
          //For sync_table
          String sqlRepoDML1 = "insert into VERTSNAP.SYNC_TABLE \n" + 
            		"  (SOURCE_SCHEMA, TARGET_SCHEMA, SOURCE_TABLE, TARGET_TABLE, CURR_STATE, TABLE_WEIGHT, TABLE_ID, \n" + 
            		"   SOURCE_TRIGGER_NAME, T_ORDER, ORA_DELIMITER, EXP_TYPE, EXP_TIMEOUT, VERT_DELIMITER, PARTITIONED, \n" + 
            		"   SOURCE_LOG_TABLE, TARGET_PK, ERR_CNT, ERR_LIM, source_db_id, target_db_id, pool_id, refresh_type) \n" + 
            		"values \n" +
            		"  ('" + srcSch +"', '" + tgtSch + "', '" + srcTbl + "', '" + tgtTbl + "', 0, 1, " + tblID + ", \n" +
            		"  '',  '', '|', 1, 500, '|', 'N', " +    // no SOURCE_TRIGGER_NAME
            		"    '" + journal + "', 'DB2RRN', 0, 5, " + srcDBid + ", " + tgtDBid + ", " + poolID + ", " + refType + ") \n;";           //SOURCE_LOG_TABLE contains JOURNAL info	
          //System.out.println(sqlRepoDML1);
          repoInsTbl.write(sqlRepoDML1);
          
          // for sync_table_field:
          String sqlRepoDMLTemp = "insert into SYNC_TABLE_FIELD "
            		+ "(FIELD_ID, TABLE_ID, SOURCE_FIELD, TARGET_FIELD, XFORM_FCTN, XFORM_TYPE ) "
            		+ "values ("
            		+ "";
          String sqlRepoDMLfield = "";
          // prepare target DDL(Vertica):
          String sqlDDLv = "create table " + tgtSch + "." + tgtTbl + "\n ( ";
          String strDataSpec;
          int scal;
          String sDataType, tDataType;
          String xForm=""; int xType;
          int fieldCnt = 0;
          while (sRset.next() ) {
        	  sqlRepoDMLfield = "";
        	  fieldCnt++;
        	  
        	  sDataType = sRset.getString("data_type");
        	  
        	  if (sDataType.equals("VARCHAR")) {
        		  strDataSpec = "VARCHAR2(" + 2 * sRset.getInt("length") + ")";   //simple double it to handle UTF string

        		  xType = 1;
        		  xForm ="nvl(" + sRset.getString("column_name") + ", NULL)" ;
         	 } else if (sDataType.equals("DATE")) {
	       		  strDataSpec = "DATE";
	       		  xType = 7;
	       		  xForm ="nvl(to_char(" + sRset.getString("column_name") + ",''dd-mon-yyyy''), NULL)" ;
        	 } else if (sDataType.equals("TIMESTMP")) {
        		  strDataSpec = "TIMESTAMP";
        		  xType = 6;
        		  xForm ="nvl(to_char(" + sRset.getString("column_name") + ",''dd-mon-yyyy hh24:mi:ss''), NULL)" ;
        	  } else if (sDataType.equals("NUMERIC")) {
	        	  scal = sRset.getInt("numeric_scale");
	        	  if (scal > 0) {
	        		  strDataSpec = "NUMBER(" +  sRset.getInt("length") + ", " + sRset.getInt("numeric_scale") + ")"; 

	        		  xType = 4;   //was 5; but let's make them all DOUBLE
	        		  xForm ="nvl(to_char(" + sRset.getString("column_name") + "), NULL)" ;
	        	  } else {
	        		  strDataSpec = "NUMBER(" +  sRset.getInt("length") + ")";

	        		  xType = 1;  //or 2
		        	  //xForm = ;
	        	  }
        	  } else if (sDataType.equals("CHAR")) {
        		  strDataSpec = "CHAR(" +  2 * sRset.getInt("length") + ")";   //simple double it to handle UTF string
        		  
        		  xType = 1;
        		  xForm ="nvl(" + sRset.getString("column_name") + "), NULL)" ;
        	  }  else {
        		  strDataSpec = sDataType; 

        		  xType = 1;
        		  xForm ="nvl(to_char(" + sRset.getString("column_name") + "), NULL)" ;
        	  }
        	  sqlDDLv = sqlDDLv + "\"" + sRset.getString("column_name") + "\" " + strDataSpec + ",\n";
        	  
        	  sqlRepoDMLfield = sqlRepoDMLTemp  
        			 +  sRset.getInt("ordinal_position") + ", " + tblID + ", '" + sRset.getString("column_name")  + "', '" 
        			 + sRset.getString("column_name")  + "', '" + xForm + "', " + xType + ") ;\n";
        			 
   	    	  //System.out.println(sqlRepoDMLfield);
   	    	  repoInsCols.write(sqlRepoDMLfield);
          }
          sqlDDLv = sqlDDLv + 
        		  " DB2RRN int ) \n;";
    	  //System.out.println(sqlDDLv);
    	  verticaDDL.write(sqlDDLv);

    	  fieldCnt++;
    	  sqlRepoDMLfield = sqlRepoDMLTemp + fieldCnt + ", "+ tblID + ", 'RRN(a) as DB2RRN', 'DB2RRN', " 
    			  + " 'nvl(rrn(a), NULL)', 1) \n;"; 
    	  //System.out.println(sqlRepoDMLfield);
    	  repoInsCols.write(sqlRepoDMLfield);
    	  
          sRset.close();
          srcStmt.close();
          srcConn.close();
       } catch(SQLException e) {
          ovLogger.error(e.getMessage());

          
          return 1;
       }
       
      return 0;
   }   
   
	   
   private int getNextTblID() {
	   return metaData.getNextTblID();
   }
	
	public static void main(String[] args) throws IOException {

      if (args.length == 10) {
    	  tblID = 0;  //have to be set later.
      } else if (args.length == 11) {
    	  tblID = Integer.parseInt(args[10]);
      } else {
          System.out.println("Usage:   RegisterTbl400 <sdbID> <ssch> <stbl> <jrnl> <tdbID> <tsch> <ttbl> <pID> <rtype> <opath>");
          System.out.println("   or:   RegisterTbl400 <sdbID> <ssch> <stbl> <jrnl> <tdbID> <tsch> <ttbl> <pID> <rtype> <opath> <tblID>");
          System.out.println("E. g.:   RegisterTbl400 7 JOHNLEE2 TESTTBL1 JOHNLEE2.QSQJRN 4 DB2T TESTTBL1 11 1 c:\\Users\\johnlee\\");
          System.out.println("   or:   RegisterTbl400 7 JOHNLEE2 TESTTBL1 JOHNLEE2.QSQJRN 4 DB2T TESTTBL1 11 1 c:\\Users\\johnlee\\  285");
          // test parms: 7 JOHNLEE2 TESTTBL2 JOHNLEE2.QSQJRN 4 test TESTTBL2 11 1 c:\Users\johnlee\ 501
          // test parms: 7 JDAADM INVORD JDAADM.INVORD 4 test INVORD 11 1 c:\Users\johnlee\ 
          // 8 MM510CMN INVMST ITHAB1JRN.JDAJRN 4 test INVMST 11 1 c:/Users/johnlee/
          return ;
      } 

      String srcDBid = args[0];
      String srcSch = args[1];
      String srcTbl = args[2];
      String jrnlName = args[3];
      String tgtDBid = args[4];
      String tgtSch = args[5];
      String tgtTbl = args[6];
      int poolID = Integer.parseInt(args[7]);
      int refType = Integer.parseInt(args[8]);
      String outPath = args[9];

System.out.println(Arrays.toString(args));      
System.out.println(outPath);      
      RegisterTbl400 regTbl = new RegisterTbl400();
      
      //check if the Journal can be accessed 
      boolean isJournalOK = regTbl.verifyJournal (srcDBid, srcSch, srcTbl, jrnlName);
      if(isJournalOK) {
	      if (tblID == 0) {
	    	  tblID = regTbl.getNextTblID();
	      }
	      
	      // make sure tableID is not used
	      if (metaData.isNewTblID(tblID)) {
/*		      File dir = new File (outPath);
		      verticaDDL = new FileWriter(new File(dir, "verticaDDL.sql"));
		      repoInsTbl = new FileWriter(new File(dir, "repoTblDML.sql"));
		      repoInsCols = new FileWriter(new File(dir, "repoColsDML.sql"));
*/		      
		      verticaDDL = new FileWriter(new File(outPath + "verticaDDL.sql"));
		      repoInsTbl = new FileWriter(new File(outPath + "repoTblDML.sql"));
		      repoInsCols = new FileWriter(new File(outPath + "repoColsDML.sql"));

		      regTbl.genDDL(srcDBid, srcSch, srcTbl, jrnlName, tgtDBid, tgtSch, tgtTbl, poolID, refType);
		
		      verticaDDL.close();
		      repoInsTbl.close();
		      repoInsCols.close();
		      
		      // generate comands for checking existence
		      String strText;
		      hadRegistered = new FileWriter(new File(outPath + "hadRegistered.sql"));
		      strText="select 'exit already!!!' from sync_table where source_db_id="
		    		  +srcDBid+" and source_schema='"+srcSch+"' and source_table='"+srcTbl+"';";
		      hadRegistered.write(strText);
		      hadRegistered.close();
		      
		      //generate command for create kafka topic
		      kafkaTopic = new FileWriter(new File(outPath + "kafkaTopic.sh"));
		      strText="/opt/kafka/bin/kafka-topics.sh --zookeeper usir1xrvkfk02:2181 --delete --topic " + srcSch+"."+srcTbl+"\n\n" +
		              "/opt/kafka/bin/kafka-topics.sh --create " + 
		              "--zookeeper usir1xrvkfk02:2181 " + 
		              "--replication-factor 2 " + 
		              "--partitions 2 " + 
		              "--config retention.ms=86400000 " + 
		              "--topic " + srcSch+"."+srcTbl+" \n"  
		              ;
		      kafkaTopic.write(strText);
		      kafkaTopic.close();
		      
		      FileWriter repoJournalRow = new FileWriter(new File(outPath + "repoJ400row.sql"));
		      String jRow = "merge into VERTSNAP.sync_journal400 a \n" + 
		      		"using (select distinct source_db_id, source_log_table from VERTSNAP.sync_table where pool_id = " + poolID + " ) b \n" + 
		      		"on (a.source_db_id = b.source_db_id and a.source_log_table=b.source_log_table) \n" + 
		      		"when not matched then \n" + 
		      		"  insert (a.source_db_id, a.source_log_table) \n" + 
		      		"  values (b.source_db_id, b.source_log_table) \n" 
		      		;
		      repoJournalRow.write(jRow);
		      repoJournalRow.close();
		      
	      }else {
	    	  System.out.println("TableID " + tblID + " has been used already!");
	      }
      }else {
    	  System.out.println("Is the journal for " + srcSch + "." + srcTbl + ": " + jrnlName + " right?" );
      }
	}

	private boolean verifyJournal (String srcDBid, String srcSch, String srcTbl, String srcLog) {
		boolean rslt = false;
		String[] res = srcLog.split("[.]", 0);
  	    //String jLibName = "JOHNLEE2";
  	    //String jName = "QSQJRN";
  	    String jLibName = res[0];
  	    String jName = res[1];
  	  
        JSONObject srcDBDtl=metaData.readDBDetails(srcDBid);
		try {
			srcConn = DriverManager.getConnection(srcDBDtl.get("db_url").toString(), 
					srcDBDtl.get("db_user").toString(), 
					srcDBDtl.get("passwd").toString());
			srcConn.setAutoCommit(false);
			srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			////
	    	String rLib="", rName="";  //all receiver?
	    	// try to read journal of the last 4 hours(I know I'm using the client time; that does not matter)
	    	Calendar cal = Calendar.getInstance();
	    	cal.add(Calendar.HOUR_OF_DAY, -4);

	    	String strTS = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS").format(cal.getTime());
	    	String sqlStmt =  " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR"
	    	              		+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', "
	    	              		+ "   '" + rLib + "', '" + rName + "', "
	    	              		+ "   cast('" + strTS +"' as TIMESTAMP), "    //pass-in the start timestamp;
	    	              		+ "   cast(null as decimal(21,0)), "    //starting SEQ #
	    	              		+ "   'R', "   //JOURNAL CODE: 
	    	              		+ "   '',"    //JOURNAL entry:UP,DL,PT,PX
	    	              		+ "   '" + srcSch + "', '" + srcTbl + "', '*QDDS', '',"  //Object library, Object name, Object type, Object member
	    	              		+ "   '', '', ''"   //User, Job, Program
	    	              		+ ") ) as x order by 2 asc"
	    	              		;
	
			sRset=srcStmt.executeQuery(sqlStmt );		
        
			if (sRset.next() ) {
			//	rslt = true;
			}
			rslt = true;
			
			sRset.close();
			srcStmt.close();
			srcConn.close();
		} catch(SQLException e) {
			ovLogger.error(e.getMessage());
		}
		
		return rslt;
	}

}