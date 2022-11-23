package com.future.DI2020;

import java.sql.SQLException;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
//import org.graalvm.polyglot.*;
//import org.graalvm.polyglot.proxy.*;

import org.json.simple.JSONObject;

class Test
{
   private static final Matrix metrix = Matrix.getInstance();
   
   public static void main (String args[]) {   
	   //metrix.sendMXrest("initDuration,jobId=test,tblID=0 value=6\n");
	   //metrix.sendMX("initDuration,jobId=test,tblID=0 value=6\n");
      
	   //testAVROConsumer();
	   
	   //testES();
	   
	   //testJSNashorn();
	   //testJSGraal();
	   
	   //2020.07.24
	   testHiveData();
	   
	   return ;
   }
   
   private static void testHiveData() {
	   JSONObject jo = new JSONObject();
	   //db_id, db_cat, db_type, db_conn, db_driver, db_usr, db_pwd
	   jo.put("db_id", "HIVE1");
	   jo.put("db_cat", "RDBMS");
	   jo.put("db_type", "HIVE");
	   jo.put("db_conn", "jdbc:hive2://dbatool03:10000/test");
	   jo.put("db_driver", "org.apache.hive.jdbc.HiveDriver");
	   jo.put("db_usr", "APP");
	   jo.put("db_pwd", "mine");
/*	   try {
		HiveData hd = new HiveData(jo, "TEST");
		if(hd==null)
			return;  //something is not right!
		String sql="select * from product";
		int c = hd.SQLtoResultSet(sql);
	   } catch (SQLException e) {
		   e.printStackTrace();
	   }
*/	   
   }

private static void testES() {
	   JSONObject jo = new JSONObject();
	   //db_id, db_cat, db_type, db_conn, db_driver, db_usr, db_pwd
	   jo.put("db_id", "ES1");
	   jo.put("db_cat", "ES");
	   jo.put("db_type", "ES");
	   jo.put("db_usr", "APP");
	   jo.put("db_pwd", "mine");

	   ESData es = new ESData(jo);
	   es.test();
   }
   private static void testAVROConsumer() {
		int tableID=1;
		TaskMeta metaData = TaskMeta.getInstance();

//		metaData.setupTaskForAction("testConsumeAVRO", tableID, 21);  // actId for dev activities.
		
		Map tblDetail = metaData.getTaskDetails();
		String actTemp = tblDetail.get("template_id").toString();

//		KafkaData tgtData = (KafkaData) DataPointMgr.dataPtrCreater(tblDetail.get("tgt_db_id").toString(), "TGT");
//		tgtData.test();

   }
   
   private static void testJSNashorn() {
	   //ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");  //to be deprecated!
	   ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");  //to be deprecated!
	   //engine.eval(new FileReader("script.js"));
	   try {
	    Invocable invocable = (Invocable) engine;

	    engine.eval("value = 10");
	    Boolean greaterThan5 = (Boolean) engine.eval("value > 5");
	    Boolean lessThan5 = (Boolean) engine.eval("value < 5");
	    System.out.println("10 > 5? " + greaterThan5); // true
	    System.out.println("10 < 5? " + lessThan5); // false
	    
	    engine.eval("function sum(a,b){return a+b;}");
	    int v = (Integer)engine.eval("sum(21,22)");
	    System.out.println(v);
	   } catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
   }
   /*
   https://www.graalvm.org/downloads/ don't know if that will work when you read this.
	   From the GraalVM download put the following jar files onto your classpath (in my case the download contained a "Contents/Home/jre" folder):
	   graaljs.jar (Contents/Home/jre/languages/js/graaljs.jar)
	   graaljs-scriptengine.jar (Contents/Home/jre/lib/boot/graaljs-scriptengine.jar)
	   graal-sdk.jar (Contents/Home/jre/lib/boot/graal-sdk.jar)
	   truffle-api.jar (Contents/Home/jre/lib/truffle/truffle-api.jar)
	   icu4j.jar (Contents/Home/jre/languages/js/icu4j.jar)
	   */
   static ScriptEngine graalEngine;
   private static void testJSGraal() {
	    /*ScriptEngine*/ graalEngine = new ScriptEngineManager().getEngineByName("graal.js");
	    try {
			graalEngine.eval("print('Hello Graal World!');");

			graalEngine.eval("function sum(a,b){return a.concat(b);}\n"
					+ "function fi(a){return 2*a;}\n"
					+ "function testGlobalVar(){print(parm1);}\n"
					+ "function testVar1(){print(var1);}");
		    String v = (String)graalEngine.eval("sum(\"Hello, \", \"the other world!\")");
		    System.out.println(v);
		    
		    Invocable invocable = (Invocable) graalEngine;
			Object o = invocable.invokeFunction("sum", "this ", "that");
			System.out.println(o.toString());
			o = invocable.invokeFunction("fi", 5);
			System.out.println(o.toString());
			
			graalEngine.put("parm1", "Parm1: Hi");
			graalEngine.eval("var var1='variable 1' + parm1;\n");
			v = (String)graalEngine.eval("'hope it works: ' + parm1 ");
			System.out.println(v);
			v = (String)graalEngine.eval("'hope it works, still: ' + parm1 ");
			System.out.println(v);
			invocable.invokeFunction("testGlobalVar");
			invocable.invokeFunction("testVar1");
			
	    } catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
   
//   private static void testJSGraalContext() {
//	   try (Context context = Context.create()) {
 //          context.eval("js", "print('Hello JavaScript!');");
//       }
//   }  
}