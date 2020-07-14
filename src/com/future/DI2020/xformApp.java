package com.future.DI2020;

import java.util.List;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

class xformApp
{
	private static final Logger logger = LogManager.getLogger();
	private static final Metrix metrix = Metrix.getInstance();
	private static final MetaData metaData = MetaData.getInstance();

	static DataPoint srcData;
	static DataPoint tgtData;

	static String jobID ;

   /*
    * goal: Read data from Kafka that needs only simple Transformation;
    *       and pump into ElasticSearch.
    *       The reason for ES is because of its TS nature and Kibana that 
    *       can answer a lot of business needs 
    */
   public static void main (String args[]) {   
		System.out.println(args.length);

		if (args.length != 2) {
			System.out.println("Usage:   xFormData <tbl|pool> id");
			//return -1;
		}

		String parmCat = args[0];
		int parmId = Integer.parseInt(args[1]);
		
		if(parmCat.contentEquals("pool"))
			xFormTables(parmId);
		else if(parmCat.contentEquals("tbl"))
			xFormTable(parmId);
		else 
			System.out.println("Usage:   syncTable <tbl|pool> oId aId");
			
	}
	static void xFormTables(int poolID) {
		List<Integer> tblList = metaData.getTblsByPoolID(poolID);
		for (int i : tblList) {
           xFormTable(i);
       }
	   return ;
   }
	static void xFormTable(int tblId) {
		jobID = "xForm";
		MetaData metaData = MetaData.getInstance();

		metaData.setupTaskForAction(jobID, tblId, 21);  // actId for dev activities.
		
		JSONObject tblDetail = metaData.getTaskDetails();

		KafkaData srcData = (KafkaData) DataPoint.dataPtrCreater(tblDetail.get("src_db_id").toString(), "SRC");
		//srcData.testConsumer();
		ESData tgtData = (ESData) DataPoint.dataPtrCreater(tblDetail.get("tgt_db_id").toString(), "TGT");
		//tgtData.test();
		srcData.xformInto(tgtData);
	}
   
   
   private static void testJSNashorn() {
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
   private static void testJSGraal() {
	    ScriptEngine graalEngine = new ScriptEngineManager().getEngineByName("graal.js");
	    try {
			graalEngine.eval("print('Hello Graal World!');");

			graalEngine.eval("function sum(a,b){return a.concat(b);}");
		    String v = (String)graalEngine.eval("sum(\"Hello, \", \"the other world!\")");
		    System.out.println(v);
	    } catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}   
	}
}