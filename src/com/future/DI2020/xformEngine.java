package com.future.DI2020;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

public class xformEngine {
	public static final Logger logger = LogManager.getLogger();
	protected static final TaskMeta metaData = TaskMeta.getInstance();
	protected static final Conf conf = Conf.getInstance();
	protected static final Matrix metrix = Matrix.getInstance();
	protected static final String logDir = conf.getConf("logDir");

	ScriptEngine graalEngine;
    //Invocable invocable = (Invocable) graalEngine;
	
	public xformEngine() {
	  //  graalEngine = new ScriptEngineManager().getEngineByName("graal.js");
	}
	String scripts;
	List<String> fnList;
	public void setupScripts() {
		 graalEngine = new ScriptEngineManager().getEngineByName("graal.js");

		scripts = metaData.getScripts();
		fnList = metaData.getxFnctList(); 
		
		//String scripts="function sum(a,b){return a.concat(b);}\n"
		//		+ "function fi(a){return 2*a;}\n"
		//		+ "function testGlobalVar(){print(rec0);}\n"
		//		+ "function testVar1(){print(rec.COL2);\n"    //field name is case-sensitive!
		//		+ "return rec.COL;}";
		
		try {
			graalEngine.eval(scripts);

			//Invocable invocable = (Invocable) graalEngine;
			//invocable.invokeFunction("testGlobalVar");
			//invocable.invokeFunction("testVar1");
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public JSONObject transform(GenericRecord a) {
		JSONObject b=new JSONObject();
		//iterate through the scripts and run all of them to create the json result
		try {
		    //graalEngine.put("rec0", "{\"COL\": 2, \"COL2\": \"val 1\"}");
			///graalEngine.put("rec", a);    //"a" is JSONObject not!
			graalEngine.put("rec0", a.toString());
			graalEngine.eval("print(rec0);");
			graalEngine.eval("var rec=JSON.parse(rec0);\n");
			Invocable invocable = (Invocable) graalEngine;
			//invocable.invokeFunction("testGlobalVar");
			//int n = (int) invocable.invokeFunction("testVar1");
			//System.out.println(n);
			Object o;
			for(String fn : fnList) {
				o = invocable.invokeFunction(fn);
				if(o!=null) {
				System.out.println(o.toString());
				System.out.println(o.getClass());
				b.put(fn, o);
				}
			}
			} catch (NoSuchMethodException | ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return b;
	}

	public void test() {
		try {
			graalEngine.eval("print('Hello Graal World!');");
			graalEngine.eval("function sum(a,b){return a.concat(b);}");
		    String v = (String)graalEngine.eval("sum(\"Hello, \", \"the other world!\")");
		    System.out.println(v);
		    
		    graalEngine.put("rec0", "{\"COL\": 2, \"col2\": \"val 1\"}");
			graalEngine.eval("var rec=JSON.parse(rec0);\n");
			
		    Invocable invocable = (Invocable) graalEngine;
			invocable.invokeFunction("testGlobalVar");
			int n = (int) invocable.invokeFunction("testVar1");
			System.out.println(n);
			
	    } catch (ScriptException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
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
}
