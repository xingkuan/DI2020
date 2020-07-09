package com.future.DI2020;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class xformEngine {
	public static final Logger logger = LogManager.getLogger();
	protected static final MetaData metaData = MetaData.getInstance();
	protected static final Conf conf = Conf.getInstance();
	protected static final Metrix metrix = Metrix.getInstance();
	protected static final String logDir = conf.getConf("logDir");

	ScriptEngine graalEngine;
	
	public void xformEngine() {
		graalEngine = new ScriptEngineManager().getEngineByName("graal.js");
		
		try {
			graalEngine.eval("print('Hello Graal World!');");
			graalEngine.eval("function sum(a,b){return a.concat(b);}");
		    String v = (String)graalEngine.eval("sum(\"Hello, \", \"the other world!\")");
		    System.out.println(v);
	    } catch (ScriptException e) {
			e.printStackTrace();
		}   
	}
	public GenericRecord transform(GenericRecord a) {
		GenericRecord b=null;
	    Invocable invocable = (Invocable) graalEngine;
		try {
			Object o = invocable.invokeFunction("sum", "this ", "that");

			//b=
		} catch (NoSuchMethodException | ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return b;
	}

}
