package com.future.DI2020;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

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

	
	public void test() {
		xformInto(null);
	}
	
	public void xformInto(DataPoint tgtData) {
		String topic=metaData.getTaskDetails().get("tgt_schema")+"."+metaData.getTaskDetails().get("tgt_table");
		ConsumerRecords<Long, byte[]> records;
		GenericRecord a=null, b=null;
		
		String jsonSch = metaData.getAvroSchema();
		Schema schema = new Schema.Parser().parse(jsonSch); //TODO: ??  com.fasterxml.jackson.core.JsonParseException

		KafkaConsumer<Long, byte[]> kafkaConsumer = createKafkaAVROConsumer();
		kafkaConsumer.subscribe(Arrays.asList(topic));

		xformEngine jsEngin = new xformEngine();
		
		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);

			ByteArrayInputStream in;
			while (true) {
				records = kafkaConsumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<Long, byte[]> record : records) {
				    System.out.println("Partition: " + record.partition() 
				        + " Offset: " + record.offset()
				        + " Value: " + java.util.Arrays.toString(record.value()) 
				        + " ThreadID: " + Thread.currentThread().getId()  );
			//TODO: is that right? 
					in = new ByteArrayInputStream(record.value());
					BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
					kafkaConsumer.commitAsync();

					a = reader.read(null, decoder);
					//a = reader.read(b, decoder); 
					System.out.println(a);
					System.out.println(a.get(1));
					System.out.println(a.get(2));  //TODO: still in long, not DATE!?
					System.out.println(a.get(3)); 
					if(tgtData!=null) {
						b = jsEngin.transform(a);
						tgtData.write(a);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void testJSGraal() {
	   ScriptEngine graalEngine = new ScriptEngineManager().getEngineByName("graal.js");
	   //load the transformation JS script
	   try {
			graalEngine.eval("print('Hello Graal World!');");
			graalEngine.eval("function sum(a,b){return a.concat(b);}");
		    String v = (String)graalEngine.eval("sum(\"Hello, \", \"the other world!\")");
		    System.out.println(v);
	    } catch (ScriptException e) {
			e.printStackTrace();
		}   
	   return ;
	}
}
