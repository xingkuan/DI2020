module DI2020 {
	exports com.future.DI2020;

	requires elasticsearch.rest.client;
	requires java.scripting;
	requires java.sql;
	requires json.simple;
	requires kafka.clients;
	requires org.apache.avro;
	requires org.apache.httpcomponents.httpcore;
	requires org.apache.httpcomponents.httpcore.nio;
	requires org.apache.logging.log4j;
}