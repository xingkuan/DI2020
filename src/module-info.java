module projDI {
	requires java.sql;
	requires kafka.clients;
	requires json.simple;
	requires org.apache.logging.log4j;
	requires jt400;
	requires vertica.jdbc;
	requires com.oracle.database.jdbc;
	requires org.apache.avro;
	requires com.fasterxml.jackson.core;
	requires com.fasterxml.jackson.annotation;
	requires elasticsearch.rest.client;
	requires org.apache.httpcomponents.httpcore;
	requires org.apache.httpcomponents.httpcore.nio;
	requires java.scripting;
}