package com.future.DI2020;

public class RegXform0 {
//TODO: to be implemented.
	/*
	 * 
	 * 
	 * 
not okay
{"namespace": "com.future.DI2020.avro", 
"type": "record", 
"name": "VERTSNAP.TESTOK", 
"fields": [ 
{"name": "COL", "type": "long"} 
, {"name": "COL2", "type": ["string", "null"]} 
, {"name": "COL3", "type": ["null","string"], "logicalType": "date"} 
, {"name": "COL4",  "type": ["string","null"], "logicalType": "timestamp-micro"} 
, {"name": "ORARID", "type": "string"} 
] }

not good.
{"namespace": "com.future.DI2020.avro", 
"type": "record", 
"name": "VERTSNAP.TESTOK", 
"fields": [ 
{"name": "COL", "type": "long"} 
, {"name": "COL2", "type": ["string", "null"]} 
, {"name": "COL3", "type": ["null", {"type": "long","logicalType": "date"}]} 
, {"name": "COL4", "type": ["null", {"type": "long","logicalType": "timestamp-micros"}]} 
, {"name": "ORARID", "type": "string"} 
] }
{"COL": 1, "COL2": null, "COL3": null, "COL4": null, "ORARID": "AAEMYfAAwAAAABiAAA"}
{"COL": 4, "COL2": "test 4", "COL3": 1591772400000, "COL4": null, "ORARID": "AAEMYfAAwAAAABiAAB"}

not good:
{"namespace": "com.future.DI2020.avro", 
"type": "record", 
"name": "JOHNLEE2.TESTTBL2", 
"fields": [ 
  {"name": "COL1", "type": "string"} 
, {"name": "COL2", "type":["string","null"], "logicalType": "date"} 
, {"name": "COL5", "type":"long"} 
, {"name": "COL6", "type": "string"} 
, {"name": "COL7", "type": "string"} 
, {"name": "COL8", "type":["string","null"], "logicalType": "timestamp-micros"} 
, {"name": "COL#", "type": "string"} 
, {"name": "DB2RRN", "type":"long"} 
] }	 

not okay
{"namespace": "com.future.DI2020.avro", 
"type": "record", 
"name": "JOHNLEE2.TESTTBL2", 
"fields": [ 
  {"name": "COL1", "type": ["string", "null"]} 
, {"name": "COL2", "type": ["string", "null"], "logicalType": "date"} 
, {"name": "COL5", "type": ["long", "null"]} 
, {"name": "COL6", "type": ["string", "null"]} 
, {"name": "COL7", "type": ["string", "null"]} 
, {"name": "COL8", "type": ["string","null"], "logicalType": "timestamp-micros"} 
, {"name": "COLNUM", "type": ["string", "null"]} 
, {"name": "DB2RRN", "type": "long"} 
] }

{"namespace": "com.future.DI2020.avro", 
"type": "record", 
"name": "JOHNLEE2.QSQJRN", 
"fields": [ 
{"name": "null", dbl} 
] }

	 */
}
