package com.future.DI2020;

public class RegXform0 {
//TODO: to be implemented.
	/*
	 * 
	 * 
	 * 
AVRO:
---------
seems okay
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

?
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

:transform:
----------------------------
ES:
PUT /testok
{
    "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas" : 1
    },
    "mappings" : {
        "properties" : {
            "col" : { "type" : "integer" },
            "col2" : { "type" : "text" },
            "col3" : { "type" : "date" },
            "col4" : { "type" : "date" },
            "ORARID" : { "type" : "keyword" }
        }
    }
}

insert into task (TASK_ID, TEMPLATE_ID, TASK_CAT, DATA_PK, 
	SRC_DB_ID, SRC_SCHEMA, SRC_TABLE, 
	TGT_DB_ID,TGT_SCHEMA,  TGT_TABLE, 
	POOL_ID, CURR_STATE,  
	SRC_DCC_PGM, SRC_DCC_TBL, 
	DCC_DB_ID, DCC_STORE, 
	TS_REGIST) 
values  ( 11, 'XFRM', 'XFRM', 'ORARID',  
		'KAFKA1', 'TEST', 'TESTOK',  
		'ES1', '', 'testok',
		-5, 0, 
		'XFRM', 'na',  
		'na', 'na', 
		now()
)

--NOTE: field name in javascript (XFORM0 field) is case-sensitive!
insert into XFORM_SIMPLE  (X_ID,  
SRC_AVRO, 
TGT_AVRO, 
XFORM0
) values (11,
'{"namespace": "com.future.DI2020.avro", 
"type": "record", 
"name": "VERTSNAP.TESTOK", 
"fields": [ 
{"name": "COL", "type": "long"} 
, {"name": "COL2", "type": ["string", "null"]} 
, {"name": "COL3", "type": ["string", "null"], "logicalType": "date"} 
, {"name": "COL4", "type": ["string","null"], "logicalType": "timestamp-micros"} 
, {"name": "ORARID", "type": "string"} 
] }',
'',
'[
{"name": "COLX", "script": "function COLX(){ return rec.COL + rec.COL2 + rec.COL3;}"},
{"name": "COLY", "script": "function COLY(){return rec.COL4;}"},   
{"name": "ORARID", "script": "function ORARID(){return rec.ORARID;}"}
]'
);



	 */
}
