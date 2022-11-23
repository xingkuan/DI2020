systemctl status postgresql-9.6
postgres=# show data_directory;
     data_directory
-------------------------
 /var/lib/pgsql/9.6/data
cd /var/lib/pgsql/9.6/data
vi pg_hba.conf
psql -h dbatool01 -d didb -U repuser -W


create database didb;

create user repuser password 'passwd';
grant connect on database didb to repuser;
grant all privileges on database didb to repuser;

\c didb

CREATE TABLE DB_ENGINE
(
  DB_ENGINE  VARCHAR(15)   PRIMARY KEY,
  DB_VENDOR  VARCHAR(15),
  DB_DRIVER  VARCHAR(150),
  SQL_FLD_TMPLT TEXT,
  AVRO_FLD_TMPLT TEXT
)
;
grant select,update,delete,insert on db_engine to repuser;

CREATE TABLE DATA_POINT
(
  DB_ID      VARCHAR(15)   PRIMARY KEY,
  DB_ENGINE  VARCHAR(15),
  DB_ROLE    VARCHAR(5),  --reference only. R for Read from or W for Write to
  DB_CAT     VARCHAR(15),
  DB_CONN    VARCHAR(150),
  DB_USR     VARCHAR(25),
  DB_PWD     VARCHAR(25),
  DB_DESC    VARCHAR(225),
  INSTRUCT   TEXT    --the templates of registration, initialization, remove, incremental...
)
;
grant select,update,delete,insert on data_point to repuser;

-- Let's explicitly know that the DB are RDBMS or KAFKA, for both SRC, TGT and DCC
CREATE TABLE TASK
(
  TASK_ID         INTEGER PRIMARY KEY,
  CDCTASK_ID      INTEGER,
  POOL_ID         INTEGER,
  CURR_ST      INTEGER,	-- -1: not runable(e.g populated by DB trigger); 
  							--  0: not initialized; 
  							--  1: disabled;
  							--	2: runable;
  							--	3: being run 
  SRC_DB_ID       VARCHAR(15),
  SRC_TBL         VARCHAR(50),
  SRC_STMT        TEXT,		--if customer SQL is used.
  FLD_CNT      int,
  TGT_DB_ID       VARCHAR(15),
  TGT_TBL         TEXT,		-- can be a list of, eg. DB2 journal table members
  TGT_STMT        TEXT,		--if JDBC.
  FLDS     TEXT,
  REG_DT            DATE,
  INIT_DT	        DATE,
  INIT_DURATION     INTEGER,	-- seconds
  TS_LAST_REF       TIMESTAMP(6),
  SEQ_LAST_REF      BIGINT
--  CONSTRAINT unique_src UNIQUE (SRC_DB_ID, SRC_SCHEMA, SRC_TABLE)
)
; 
grant select,update,delete,insert on task to repuser;

CREATE TABLE XFORM_SIMPLE  (
   X_ID        INTEGER PRIMARY KEY,  --Let it be that of task_id
   SRC_AVRO    varchar(2000),
   TGT_AVRO    varchar(2000),
   XFORM0      varchar(2000)
)
;

create user repuser password 'passwd';
grant connect on database didb to repuser;
grant all privileges on database didb to repuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES in schema public to repuser;

---------------------------------------------------
insert into DB_ENGINE (
  DB_ENGINE, DB_VENDOR,
  DB_DRIVER,
  SQL_FLD_TMPLT,
  AVRO_FLD_TMPLT
)values (
 'ORACLE','ORACLE',
 'oracle.jdbc.OracleDriver',
 '{
["INT","NUMBER(10)"],
["LONG","NUMBER(19)"],
["BOOL","NUMBER(1)"],
["UTF8","BYTE_ARRAY	RAW(2000)"],
["FLOAT","BINARY_FLOAT"],
["DOUBLE","BINARY_DOUBLE"],
["DECIMAL(<p>)","NUMBER(<p>)"],
["DECIMAL(<p>,<s>)","NUMBER(<p>,<s>)"],
["DATE","DATE"],
["STRING","VARCHAR2"],
["TIME-MILLIS","TIMESTAMP(3)"],
["TIME-MICROS","TIMESTAMP(6)"]		 
 }', 
 '{
["INT","NUMBER(10)"],
["LONG","NUMBER(19)"],
["BOOL","NUMBER(1)"],
["UTF8","BYTE_ARRAY	RAW(2000)"],
["FLOAD","BINARY_FLOAT"],
["DOUBLE","BINARY_DOUBLE"],
["type": {
       "type": "bytes",
       "logicalType": "decimal",
       "precision": <p>
     }","NUMBER(<p>)"],
["type": {
       "type": "bytes",
       "logicalType": "decimal",
       "precision": <p>,
       "scale": <s>
     }","NUMBER(<p>,<s>)"],
["type": {
        "type": "int",
        "logicalType": "date"
      }","DATE"],
["type": "string","VARCHAR2"],
["type": {
        "type": "int",
        "logicalType": "time-millis"
      },"TIMESTAMP(3)"],
["type": {
        "type": "long",
        "logicalType": "time-micros"
      },"TIMESTAMP(6)"]		 
 }'
);

insert into DB_ENGINE (
  DB_ENGINE, DB_VENDOR,
  DB_DRIVER,
  SQL_FLD_TMPLT,
  AVRO_FLD_TMPLT
)values 
('DB2/AS400','IBM',
 'com.ibm.as400.access.AS400JDBCDriver', 
 '{
 [ ]
 }',
 '{
 [ ]
 }' 
 );

insert into DB_ENGINE (
  DB_ENGINE, DB_VENDOR,
  DB_DRIVER,
  SQL_FLD_TMPLT,
  AVRO_FLD_TMPLT
)values 
('VERTICA','VERTICA',
 'com.vertica.jdbc.Driver', 
 '{
 [ ]
 }',
 '{
 [ ]
 }' 
 );
insert into DB_ENGINE (
  DB_ENGINE, DB_VENDOR,
  DB_DRIVER,
  SQL_FLD_TMPLT,
  AVRO_FLD_TMPLT
)values 
('KAFKA','APACHE',
 '', 
 '{
 [ ]
 }',
 '{
 [ ]
 }' 
 );
insert into DB_ENGINE (
  DB_ENGINE, DB_VENDOR,
  DB_DRIVER,
  SQL_FLD_TMPLT,
  AVRO_FLD_TMPLT
)values 
('ES','ES',
 '', 
 '{
 [ ]
 }',
 '{
 [ ]
 }' 
 );
 
---------------------------------------------
insert into DATA_POINT (
  DB_ID,DB_ENGINE, DB_ROLE, DB_CAT, 
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values 
('DB2AK', 'DB2/AS400', 'R', 'JDBC', 
 'jdbc:as400://DEVELOPM:2551/DB2_RETSYS', 
 'johnlee2', 'C3line1998', 
 'DB2/AS400 data id',
 '{
  "cdcDB":"KAFKAK1",
  "cdcTbl":"<SRCTBL>",	
  "bareSQL":"select COUNT_OR_RRN as DATAKEY,SEQUENCE_NUMBER AS SEQNBR 
  FROM table (Display_Journal(''<DIJRNL>'', ''<DIJMBR>'', ''*CURCHAIN'', '''', '''',
  cast(''<DICDCTS>'' as TIMESTAMP), cast(''<DICDCSEQ>'' as decimal(21,0)),
   ''R'','''','''' ,'''',''*QDDS'','''','''','''','''') ) as x
   order by 2 asc",
  "bareSQL1":"select COUNT_OR_RRN as DATAKEY,SEQUENCE_NUMBER AS SEQNBR
  FROM table (Display_Journal(''<DIJRNL>'', ''<DIJMBR>'', '''', '''', '''',
  cast(''<DISTARTTS>'' as TIMESTAMP), cast(''<DISTARTSEQ>'' as decimal(21,0)),
  ''R'','''','''' ,'''',''*QDDS'','''','''','''','''') ) as x
  order by 2 asc",
  "regist":[],
  "unregist":[],
  "initRun":[
   {"note":"if the CDC marks are not set yet, set them.", 
    "cmd":"select max(COUNT_OR_RRN) as CDCDATAKEY,max(SEQUENCE_NUMBER AS SEQNBR) as CDCSEQNUM, max(ENTRY_TIMESTAMP) as CDCTS
     FROM table (Display_Journal(''<DIJRNL>'', ''<DIJMBR>'', ''*CURCHAIN'', '''', '''',
     cast(''<DISTARTTS>'' as TIMESTAMP),cast(''<DISTARTSEQ>'' as decimal(21,0)),  
     ''R'','''','''','''',''*QDDS'','''','''','''','''') ) as x 
     order by 2 asc", 
    "type":"MULTIV", 
    "return": ["KEY, SEQ","TS"] 
   }
  ] 
 }'
 );
insert into DATA_POINT (
  DB_ID,DB_ENGINE, DB_ROLE, DB_CAT, 
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values 
('DB2ADS', 'DB2/AS400', 'R', 'JDBC', 
 'jdbc:as400://DEVELOPM:2551/DB2_RETSYS', 
 'johnlee2', 'C3line1998', 
 'DB2/AS400 data',
 '{
   "cdcDB":"KAFKAK1",	
   "bareSQL": "select a.*, rrn(a) as datakey from <DISRCTBL> as a",
   "incRun": [],
   "incRun":[],
   "regist":[],
   "unregist":[]
  }'
 );
 
insert into DATA_POINT (
  DB_ID,DB_ENGINE,DB_ROLE,DB_CAT, 
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values(
'VERTD1', 'VERTICA', 'W', 'JDBC', 
 'jdbc:vertica://vert41:5433/vertc', 
 'dbadmin', 'D0gB0ne5y', 
 'Vert x',
 '{	
  "notes":"no specific instrunction for Vertica destination" ,
  "regist": [
    {"name":"create tgt tbl",
     "cmd":"drop table <CDCTBL>",
     "type":"AVRO2TBL"
    },
    {"name":"maybe grant", 
     "cmd":"grant ...",
     "type":"not implemented"
    }
  ]
  }'
 );
 
insert into DATA_POINT (
  DB_ID,DB_ENGINE,DB_ROLE, DB_CAT, 
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values(
 'ORAK1', 'ORACLE', 'R','JDBC', 
 'jdbc:oracle:thin:@crmdbtest2:1521:CRMP64', 
 'VERTSNAP', 'v3rtsnap', 
 'Oracle Dev key source',
 '{	
  "note":"trigger based approach for CDC",
  "cdcObj":"CDC<DISRCTBL>",
  "cdcObjNameLen":15,
  "bareSQL": "select CDC_DATA_KEY from <CDCLOG>",
  "initRun":[
   {"note":"get the CDC TS, which will be saved in repo",
   	"cmd":"select sysdate from from dual",
    "type":"SINGLEV", 
   	"return": ["TS"]
   },
   {"name":"enable cdc trigger", 
    "cmd":"alter trigger <CDCLOG> enable",
    "type":"NOV", 
   }
  ]
  "incRun":[ ],
  "preRegist":[
   {"name":"verify log tbl name not used",
    "cmd":"select case when 
    exists(select 0 from user_objects where OBJECT_NAME=''<CDCTBL>'') then -1 else 0  end as rtcode 
    from dual",
    "type":"SINGLEV", 
   	"return": ["RC"]
   },
   {"name":"verify log trigger name is not used",
    "cmd":"select case when 
    exists(select 0 from user_objects where OBJECT_NAME=''<CDCTRG>'') then -1 else 0  end as rtcode 
    from dual",
    "type":"SINGLEV", 
   	"return": ["RC"]
   }
  ],
  "regist": [
   {"name":"create log tbl name",
    "cmd":"create table <CDCTBL> (CDCKEY varchar2(20), action char(1), ts_dcc date) ",
    "type":"NOV"
   },
   {"name":"create log trigger", 
    "cmd":"create or replace trigger <CDCTRG>   
		after insert or update or delete on <DISRCTBL>  
		for each row  
		begin
		DECLARE
    	action  char(1);
		BEGIN 
		IF DELETING THEN 
 			action := ''D'';
		END IF;
		IF INSERTING THEN 
 			action := ''I'';
		END IF;
		IF UPDATING THEN 
 			action := ''U'';
		END IF;
			
		insert into <CDCTBL> (datakey, action, ts_dcc )  
		values ( :new.rowid, sysdate );  
		end; ",
    "type":"NOV"
	},
    {"name":"trigger is disabled",
     "cmd":"alter trigger <CDCTRG> disable",
     "type":"NOV"
    }
  ],
  "unregist": [
    {"name":"drop cdc log tbl",
     "cmd":"drop table <CDCTBL>",
     "type":"NOV"
    },
    {"name":"drop cdc trigger", 
     "cmd":"drop trigger <CDCTRG>",
     "type":"NOV"
    }
  ], 
  "disable": [
    {"name":"disable cdc trigger", 
     "cmd":"alter trigger <CDCTRG> disable",
     "type":"NOV"
    }
   ] 
  }'
);

insert into DATA_POINT (
  DB_ID,DB_ENGINE,DB_ROLE,DB_CAT, 
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values(
 'ORAD1','ORACLE', 'R', 'JDBC', 
 'jdbc:oracle:thin:@crmdbtest2:1521:CRMP64', 
 'VERTSNAP', 'v3rtsnap', 
 'Oracle Dev',
 '{
  "note":"only action here will be the selecting the data",
  "CDCDB":"ORAK1",
  "CCDKEY":"a.rowid as CDCKEY",
  "bareSQL": "select a.*, a.rowid as CDCKEY from <DISRCTBL> a ",
 }'
);

insert into DATA_POINT (
  DB_ID,DB_ENGINE,DB_ROLE,DB_CAT, 
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values(
 'ORAD2','ORACLE', 'W', 'JDBC', 
 'jdbc:oracle:thin:@rhvrep:1523:DPRD', 
 'VERTSNAP', 'v3rtsn,@P', 
 'Oracle Dev',
 '{
  }'
 );

insert into DATA_POINT (
  DB_ID,DB_ENGINE,DB_ROLE, DB_CAT, 
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values( 
 'KAFKAK1', 'KAFKA', 'WR', 'MQ', 
 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'xxx', 'xxx', 
 'kafka data consumer',
 '{
  "note":"all details are actually handled in Java class."
  "regist": [
    {"name":"create topic",
     "cmd":"creade topic",
     "type":"NOV"
    }
  ],
  "unregist": [
    {"name":"delete topic",
     "cmd":"delete topic",
     "type":"NOV"
    }
  ]
 }'
);

insert into DATA_POINT (
  DB_ID,DB_ENGINE,DB_ROLE, DB_CAT,
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values (
 'KAFKAD1', 'KAFKA', 'WR', 'MQ', 
 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'xxx', 'xxx', 
 'kafka data sink',
 '{	
  "regist": [
    {"name":"create topic",
     "cmd":"creade topic",
     "type":"NOV"
    }
  ],
  "unregist": [
    {"name":"delete topic",
     "cmd":"delete topic",
     "type":"NOV"
    }
  ]
 }'
);
 
insert into DATA_POINT (
  DB_ID,DB_ENGINE,DB_ROLE, DB_CAT, 
  DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values 
('ES1DT', 'ES', 'DT','ES', 
 'http://dbatool02:9200', 
 'xxx', 'xxx', 
 'ElasticSearch',
'{	
  "regist": [
    {"name":"create index",
     "cmd":"creade index",
     "type":"NOV"
    }
  ],
  "unregist": [
    {"name":"drop index",
     "cmd":"drop index",
     "type":"NOV"
    }
  ]
 }'
)
;
--------------------------------

insert into XFORM_SIMPLE  (X_ID,  
SRC_AVRO, 
TGT_AVRO, XFORM0
) values (11,
'??',
'??'
);

---------------------
create table dp_job(job_id varchar(30), job_desc varchar(100), job_stmt jsonb);
grant all on dp_job to myavro;
INSERT INTO dp_job (job_id, job_desc, job_stmt) 
VALUES ('job 1', 'test ...',   
'{"src_avro": "user", 
  "src_url": "jdbc:oracle:thin:@crmdbclonetest:1521:crmp65", 
  "userID": "system",
  "userPW": "lanchong", 
  "src_driver": "oracle.jdbc.OracleDriver",
  "stmts": [
    {"name":"step 1",
     "stmt":"insert into johnlee.tuser (name, fav_num, fav_col) values (''from job 1'', 8, ''red'') ", 
     "rs": "n" },
    {"name":"step 2", 
     "stmt":"commit",
     "rs": "n" },
    {"name":"step 3",
     "stmt":"select * from johnlee.tuser",
     "rs": "y"}
  ]
}'
);

-- in Oracle, login as vertsnap: 
create table test (
c0 char(5), 
c1 varchar2(20), 
c2 int,
c3 number(8),
c4 number (8,2),
c5 number (12,2),
c6 raw(20),
c7 float,
c8 double precision,
c9 date,
c10 timestamp,
c11 timestamp(3),
c12 timestamp(6)
)
; 
-->
{"name": "C0,"type":"CHAR", "precison":5,"scale":0},
{"name": "C1,"type":"VARCHAR2", "precison":20,"scale":0},
{"name": "C2,"type":"NUMBER", "precison":38,"scale":0},   <<<
{"name": "C3,"type":"NUMBER", "precison":8,"scale":0},
{"name": "C4,"type":"NUMBER", "precison":8,"scale":2},
{"name": "C5,"type":"NUMBER", "precison":12,"scale":2},
{"name": "C6,"type":"RAW", "precison":0,"scale":0},    <<<
{"name": "C7,"type":"NUMBER", "precison":126,"scale":-127},   <<<
{"name": "C8,"type":"NUMBER", "precison":126,"scale":-127},   <<<
{"name": "C9,"type":"DATE", "precison":0,"scale":0},
{"name": "C10,"type":"TIMESTAMP", "precison":0,"scale":6},  <<<
{"name": "C11,"type":"TIMESTAMP", "precison":0,"scale":3},  <<<
{"name": "C12,"type":"TIMESTAMP", "precison":0,"scale":6},  <<<
{"name": "CDCKEY,"type":, "ROWID", "precison":0,"scale":0}  <<<
--https://www.striim.com/docs/en/data-type-support---mapping-for-postgresql-sources.html
