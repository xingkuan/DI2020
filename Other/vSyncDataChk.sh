export ORACLE_HOME=/opt/oracle/instantclient_11_2
export TNS_ADMIN=$ORACLE_HOME
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME
export PATH=$PATH:$ORACLE_HOME:/opt/vertica/bin/
export SQLPATH=$ORACLE_HOME

#
# Need rework [2018.08.15]:
#  problem: "deleted in Oracle but not replicated to vertica" can lead to re-init 
#

DDLTBLLST=/tmp/VSYNC/DDLMOD.lst
WRKDIR=/tmp/VSYNC
# to Check if any data out of sync due to DDL change
# example usage:
#  vSyncDataChk.sh CRM system/lanchong@CRMP64 CRMDEV VCRM
#  vSyncDataChk.sh JOTPP system/cal618@JOTPP SEWN VSEWN
dbName=$1
srcDBUrl=$2
sschName=$3
tschName=$4


fun_genRowIDs() {
  local sschName=$1
  local tschName=$2
  local tblName=$3

#  echo ${sschName}
#  echo ${tschName}
#  echo ${tblName}

vsql -A -t -d vertx -h vertx1 -U dbadmin -w Bre@ker321 -o ${WRKDIR}/XXX -c "select gu_rowid from ${tschName}.${tblName} tablesample(10) limit 100"

ed ${WRKDIR}/XXX <<STMT
1,\$s/^/'
1,\$s/\$/',
\$
s/,//
w
q
STMT

#local rslt=`wc -l < /tmp/XXX`
#
#echo $rslt
}


fun_oraCnt() {
  local sschName=$1
  local tschName=$2
  local tblName=$3

  echo "select count(1) from ${sschName}.$tblName a where rowid in (" > ${WRKDIR}/${tblName}O.sql
  cat ${WRKDIR}/XXX >> ${WRKDIR}/${tblName}O.sql
  echo ") ; " >> ${WRKDIR}/${tblName}O.sql
    sqlplus -s $srcDBUrl <<STMT
set heading off
set feedback off
set echo off
set verify off
set pages 0
set lines 2000
set trimspool on
set trimout on
set termout off
set colsep   "|"
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';
spool ${WRKDIR}/${tblName}O.txt
@${WRKDIR}/${tblName}O.sql
exit
STMT

#local rslt=$(cat ${WRKDIR}/${tblName}O.txt | sed 's/[^0-9]*//g')
#
#echo $rslt
}

fun_sendMatrix()
{
    curl -i -XPOST http://grafana01:8086/write?db=vsync --data-binary "vSyncMismatch,key=$1 value=$2"
    echo "${dbName}|${sschName}|$1" >> $DDLTBLLST
    # truncate the target in Vertica
    vsql -A -t -d vertx -h vertx1 -U dbadmin -w Bre@ker321 -c "truncate table ${tschName}.$1} "
    return $?
}


#### MAIN ####
##############
# make sure /tmp/VSYNC/DDLMOD.lst is empty
#> $DDLTBLLST
### To Be Called by a MASTER script vSyncChk.sh
### in it, $DDLTBLLST will be cleaned before each run
#############
# generate a list of table
sqlplus -s /nolog <<STMT
define repDBtns = 'RMAN01'
define repDBuser = 'vertsnap'
define repDBpwd = 'BAtm0B1L#'

connect &repDBuser/&repDBpwd@&repDBtns;
set termout off
set heading off
set feedback off
set echo off
set verify off
set lines 300

spool ${WRKDIR}/tmpTbls.lst
select source_table from sync_table t,sync_db d where d.db_id=t.source_db_id and d.DB_DESC='$dbName' and t.source_schema='$sschName' and rownum<4;
exit;
STMT

#vi /tmp/tmpTbls.lst -c ':1,$s/  *$//g' -c ':1,$/^$/d'  -c':wq'
#vi /tmp/tmpTbls.lst -c ':1,$/^$/d' -c':wq'

grep -v ^$ ${WRKDIR}/tmpTbls.lst | while read ln
do
    echo $ln
    # get samle rowid, and data from vertX
    #./mon1.sh $sschName $tschName $ln
    fun_genRowIDs $sschName $tschName $ln
    rowCnt=`wc -l < ${WRKDIR}/XXX`
    echo row count $rowCnt

    if [[ $rowCnt == 0 ]] ; then
      echo skip!
    else
      echo compare $ln ...
      fun_oraCnt $sschName $tschName $ln
      mis=`cat ${WRKDIR}/${ln}O.txt|grep ERROR|wc -l`
      if [[ $mis > 0 ]] ; then
        echo mismatch $ln
        fun_sendMatrix $ln 10   ## just hard code a number here!
      else
        srcCnt=$(cat ${WRKDIR}/${ln}O.txt | sed 's/[^0-9]*//g')
        echo source count $srcCnt
        diff=$(( $rowCnt - $srcCnt ))
        echo difference $diff
        if [[ $diff == 0 ]] ; then
          echo $ln okay!
        else
          echo $ln $diff
          # echo and report to Grafana
          fun_sendMatrix $ln $diff
        fi
      fi
      rm ${WRKDIR}/${ln}O.txt
      rm ${WRKDIR}/${ln}O.sql
    fi
    rm ${WRKDIR}/XXX

done

rm ${WRKDIR}/tmpTbls.lst
