ARGS=""
optDEL=N
echo "options :"
while [ $# -gt 0 ]
do
   unset OPTIND
   unset OPTARG
   while getopts y  options
   do
    case $options in
         y)  echo "TO really delete"
             optDEL=Y
             ;;
    esac
   done
   shift $((OPTIND-1))
   ARGS="${ARGS} $1 "
   shift
done

tblID=$ARGS

repURL='vertsnap/BAtm0B1L#@RMAN01'

get_detailOfTblID () {
    sqlplus -s $repURL <<!
    set heading off
    set feedback off
    set pages 0
    select d.DB_DESC, t.SOURCE_SCHEMA||'.'||source_table, t.TARGET_SCHEMA||'.'||t.TARGET_TABLE
from sync_table t, sync_db d
where d.DB_ID = t.SOURCE_DB_ID
and t.table_id=$1 ;
!
}

dtls=$(get_detailOfTblID $tblID)
echo $dtls
rslt=($dtls)
SRCDB=${rslt[0]}
SRCTBL=${rslt[1]}
TGTTBL=${rslt[2]}
echo Src DB : $SRCDB
echo Src Tbl: $SRCTBL
echo Tgt Tbl: $TGTTBL


if [ "Y" = optDEL ]; then
  echo "...to delete..."
  # delete from Repository:
  sqlplus -s $repURL <<!
  set pages 0
  delete VERTSNAP.SYNC_TABLE_FIELD
  where table_id=$tblID ;
  delete VERTSNAP.SYNC_TABLE
  where table_id=$tblID ;
  commit;
!
  #
  # drop target table in VertX
  #
  VHOST=vertx1
  VUSER=dbadmin
  VPASS="Bre@ker321"

  #read -p "create tgt tbl in vertica. Press enter to continue ..."
  #echo vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS $TGTTBL
  vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS -c "drop table $TGTTBL"
else
  echo "provide option -y to unregister!"
fi
