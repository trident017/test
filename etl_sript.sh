#!/bin/bash
#*******************************************************************************************************#
#  DEPARTMENT     : DCI - Data Center Intelligence                                                      #
#  DATA DOMAIN    : POWER                                                                               #
#  FREQUENCY      : Daily                                                                               #
#  NAME           : etl_sript.sh                                                        				#
#*******************************************************************************************************#
# MODIFICATION HISTORY
#-------------------------------------------------------------------------------------------------------#
#   purpose: Script will Extract Data from Source and Load into Target DataWarehouse.                   #
#                                                                                                       #
#   Date                           Author                Comments:                                      #
#-------------------------------------------------------------------------------------------------------#
#   YYYYMMDD                       **************       Initial Version                                 #
#*******************************************************************************************************#
JOB_ID=`date +'%Y%m%d%H%M%S'`
JOB_NAME=$(basename $0 .sh)
export APP=app_name1
. ${ETL_FRAMEWORK}/set_etl_env_global.sh
. ${ETL_FRAMEWORK}/common_repo.env
. ${APP_CONFIG_DIR}/etl_config_file.config
#Project Specific Start
ETL_JOB_RUN_ID=$(date -u +\%Y\%m\%d\%H\%M\%S)
#Project Specific End

#####################
# CDC date pull
#####################
incremental_date()
{
    WRITE_LOG "Start.. Pulling date from database audit table for CDC"
    WRITE_LOG "AUDITTABLE_JOBTYPE = $AUDITTABLE_JOBTYPE"
    if [ ${FULL_LOAD} = 'Y' ];then
        INCR_VAL=${DEFAULT_DATE}
    else
        INCR_VAL=`${VSQL_NO_TUPLES} -c "SELECT p.start_time|| '~' ||trunc(sysdate) end_time FROM (SELECT REPLACE(TRUNC(processendts)::VARCHAR,' ','T') start_time FROM ${SCHEMA_NAME}.${AUDITTABLE} WHERE jobtype = '${AUDITTABLE_JOBTYPE}') p;"`
        die $? "Error.. while pulling CDC date from db"
    fi

    export starttime=`echo ${INCR_VAL} | awk -F'~' '{print $1}'`
    export endtime=`echo ${INCR_VAL} | awk -F'~' '{print $2}'`
    WRITE_LOG "starttime = $starttime"
    WRITE_LOG "endtime = $endtime"

    WRITE_LOG "Complete..Incremental date pulled"

}

#########################
# Source extraction
#########################
source_extract()
{

    WRITE_LOG "Removing previous iteration response files"
    rm -f $INPUT_DIR/api_response_json*.json
    rm -f $INPUT_DIR/data_file.txt
    WRITE_LOG "Start.. Python extraction"
    RETURN_MSG=$(python ${SCRIPTS_DIR}/api_extract_json_parser.py 2>&1)
    RETURN_CODE=$?
    RETURN_MSG=$(echo $RETURN_MSG | sed "s/\([\'|\"]\)//g")
    die $RETURN_CODE "$RETURN_MSG"
    WRITE_LOG "Complete.. Python extraction is completed"

}

###############
#Stage load
###############
stage_load()
{
    WRITE_LOG "Start.. stage load"
    rows_extracted=`cat ${INPUT_DIR}/data_file.txt | wc -l`
    if [ "$rows_extracted" -gt "1" ]; then
        ${VSQLALL} <<EOF >> ${LOGFILE}
            \set ON_ERROR_STOP on
            TRUNCATE TABLE ${SCHEMA_NAME}.${STG_TABLE};
            COPY ${SCHEMA_NAME}.${STG_TABLE}(${STG_TABLE_COLUMNS}) 
            FROM LOCAL '${INPUT_DIR}/data_file.txt' 
            DELIMITER E'\011' DIRECT NULL AS 'NULL' 
            EXCEPTIONS '${INPUT_DIR}/EXCPT_FILE.txt' 
            REJECTED DATA '${INPUT_DIR}/REJECTED_FILE.txt' 
            SKIP 1;
EOF

        die $? "Error.. while loading data to stage tables."

        if [ -s "${INPUT_DIR}/REJECTED_FILE.txt" ]; then
            WRITE_LOG "Exceptions Found. Archiving exception and rejection files"
            mv ${INPUT_DIR}/REJECTED_FILE.txt ${INPUT_ARC_DIR}/REJECTED_FILE_$ETL_JOB_RUN_ID.txt
            mv ${INPUT_DIR}/EXCPT_FILE.txt ${INPUT_ARC_DIR}/EXCPT_FILE_$ETL_JOB_RUN_ID.txt
            die 1 "FAILED : Some of the records are rejected while loading data into ${SCHEMA_NAME}.${STG_TABLE}. Please check ${LOGFILE}  ...Aborting."
        else
            WRITE_LOG "Removing exception and rejection files"
            rm -f ${INPUT_DIR}/REJECTED_FILE.txt
            rm -f ${INPUT_DIR}/EXCPT_FILE.txt
            WRITE_LOG "Archiving CSV Data file for AMTRAC Assets"
            mv ${INPUT_DIR}/data_file.txt ${INPUT_ARC_DIR}/data_file_${ETL_JOB_RUN_ID}.txt
            gzip ${INPUT_ARC_DIR}/data_file_${ETL_JOB_RUN_ID}.txt
        fi
    else
        WRITE_LOG "No Data Updated in Source since the last extract. So cleaning off data file."
        rm -f ${INPUT_DIR}/data_file.txt
        WRITE_LOG "Start: CLOSE_AUDIT_RECORD"
        CLOSE_AUDIT_RECORD \ || eval WRITE_LOG "Error in CLOSE_AUDIT_RECORD, aborting..."\; die 1
        WRITE_LOG "Complete: CLOSE_AUDIT_RECORD"

        WRITE_LOG "Start: SEND_COMPLETED_EMAIL"
        SEND_COMPLETED_EMAIL \ || eval WRITE_LOG "Error in SEND_COMPLETED_EMAIL, aborting..."\; die 1
        WRITE_LOG "Complete: SEND_COMPLETED_EMAIL"

        exit 0
    fi

    WRITE_LOG "Stage load completed"

}

#############
# Fact load
#############
fact_load()
{
    WRITE_LOG "Start..fact load"
    ${VSQLALL} <<EOF >> ${LOGFILE}
        \set ON_ERROR_STOP on
        INSERT /*+DIRECT*/ INTO ${SCHEMA_NAME}.${HIST_TABLE} SELECT sysdate::date, $ETL_JOB_RUN_ID, src.* FROM ${SCHEMA_NAME}.${FACT_TABLE} SRC;
        DELETE /*+DIRECT*/ FROM ${SCHEMA_NAME}.${FACT_TABLE} WHERE ID IN (SELECT ID FROM ${SCHEMA_NAME}.${STG_TABLE});
        INSERT /*+DIRECT*/ INTO ${SCHEMA_NAME}.${FACT_TABLE}(${STG_TABLE_COLUMNS}) 
        SELECT ${STG_TABLE_COLUMNS} 
        FROM (
            SELECT ${STG_TABLE_COLUMNS},
            row_number() over (partition by id order by lastmodifieddate desc) rnk
            from ${SCHEMA_NAME}.${STG_TABLE}
        )a
        where rnk = 1;
        delete from ${SCHEMA_NAME}.${HIST_TABLE} where ARCHIVAL_DT <= sysdate::date - $RETENTION_PERIOD_DAYS;
        COMMIT;
EOF

    die $? "Error.. while loading data from stage to fact table"
    WRITE_LOG "Complete.. fact loaded successfully"

}

#######################
# Updating audit table
#######################
upd_audit_dates()
{
    WRITE_LOG "Inside audit table update"

    if [ ${FULL_LOAD} = 'Y' ];then
        WRITE_LOG "As Manual full load ran, we are not updating the control table"
    else

    ${VSQLALL} << EOF >> $LOGFILE
        \set ON_ERROR_STOP on
        UPDATE /*+DIRECT*/ ${SCHEMA_NAME}.${AUDITTABLE} SET processendts='${endtime}',PROCESSSTARTTS='${endtime}',ETL_JOB_RUN_ID=${ETL_JOB_RUN_ID},ETL_JOB_ID=${ETL_JOB_ID} WHERE jobtype = '${AUDITTABLE_JOBTYPE}';
        COMMIT;
EOF

    fi
    die $? "Error while updating the audit table"
    WRITE_LOG "Completed updating audit table"
}

#####################
### FUNCTION MAIN ###
#####################
main()
{

    WRITE_LOG "Start: OPEN_AUDIT_RECORD"
    OPEN_AUDIT_RECORD \ || eval WRITE_LOG "Error in OPEN_AUDIT_RECORD, aborting..."\; die 1
    WRITE_LOG "Complete: OPEN_AUDIT_RECORD"

    WRITE_LOG "Start: incremental_date"
    incremental_date \ || eval WRITE_LOG "Error in incremental_date, aborting..."\; die 1
    WRITE_LOG "Complete: incremental_date"

    WRITE_LOG "START: source_extract"
    source_extract \ || eval WRITE_LOG "Error in source_extract, aborting..."\; die 1
    WRITE_LOG "COMPLETED: source_extract"

    WRITE_LOG "START: stage_load"
    stage_load \ || eval WRITE_LOG "Error in stage_load, aborting..."\; die 1
    WRITE_LOG "COMPLETED: stage_load"

    WRITE_LOG "START: fact_load"
    fact_load \ || eval WRITE_LOG "Error in fact_load, aborting..."\; die 1
    WRITE_LOG "COMPLETED: fact_load"

    WRITE_LOG "Start: upd_audit_dates"
    upd_audit_dates \ || eval WRITE_LOG "Error in upd_audit_dates, aborting..."\; die 1
    WRITE_LOG "Complete: upd_audit_dates"

    WRITE_LOG "Start: CLOSE_AUDIT_RECORD"
    CLOSE_AUDIT_RECORD \ || eval WRITE_LOG "Error in CLOSE_AUDIT_RECORD, aborting..."\; die 1
    WRITE_LOG "Complete: CLOSE_AUDIT_RECORD"

    WRITE_LOG "Start: SEND_COMPLETED_EMAIL"
    SEND_COMPLETED_EMAIL \ || eval WRITE_LOG "Error in SEND_COMPLETED_EMAIL, aborting..."\; die 1
    WRITE_LOG "Complete: SEND_COMPLETED_EMAIL"

}

echo "====================================================================================" > $LOGFILE
echo "                               START TIME: $(date) " >> $LOGFILE
echo "====================================================================================" >> $LOGFILE
WRITE_LOG "Calling Main"
main \ || eval WRITE_LOG "Error in main, Abording ..." \; die 1
WRITE_LOG "Completed Main"
echo "====================================================================================" >> $LOGFILE
echo "                               END TIME: $(date) " >> $LOGFILE
echo "====================================================================================" >> $LOGFILE