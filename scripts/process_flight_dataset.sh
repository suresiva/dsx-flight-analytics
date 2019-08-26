#!/bin/bash

#function to log the std out and std error messages into the log file
###########################################################################
function writeToLog()
###########################################################################
{
    echo "$(date) : $@" >> ${scriptFilePath}/../logs/$logFileName 2>&1
}
###########################################################################

###########################################################################
#  MAIN PROGREAM BEGINS HERE
###########################################################################

    # recording the job start time
    scriptStartTime=$(date +%Y_%m_%d_%H%M%S)
    startTimeLog=`date +%s`   

    # getting the script file name
    programFileName=`basename "$0"`
    scriptFilePath=$(dirname $0)

    configFilePath="${scriptFilePath}/job.properties"

    logFileName="$programFileName-$scriptStartTime.log"

    if [ $# -lt 1 ]; then
	echo "ERROR: missing argument, expects load/downstream/queries values"
	exit 1
    fi

    #creating the log file for current job execution
    touch ${scriptFilePath}/../logs/$logFileName   
    echo "check the $logFileName file for logs of this job..." 

    writeToLog "loading the job configuration from job.properties file"
    cp ${scriptFilePath}/../files/job.properties.template ${scriptFilePath}/../files/job.properties

    dse_spark_master="`dse client-tool spark master-address`"
    tbd_flag="TBD"

    writeToLog "dse spark master = ${dse_spark_master}"

    sed -i "s|TBD|${dse_spark_master}|g" ${scriptFilePath}/../files/job.properties

    if [ $1 = "load" ]; then
	writeToLog "loading the given flight data set into cassandra table."
	writeToLog "invloking the spark-submit command,"
	payload_class="com.datastax.flights.batch.dataload.FlightsInputLoader"

    elif [ $1 = "downstream" ]; then
        writeToLog "loading the flights table data airport_departure and flights_airtime tables."
        writeToLog "invloking the spark-submit command,"
        payload_class="com.datastax.flights.batch.dataload.DownstreamTablesLoader"        
    
    elif [ $1 = "queries" ]; then
        writeToLog "executing the application to extract all 6 questions' insight from all tables."
        writeToLog "invloking the spark-submit command,"
        payload_class="com.datastax.flights.batch.insights.QueryInsightsWorker"

    else
       writeToLog "wrong input was received"
    fi

    `dse spark-submit --class "${payload_class}" ${scriptFilePath}/../files/dsx-flight-analytics-0.0.1.jar ${scriptFilePath}/../files/job.properties >> ${scriptFilePath}/../logs/$logFileName 2>&1`
