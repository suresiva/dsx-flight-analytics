package com.datastax.flights.batch.dataload

import org.apache.log4j.Logger
import java.util.Date
import com.datastax.flights.batch.common.RuntimeArguments
import com.datastax.flights.batch.common.ArgumentParser
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.analysis.function.Ceil

object AirportDepartureLoad {
  
    val logger = Logger.getLogger("AirportDepartureLoad")
    var arguments:RuntimeArguments = null
    
    def main(args:Array[String]){
    
        logger.info("batch load has started at "+ new Date())
        
        try{
          
            /** parsing the runtime arguments from properties file or with defaults*/
            if(args.length == 0)  arguments = new RuntimeArguments()
            else arguments =  ArgumentParser.parseArguments(args(0))
            logger.info(s"arguments resolved for this job run are ${arguments}")
            
            
            /** initializing the spark context with given master url*/
            val sparkConf = new SparkConf(true).setAppName("AirportDepartureLoad")
                                               .setMaster(arguments.masterURL)
                                               .set("spark.cassandra.connection.host", arguments.dseConnectionHost)
            
            val sc = new SparkContext(sparkConf)
            
            val flightsRDD:RDD[CassandraRow] = sc.cassandraTable(arguments.keySpaceName, arguments.flightTableName)
                                                 .select( "id",
                                                          "dep_time",
                                                          "origin",
                                                          "airline_id",
                                                          "carrier",
                                                          "fl_num",
                                                          "origin_city_name",
                                                          "origin_state_abr",
                                                          "dest",
                                                          "dest_city_name",
                                                          "dest_state_abr",
                                                          "distance",
                                                          "air_time")
                                                          
                                                          

            
            flightsRDD.foreach(println)
            
            /*
            flightsRDD.saveToCassandra(arguments.keySpaceName, "airport_departures", SomeColumns( "id",
                                                                                                  "dep_time",
                                                                                                  "origin",
                                                                                                  "airline_id",
                                                                                                  "carrier",
                                                                                                  "fl_num",
                                                                                                  "origin_city_name",
                                                                                                  "origin_state_abr",
                                                                                                  "dest",
                                                                                                  "dest_city_name",
                                                                                                  "dest_state_abr",
                                                                                                  "distance"))
                 */                                                                                 
           def cassandraRowToMap( cr: CassandraRow) = { cr.toMap}                                                                                        
                                                                                                  
           val flightsRDD1:RDD[CassandraRow] =  flightsRDD.map(row => CassandraRow.fromMap(cassandraRowToMap(row) + ("arr_time_bucket" -> math.ceil(row.getInt("air_time")/10.0).toInt*10)))
   
        		            
           flightsRDD1.foreach(println)
           /* val flightRecordsDS = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                                   .options(Map(  "keyspace" -> arguments.keySpaceName, 
                                                                  "table" -> arguments.flightTableName ))
                                                   .load()
                                                   .filter($"id" === 10)
                                                   
            flightRecordsDS.show()*/
        }
    }
  
}