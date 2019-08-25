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
import com.datastax.flights.batch.common.CqlDmlExecutor

/**
 * @author sureshsivva 
 * 
 * This object is used to extract the flights data from cassandra
 * flights table and to persist it to the airport_depatures and 
 * flights_airtime cassandra table with required transformations.
 * Mostly used spark RDD structures to implement this, here I used
 * RDD instead of DataSet just for an experimental purpose. 
 */
object DownstreamTablesLoader {
  
    val logger = Logger.getLogger("DownstreamTablesLoader")
    var arguments:RuntimeArguments = null
    val cqlExecutor:CqlDmlExecutor = new CqlDmlExecutor()
    
    def main(args:Array[String]){
    
        logger.info("batch load has started at "+ new Date())
        
        try{
          
            /** parsing the runtime arguments from properties file or with defaults*/
            if(args.length == 0)  arguments = new RuntimeArguments()
            else arguments =  ArgumentParser.parseArguments(args(0))
            logger.info(s"arguments resolved for this job run are ${arguments}")
            
            
            /** initializing the spark context with given master url*/
            val sparkConf = new SparkConf(true).setAppName("DownstreamTablesLoader")
                                               .setMaster(arguments.masterURL)
                                               .set("spark.cassandra.connection.host", arguments.dseConnectionHost)
            
            val sc = new SparkContext(sparkConf)
            logger.info("initialized spark context...")
            
            /** to load the flight data from flights table using RDD cassandraTable*/
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
                                                          
            logger.info(s"loaded the flights data in RDD[CassandraRow], # rows = ${flightsRDD.count()}")     
            
            /** to persist to the airports_departure table with required columns*/
            writeAirportDepartures(sc, flightsRDD)
            logger.info("persisted the flights records to airport_departure table with required columns.")
                
            /** to persist to the flights_airtime table with required columns and air time buckets*/
            writeFlightsAirtime(sc, flightsRDD)
            logger.info("persisted the flights records to flights_airtime table with required columns.")   
            
        }catch {
          case e : Throwable => { logger.error("loading extracted data into airport_departures & flights_airtime tables have failed due to $e")
                                  e.printStackTrace()}
        }
    }
   
    
  /** method to prsist the flights RDD into aripirt_departure table
   *  using RDD saveToCassandra option.
   */
  def writeAirportDepartures(sc:SparkContext, flightsRDD : RDD[CassandraRow]) = {
    
    try{
          /** creating the airport_departures table as needed*/
          cqlExecutor.createAirportDepartureTable(sc)
          logger.info("created airport_departures table if it was not existed.")
          
          flightsRDD.saveToCassandra(  arguments.keySpaceName, 
                                       "airport_departures", 
                                       SomeColumns( "id",
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
    } catch {
          case e : Throwable => throw new Exception(s"failed to persist to the airports_departure table due to $e")
    }
  }
  
  
  /** method to persist the flights RDD with buckets based on air_time
   *  into aripirt_departure table using RDD saveToCassandra option.
   */
  def writeFlightsAirtime(sc:SparkContext, flightsRDD : RDD[CassandraRow]) = {
    
    try{
      
      /** creating the flights_airtime table as needed*/
      cqlExecutor.createFlightsArrtimeTable(sc)
      logger.info("created flights_airtime table if it was not existed.")      
    
      val flightsAirtimeRDD:RDD[CassandraRow] = flightsRDD.map(row => 
                                                                  CassandraRow.fromMap(  row.toMap + 
                                                                                        ("air_time_bucket" -> 
                                                                                         math.ceil(
                                                                                                   (row.getInt("air_time")/10.0))
                                                                                              .toInt*10)))
   
      flightsAirtimeRDD.saveToCassandra( arguments.keySpaceName, 
                                         "flights_airtime", 
                                         SomeColumns( "fl_num",
                                                      "air_time_bucket",
                                                      "id",
                                                      "carrier",
                                                      "origin",
                                                      "origin_city_name",
                                                      "dest",
                                                      "dest_city_name"))
    } catch {
          case e : Throwable => throw new Exception(s"failed to persist to the flights_airtime table due to $e")
    }
  }  
}