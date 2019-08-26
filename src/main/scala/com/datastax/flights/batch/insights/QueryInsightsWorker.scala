package com.datastax.flights.batch.insights

import org.apache.log4j.Logger
import com.datastax.flights.batch.common.RuntimeArguments
import com.datastax.flights.batch.common.ArgumentParser
import org.apache.spark.sql.SparkSession
import com.datastax.flights.batch.common.CqlDmlExecutor
import com.datastax.flights.batch.common.CqlDmlExecutor

/**
 * @author sureshsivva 
 * 
 * This object is used to execute the queries against the cassandra tables 
 * prepared. This program executes the steps to extract the results for the
 * questions given in the task list.
 */
object QueryInsightsWorker {
  
    val logger = Logger.getLogger("FlightsInputLoader")
    var arguments:RuntimeArguments = null
    val cqlWorker:CqlDmlExecutor = new CqlDmlExecutor()
    
   /**
    * below method is to prepare runtime environment and to prepare 
    * the the results for the query list
    */
    def main(args:Array[String]){
      
        /** parsing the runtime arguments from properties file or with defaults*/
        if(args.length == 0)  arguments = new RuntimeArguments()
        else arguments =  ArgumentParser.parseArguments(args(0))
        logger.info(s"arguments resolved for this job run are ${arguments}")   
        
        /** initializing the spark session with given master url*/
    		val sparkSession = SparkSession.builder().appName("FlightsInputLoader")
    		                                          .master(arguments.masterURL)
    		                                          .config("spark.cassandra.connection.host", arguments.dseConnectionHost)
    		                                          .getOrCreate()    
        import sparkSession.implicits._
        logger.info("initialized spark session...")
        
        queryQuestion4Results(sparkSession, "", "", arguments)
        
    }
  
    /** method to extract query 1 results
     */
    def queryQuestion1Results(sparkSession:SparkSession, arguments:RuntimeArguments) = {
      
        import sparkSession.sqlContext.implicits._
      
        val count = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                     .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "airport_departures" ))
                                     .load()
                                     .select( $"id" )
                                     .filter( $"origin" ===  "HNL" && 
                                              $"dep_time" >= "2012-01-25 00:00:00.000000+0000" && 
                                              $"dep_time" <= "2012-01-25 23:59:59.000000+0000")
                                     .count()
        logger.info( "Query 1: how many flights orginated from the HNL airport code on 2012-01-25 ?")                             
        logger.info(s"Query 1: result = ${count}")
    }
    

    /** method to extract query 2 results
     */
    def queryQuestion2Results(sparkSession:SparkSession, arguments:RuntimeArguments) = {
      
        import sparkSession.sqlContext.implicits._
      
        val count = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                     .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "airport_departures" ))
                                     .load()
                                     .select( $"origin")
                                     .filter( $"origin".startsWith("A"))
                                     .distinct()
                                     .count()
        logger.info( "Query 2: how airport codes start with the letter 'A'?")                             
        logger.info(s"Query 2: result = ${count}")
        
    } 
    
    
    /** method to extract query 3 results
     */
    def queryQuestion3Results(sparkSession:SparkSession, arguments:RuntimeArguments) = {
      
        import sparkSession.sqlContext.implicits._
        import org.apache.spark.sql.functions._
      
        val apMaxFlights = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                            .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "airport_departures" ))
                                            .load()
                                            .select( $"origin", $"id" )
                                            .filter( $"dep_time" >= ("2012-01-23 00:00:00.000000+0000") && 
                                                     $"dep_time" <= ("2012-01-23 23:59:59.000000+0000"))
                                            .groupBy($"origin")
                                            .agg(count($"id").as("flights_count"))
                                            .orderBy($"flights_count".desc)
                                            .first()
                                 
        logger.info( "Query 3: What originating airport had most flights on 2012-01-23?")                             
        logger.info(s"Query 3: result = ${apMaxFlights(0).toString()}")
        
    }
    
    
    /** method to extract query 4 results
     */
    def queryQuestion4Results(sparkSession:SparkSession, srcAirportCode:String, targetAirportCode:String, arguments:RuntimeArguments) = {
      
        import sparkSession.sqlContext.implicits._
        import org.apache.spark.sql.functions._
        
        logger.info( s"Query 4: make a batch update to all records with '${srcAirportCode}' airport code using spark and change it to '${targetAirportCode}'")

        /** updating the origin records of flights table*/
        val flightsOrgnMatchDF = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                              .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> arguments.flightTableName ))
                                              .load()
                                              .select($"id")
                                              .filter($"origin" === srcAirportCode)
                                              .withColumn("origin", lit(targetAirportCode))
        logger.info(s"Query 4: result 1: ${flightsOrgnMatchDF.count()} rows to update") 
                                              
        flightsOrgnMatchDF.write.format("org.apache.spark.sql.cassandra")
                               .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> arguments.flightTableName ))
                               .mode(org.apache.spark.sql.SaveMode.Append)
                               .save()
                                  
        logger.info(s"Query 4: result 1: updated the origin entries of flights table from ${srcAirportCode} -> ${targetAirportCode}")
        
        /** updating the dest records of flights table*/
        val flightsDestMatchDF = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                              .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> arguments.flightTableName ))
                                              .load()
                                              .select($"id")
                                              .filter($"dest" === srcAirportCode)
                                              .withColumn("dest", lit(targetAirportCode))
        logger.info(s"Query 4: result 2: ${flightsDestMatchDF.count()} rows to update")    
        
        flightsDestMatchDF.write.format("org.apache.spark.sql.cassandra")
                               .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> arguments.flightTableName ))
                               .mode(org.apache.spark.sql.SaveMode.Append)
                               .save()
                                  
        logger.info(s"Query 4: result 2: updated the dest entries of flights table from ${srcAirportCode} -> ${targetAirportCode}")        
        
        /** updating the origin records of flights_airtime table*/
        val flAirTimeOrgnMatchDF = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                                  .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "flights_airtime" ))
                                                  .load()
                                                  .select( $"fl_num", 
                                                           $"air_time_bucket", 
                                                           $"id")
                                                  .filter($"origin" === srcAirportCode)
                                                  .withColumn("origin", lit(targetAirportCode))
        logger.info(s"Query 4: result 3: ${flAirTimeOrgnMatchDF.count()} rows to update")                                               
        
        flAirTimeOrgnMatchDF.write.format("org.apache.spark.sql.cassandra")
                               .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "flights_airtime" ))
                               .mode(org.apache.spark.sql.SaveMode.Append)
                               .save()   
                               
        logger.info(s"Query 4: result 3: updated the origin entries of flights_airtime table from ${srcAirportCode} -> ${targetAirportCode}")   
        
        /** updating the dest records of flights_airtime table*/
        val flAirTimeDestMatchDF = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                                  .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "flights_airtime" ))
                                                  .load()
                                                  .select( $"fl_num", 
                                                           $"air_time_bucket", 
                                                           $"id")
                                                  .filter($"dest" === srcAirportCode)
                                                  .withColumn("dest", lit(targetAirportCode))
        logger.info(s"Query 4: result 4: ${flAirTimeDestMatchDF.count()} rows to update")                                          
                                                  
        flAirTimeDestMatchDF.write.format("org.apache.spark.sql.cassandra")
                               .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "flights_airtime" ))
                               .mode(org.apache.spark.sql.SaveMode.Append)
                               .save()   
                               
        logger.info(s"Query 4: result 4: updated the dest entries of flights_airtime table from ${srcAirportCode} -> ${targetAirportCode}")   
        
        /** updating the origin records of airport_departures table*/
        val airDeptOrgnMatchDF = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                                  .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "airport_departures" ))
                                                  .load()
                                                  .filter($"origin" === srcAirportCode)
                                                  .drop($"origin")
                                                  .withColumn("origin", lit(targetAirportCode))
        logger.info(s"Query 4: result 5: ${airDeptOrgnMatchDF.count()} rows to update")
        
        println(airDeptOrgnMatchDF.count())
        airDeptOrgnMatchDF.write.format("org.apache.spark.sql.cassandra")
                               .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "airport_departures" ))
                               .mode(org.apache.spark.sql.SaveMode.Append)
                               .save()                     
                               
        cqlWorker.dropFlightAirtimeRows(sparkSession.sparkContext, srcAirportCode, arguments)
        logger.info(s"droppped the ${srcAirportCode} orgin airports from flights airtime table.")
                               
        logger.info(s"Query 4: result 5: updated the origin entries of airport_departures table from ${srcAirportCode} -> ${targetAirportCode}")     
        
        /** updating the dest records of airport_departures table*/
        val airDeptDestMatchDF = sparkSession.read.format("org.apache.spark.sql.cassandra")
                                                  .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "airport_departures" ))
                                                  .load()
                                                  .select( $"origin", 
                                                           $"dep_time", 
                                                           $"id")
                                                  .filter($"dest" === srcAirportCode)
                                                  .withColumn("dest", lit(targetAirportCode))
        logger.info(s"Query 4: result 6: ${airDeptDestMatchDF.count()} rows to update")
        
        airDeptDestMatchDF.write.format("org.apache.spark.sql.cassandra")
                               .options(Map( "keyspace" -> arguments.keySpaceName, "table" -> "airport_departures" ))
                               .mode(org.apache.spark.sql.SaveMode.Append)
                               .save()   
                               
        logger.info(s"Query 4: result 6: updated the dest entries of airport_departures table from ${srcAirportCode} -> ${targetAirportCode}")          
        
    }    
}