package com.datastax.flights.batch.dataload

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext._
import org.apache.log4j._
import scala.collection.Seq
import org.apache.spark.sql.cassandra._
import scala.collection.Map
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.util.Properties
import java.util.Properties
import java.io.FileInputStream
import java.util.Map.Entry
import scala.collection.JavaConverters._
import java.util.Calendar
import java.util.Date
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.flights.batch.common._

  	  //  sparkConf
    //.setAppName("Write performance test")
    //.set("spark.cassandra.output.concurrent.writes", "16")
    //.set("spark.cassandra.output.batch.size.bytes", "4096")
    //.set("spark.cassandra.output.batch.grouping.key", "partition")
	  //spark.conf.set("spark.sql.shuffle.partitions", 6)
    //spark.conf.set("spark.executor.memory", "2g")
	//TODO to create the tables if not exist automatically through  C* session init function 

/**
 * @author sureshsivva 
 * 
 * This object is used to parse the given flights dataset
 * and to transform it to a clean set with clean data types,
 * all the fields resolved to the expected format.
 * Also to persist the final formatted records into Cassandra flights table 
 */
object FlightsInputLoader {
 
  val logger = Logger.getLogger("FlightsInputLoader")
  var arguments:RuntimeArguments = null
  val timestampFormat:String = "yyyy-MM-dd HHmm"
  val cqlExecutor:CqlDmlExecutor = new CqlDmlExecutor()
  
  val flightInputSchema = StructType(Array( StructField("ID", IntegerType, true),
                                            StructField("YEAR", IntegerType, true),
                                            StructField("DAY_OF_MONTH", IntegerType, true),
                                            StructField("FL_DATE", DateType, true),
                                            StructField("AIRLINE_ID", IntegerType, true),
                                            StructField("CARRIER", StringType, true),
                                            StructField("FL_NUM", IntegerType, true),
                                            StructField("ORIGIN_AIRPORT_ID", IntegerType, true),
                                            StructField("ORIGIN", StringType, true),
                                            StructField("ORIGIN_CITY_NAME", StringType, true),
                                            StructField("ORIGIN_STATE_ABR", StringType, true),
                                            StructField("DEST", StringType, true),
                                            StructField("DEST_CITY_NAME", StringType, true),
                                            StructField("DEST_STATE_ABR", StringType, true),
                                            StructField("DEP_TIME", StringType, true),
                                            StructField("ARR_TIME", StringType, true),
                                            StructField("ACTUAL_ELAPSED_TIME", IntegerType, true),
                                            StructField("AIR_TIME", IntegerType, true),
                                            StructField("DISTANCE", IntegerType, true)))

 /**
  * below method is to prepare runtime environment and to prepare 
  * the dataset and to persist to cassandra eventually
  */
  def main(args:Array[String]){
    
    logger.info("batch load has started at "+ new Date())

    try{
        
        /** parsing the runtime arguments from properties file or with defaults*/
        if(args.length == 0)  arguments = new RuntimeArguments()
        else arguments =  parseArguments(args(0))
        logger.info(s"arguments resolved for this job run are ${arguments}")
        
        
        /** initializing the spark session with given master url*/
    		val sparkSession = SparkSession.builder().appName("FlightsInputLoader")
    		                                          .master(arguments.masterURL)
    		                                          .config("spark.cassandra.connection.host", arguments.dseConnectionHost)
    		                                          .getOrCreate()
    		                                          
        import sparkSession.implicits._
        logger.info("initialized spark session...")
        
        
        /** creating the required cassandra keyspace and table as needed*/
        cqlExecutor.createKeyspace(sparkSession.sparkContext)
        cqlExecutor.createFlightsTable(sparkSession.sparkContext)
        
         return
        /** loading the airports dataset as dataframe, rejecting rows with IATA as null*/
        val airportsDF = sparkSession.read.option("delimiter",",")
                                              .option("header", "true")
                                              .option("inferSchema", "true")
                                              .csv(s"${arguments.inputFilePath}/usa_airports_details.csv")
                                              .filter(col("IATA").isNotNull)
                                              
        logger.info(s"loaded the aiports dataset with ${airportsDF.count()} rows")
        
        
        /** loading the given flights dataset with the possible immediate schema as dataframe*/
        val flightsBaseDF = sparkSession.read.option("delimiter",",")
                                             .option("header", "false")
                                             .option("dateFormat", "yyyy/MM/dd")
                                             .option("inferSchema", "false")
                                             .schema(flightInputSchema)
                                             .csv(s"${arguments.inputFilePath}/flights_from_pg.csv")
                                             
        logger.info(s"loaded the flights dataset with ${flightsBaseDF.count()} rows")
        
    
        /** initializing timestamp parsers, UTC timezone converter UDF*/
        val timestampUtcUDF = udf[Long,String,String,String](timestampUtcFunc)
    
        
        /** joining the flights data with airports to resolve timezone for origin airports,
         *  and also to convert the dep_time to UTC format by associating the time origin timezone,
         *  origin_timezone_code = orgin airport's timezone code
         *  dep_time_utc_ms = flight departure time in UTC milliseconds*/ 
        
        val flightsTzTmpDF = flightsBaseDF.alias("t1")
                                          .join(    broadcast(airportsDF.alias("t2")), 
                                                    $"t1.ORIGIN" === $"t2.IATA", "left_outer")
                                          .select(  $"t1.*", 
                                                    $"t2.TIMEZONE_CODE" as "ORIGIN_TIMEZONE_CODE", 
                                                    timestampUtcUDF($"t1.FL_DATE", lpad($"t1.DEP_TIME",4,"0"), $"t2.TIMEZONE_CODE") as "DEP_TIME_UTC_MS")
                                                    
                                                    
        /** joining the flights data with airports to resolve timezone for dest airports,
         *  and also to convert the arr_time to UTC format by associating the time dest timezone,
         *  dest_timezone_code = destination airport's timezone code
         *  arr_time_utc_ms_tmp = flight departure time in UTC milliseconds, yet to fix the date*/ 
                                                    
        val flightsTzMappedDF = flightsTzTmpDF.alias("t1")
                                              .join(  broadcast(airportsDF.alias("t2")), 
                                                      $"t1.DEST" === $"t2.IATA", "left_outer")
                                              .select(  $"t1.*", 
                                                        $"t2.TIMEZONE_CODE" as "DEST_TIMEZONE_CODE", 
                                                        timestampUtcUDF($"t1.FL_DATE", lpad($"t1.ARR_TIME",4,"0"), $"t2.TIMEZONE_CODE") as "ARR_TIME_UTC_MS_TMP")
                                                        
        logger.info("successfully mapped the origin & dest airports timezone...\n" +
                    "converted the dep_time and arr_time values into UTC milliseconds...\n"+
                    s"total rows mapped at the recent dataframe = ${flightsTzMappedDF.count()}")
        
    
        /**
         * created an UDF to adjust the arr_time according to the fl_date, actual_elapsed_time and 
         * difference between the arr_time and dep_time.
         */
        val timestampFixUDF = udf[Long, Long, Long, String, String, Integer, Integer](arrTimestampFixUDF)
                                                                    
        val flightsArrTimeFixDF = flightsTzMappedDF.withColumn("ARR_TIME_UTC_MS", 
                                                                   timestampFixUDF( $"DEP_TIME_UTC_MS", 
                                                                                    $"ARR_TIME_UTC_MS_TMP", 
                                                                                    $"ORIGIN_TIMEZONE_CODE", 
                                                                                    $"DEST_TIMEZONE_CODE", 
                                                                                    $"DEP_TIME", 
                                                                                    $"ARR_TIME"))
                                                  .drop($"ARR_TIME_UTC_MS_TMP")
    
        logger.info(s"corrected the arrival time fields. records in recent dataframe=${flightsArrTimeFixDF.count()}\n" +
                     "final dataframe schema is as below,")    
                     
        flightsArrTimeFixDF.printSchema()
    
        
        /** preparing the final dataframe to persist in given cassandra table*/
        val flightToTableDF = flightsArrTimeFixDF.select( col("ID") as "id",
                                                      col("YEAR") as "year",
                                                      col("DAY_OF_MONTH") as "day_of_month",
                                                      col("FL_DATE") as "fl_date",
                                                      col("AIRLINE_ID") as "airline_id",
                                                      col("CARRIER") as "carrier",
                                                      col("FL_NUM") as "fl_num",
                                                      col("ORIGIN_AIRPORT_ID") as "origin_airport_id",
                                                      col("ORIGIN") as "origin",
                                                      col("ORIGIN_CITY_NAME") as "origin_city_name",
                                                      col("ORIGIN_STATE_ABR") as "origin_state_abr",
                                                      col("DEST") as "dest",
                                                      col("DEST_CITY_NAME") as "dest_city_name",
                                                      col("DEST_STATE_ABR") as "dest_state_abr",
                                                      col("DEP_TIME_UTC_MS") as "dep_time",
                                                      col("ARR_TIME_UTC_MS") as "arr_time",
                                                      col("ACTUAL_ELAPSED_TIME") as "actual_elapsed_time",
                                                      col("AIR_TIME") as "air_time",
                                                      col("DISTANCE") as "distance")
        
        /** persisting the dataframe into cassandra using cassandra-spark connector*/
        logger.debug("persisting the final dataframe prepared into cassandra table...")                         
        
        flightToTableDF.write.format("org.apache.spark.sql.cassandra")
                             .options( Map( "keyspace" -> arguments.keySpaceName, 
                                            "table"    -> arguments.flightTableName))
                             .save()
    
        logger.info(s"persisted the dataframe into ${arguments.keySpaceName}.${arguments.flightTableName}.")
        logger.info(s"loaded ${flightToTableDF} rows. completed execution.")
        
    } catch {
      case e : Throwable => { logger.error("loading flight data to cassandra has failed due to $e")
                              e.printStackTrace()}
    }
  }
    
  
  /** method to frame the timestamp string, to parse the local timestamp
   *  and to return the converted timestamp in UTC timezone
   */
  def timestampUtcFunc(dateStr:String, timeStr:String, timezoneCode:String) : Long = { 
    
    try{
      
        val timeStampSrcFormat = new SimpleDateFormat(timestampFormat)
        val timeStampUTCFormat = new SimpleDateFormat(timestampFormat)
        timeStampUTCFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
                                                                                          
        if(dateStr == null || timeStr == null || timezoneCode == null) 
            return 0;
        
        timeStampSrcFormat.setTimeZone(TimeZone.getTimeZone(timezoneCode));
        
        return timeStampUTCFormat.parse( 
                                      timeStampUTCFormat.format( 
                                          timeStampSrcFormat.parse(
                                              dateStr.concat(" ").concat(timeStr)) ))
                                 .getTime()
                                 
      } catch {
          case e : Throwable => throw new Exception(s"parsing dates and utc conversion has failed due to $e")
      }
  }     
  
  
  /**
   * method to resolve the arr_time to the correct date, since the dataset doesn't have 
   * actual arrival date, it can be resolved with with available fl_date and arr_time.
   * Also the Guam to Honolulu flights needed extra handling asthe it has +20hr difference
   */
  def arrTimestampFixUDF(depTimsMS:Long, arrTimeMS:Long, originTzCode:String, destTzCode:String, deptTime:Integer, arrTime:Integer):Long = { 
      
      if(originTzCode == "Pacific/Guam" && destTzCode == "Pacific/Honolulu"){
        
          if(deptTime > 1600 && arrTime < 600) arrTimeMS
          
          else arrTimeMS-86400000
              
      }else if(arrTimeMS < depTimsMS) arrTimeMS+86400000
      
       else arrTimeMS

   }
  
  
  /**
   * method to parse the application arguments from
   * given properties file and return as case class 
   */
  def parseArguments(filePath:String): RuntimeArguments = {
      
      var fileProperties:Properties = new Properties()
      
      try{
        
          logger.debug(s"parsing the $filePath to read application properties")
          
          fileProperties.load(new FileInputStream(filePath))
          
          val argProperties = fileProperties.asScala.toMap
            
          val sparkMasterURL:String = argProperties.get("spark_master_url").get
          
          val inputFilePath:String = argProperties.get("input_file_path").get
          
          val dseConnectionHost:String = argProperties.get("dse_connection_host").get
          
          val keySpaceName:String = argProperties.get("model_keyspace_name").get
          
          val flightTableName:String = argProperties.get("model_flight_table").get

          return new RuntimeArguments(sparkMasterURL, inputFilePath, dseConnectionHost, keySpaceName, flightTableName)
          
      } catch {
          case e : Throwable => throw new Exception(s"parsing given input properties file $filePath was failed due to $e")
      }
  }

}