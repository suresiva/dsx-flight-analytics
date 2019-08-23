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

object FlightsInputLoader {


	def main(args:Array[String]){
	  
	  
	//TODO to create the tables if not exist automatically through  C* session init function
	  
  val log = Logger.getLogger("FlightsInputLoader")
  
	  //  sparkConf
    //.setAppName("Write performance test")
    //.set("spark.cassandra.output.concurrent.writes", "16")
    //.set("spark.cassandra.output.batch.size.bytes", "4096")
    //.set("spark.cassandra.output.batch.grouping.key", "partition")
	  //spark.conf.set("spark.sql.shuffle.partitions", 6)
    //spark.conf.set("spark.executor.memory", "2g")
    
  
  
		val sparkSession = SparkSession.builder().appName("FlightsInputLoader")
		                                          //.master("local[*]")
		                                          .master("dse://10.128.15.195:9042?connection.local_dc=Analytics;connection.host=;")
		                                          .getOrCreate()

    import sparkSession.implicits._
		                                  
    val airportsBaseDF = sparkSession.read.option("delimiter",",")
                                          .option("header", "true")
                                          .option("inferSchema", "true")
                                          //.csv("src/main/resources/usa_airports_details.csv")
                                          .csv("flight-exercise/input/files/usa_airports_details.csv")
                                          .filter(col("IATA").isNotNull)
                                          
                                          
    val flightSchema = StructType(Array(
                                        StructField("ID", IntegerType, true),
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
                                        
                                        
    val flightsBaseDF = sparkSession.read.option("delimiter",",").option("header", "false")
                                                                 .option("dateFormat", "yyyy/MM/dd")
                                                                 .option("inferSchema", "false")
                                                                 .schema(flightSchema)
                                                                 //.csv("src/main/resources/flights_from_pg.csv")
                                                                 .csv("flight-exercise/input/files/flights_from_pg.csv")
    
    val timeStampSrcFormat = new SimpleDateFormat("yyyy-MM-dd HHmm")
  
    val timeStampUTCFormat = new SimpleDateFormat("yyyy-MM-dd HHmm")
  
    timeStampUTCFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    
    def timestampUtcFunc(dateStr:String, timeStr:String, timezoneCode:String) : Long = { 
                                                                                          if(dateStr == null || timeStr == null || timezoneCode == null) 
                                                                                              return 0; 
                                                                                          timeStampSrcFormat.setTimeZone(TimeZone.getTimeZone(timezoneCode)); 
                                                                                          timeStampUTCFormat.parse( 
                                                                                                      timeStampUTCFormat.format( 
                                                                                                            timeStampSrcFormat.parse(
                                                                                                                dateStr.concat(" ").concat(timeStr)) )).getTime() 
                                                                                        } 

    val timestampUtcUDF = udf[Long,String,String,String](timestampUtcFunc)

    val flightsTzTmpDF = flightsBaseDF.alias("t1")
                                      .join(broadcast(airportsBaseDF.alias("t2")), $"t1.ORIGIN" === $"t2.IATA", "left_outer")
                                      .select(  $"t1.*", 
                                                $"t2.TIMEZONE_CODE" as "ORIGIN_TIMEZONE_CODE", 
                                                timestampUtcUDF($"t1.FL_DATE", lpad($"t1.DEP_TIME",4,"0"), $"t2.TIMEZONE_CODE") as "DEP_TIME_UTC_MS")
                                                
                                                
                                                
    val flightsTzMappedDF = flightsTzTmpDF.alias("t1")
                                          .join(broadcast(airportsBaseDF.alias("t2")), $"t1.DEST" === $"t2.IATA", "left_outer")
                                          .select(  $"t1.*", 
                                                    $"t2.TIMEZONE_CODE" as "DEST_TIMEZONE_CODE", 
                                                    timestampUtcUDF($"t1.FL_DATE", lpad($"t1.ARR_TIME",4,"0"), $"t2.TIMEZONE_CODE") as "ARR_TIME_UTC_MS_TMP")
                                            

    val timestampFixUDF = udf( (depTimsMS:Long, arrTimeMS:Long, originTzCode:String, destTzCode:String, deptTime:Integer, arrTime:Integer) => { 
      
                                                                  if(originTzCode == "Pacific/Guam" && destTzCode == "Pacific/Honolulu"){ 
                                                                      if(deptTime > 1600 && arrTime < 600)  
                                                                          arrTimeMS 
                                                                      else 
                                                                          arrTimeMS-86400000
                                                                          
                                                                  }else if(arrTimeMS < depTimsMS) 
                                                                        arrTimeMS+86400000 
                                                                   else 
                                                                        arrTimeMS 
                                                                } )
                                                                
    val flightsArrTimeFixDF = flightsTzMappedDF.withColumn("ARR_TIME_UTC_MS", timestampFixUDF(  $"DEP_TIME_UTC_MS", 
                                                                                                $"ARR_TIME_UTC_MS_TMP", 
                                                                                                $"ORIGIN_TIMEZONE_CODE", 
                                                                                                $"DEST_TIMEZONE_CODE", 
                                                                                                $"DEP_TIME", 
                                                                                                $"ARR_TIME"))
                                              .drop($"ARR_TIME_UTC_MS_TMP")

    flightsArrTimeFixDF.show()
    
    log.debug("XXX"+flightsArrTimeFixDF.count())
    
    flightsArrTimeFixDF.printSchema()
    
    val flightToCDF = flightsArrTimeFixDF.select( col("ID") as "id",
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
    
    flightToCDF.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "flights1","keyspace" -> "test")).save()
    
    
    //val someDF = Seq( ("1", "bat"),   ("2", "mouse"),   ("3", "horse")).toDF("key", "value")

    //someDF.printSchema()
    
    
    //someDF.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "kv","keyspace" -> "test")).save()
    
    

    //val airportsDF = airportsBaseDF
		                                  
	}



}