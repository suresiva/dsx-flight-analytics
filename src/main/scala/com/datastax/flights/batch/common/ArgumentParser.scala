package com.datastax.flights.batch.common

import java.util.Properties
import org.apache.log4j.Logger
import java.io.FileInputStream
import scala.collection.JavaConverters._

/**
 * @author sureshsivva 
 * 
 * This object is used to parse the command line arguments
 * and to populate the case class for application usage 
 */
object ArgumentParser {
  
  val logger = Logger.getLogger("ArgumentParser")
  
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