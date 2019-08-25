package com.datastax.flights.batch.common

import com.datastax.driver.core.Session
import org.apache.spark.SparkContext
import com.datastax.spark.connector.cql.CassandraConnector

class CqlDmlExecutor {

	def createKeyspace(context : SparkContext) = {
	  
  	  try{

          CassandraConnector(context).withSessionDo{
                  session => session.execute(s"create keyspace if not exists dx_exercise "+
                                              "with replication = {'class' : 'SimpleStrategy', 'replication_factor':1} ;")
          }
  	  } catch {
          case e : Throwable => throw new Exception(s"creating key space has failed due to $e")
      }
	}
	
	
	def createFlightsTable(context : SparkContext) = {
	  
  	  try{

          CassandraConnector(context).withSessionDo{
                  session => session.execute(s"""create table if not exists dx_exercise.flights (
                                                              id int primary key,
                                                              year int,        
                                                              day_of_month int,
                                                              fl_date timestamp,
                                                              airline_id int,
                                                              carrier varchar,
                                                              fl_num int,
                                                              origin_airport_id int,
                                                              origin varchar,
                                                              origin_city_name varchar,
                                                              origin_state_abr varchar,
                                                              dest varchar,
                                                              dest_city_name varchar,
                                                              dest_state_abr varchar,
                                                              dep_time timestamp,
                                                              arr_time timestamp,
                                                              actual_elapsed_time int,
                                                              air_time int,
                                                              distance int)""")
          }
  	  } catch {
          case e : Throwable => throw new Exception(s"creating flights table has failed due to $e")
      }
	}
}