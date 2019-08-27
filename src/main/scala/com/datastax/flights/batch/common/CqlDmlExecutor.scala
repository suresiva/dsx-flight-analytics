package com.datastax.flights.batch.common

import com.datastax.driver.core.Session
import org.apache.spark.SparkContext
import com.datastax.spark.connector.cql.CassandraConnector

/**
 * @author sureshsivva 
 * 
 * This class is used to  create the keyspace and all the required
 * tables if they were not existing. 
 */
class CqlDmlExecutor {

  /** method to create keyspace with given name*/
	def createKeyspace(context : SparkContext) = {
  	  try{

          CassandraConnector(context).withSessionDo{
                  session => session.execute(s"create keyspace if not exists dx_exercise "+
                                              "with replication = {'class' : 'NetworkTopologyStrategy', 'dsxdc1':2} ;")
          }
  	  } catch {
          case e : Throwable => throw new Exception(s"creating key space has failed due to $e")
      }
	}
	
	/** method to create flights table with given name*/
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
	
	/** method to create airport_departures table with given name and to create origin_index*/
	def createAirportDepartureTable(context : SparkContext) = {
  	  try{
  	    
          CassandraConnector(context).withSessionDo{
                  session => session.execute(s"""create table if not exists dx_exercise.airport_departures (
                                                          		origin text,
                                                              dep_time timestamp,        
                                                              id int,		
                                                          		airline_id int,
                                                          		carrier text,
                                                          		fl_num int,
                                                          		origin_city_name text,
                                                          		origin_state_abr text,
                                                          		dest text,
                                                          		dest_city_name text,
                                                          		dest_state_abr text,
                                                          		distance int,
                                                          		primary key(origin, dep_time, id))
                                                          		with clustering order by (dep_time asc, id asc);""")
          }
          
  	      /** the below approach causing the full table scan when I only use secondary indexed orgin
  	       *  and dep_time clustering column, here I am not using the partitioning key, so ignoring this.
  	       
            CassandraConnector(context).withSessionDo{
                    session => session.execute(s"""create table if not exists dx_exercise.airport_departures (
                                                                id int,
                                                                dep_time timestamp,        
  		                                                          origin text,
  		                                                          airline_id int,
  		                                                          carrier text,
  		                                                          fl_num int,
  		                                                          origin_city_name text,
  		                                                          origin_state_abr text,
  		                                                          dest text,
  		                                                          dest_city_name text,
  		                                                          dest_state_abr text,
  		                                                          distance int,
  		                                                          primary key(id, dep_time)) 
  		                                                          with clustering order by (dep_time asc);""")
            }
  	        CassandraConnector(context).withSessionDo{
                    session => session.execute(s"""create index if not exists origin_index ON dx_exercise.airport_departures (origin);""")
          }*/
  	  } catch {
          case e : Throwable => throw new Exception(s"creating airport departure table has failed due to $e")
      }
	}	
	
	/** method to create flights_airtime table with given name*/
	def createFlightsArrtimeTable(context : SparkContext) = {
  	  try{

          CassandraConnector(context).withSessionDo{
                  session => session.execute(s"""create table if not exists dx_exercise.flights_airtime (
                                                              fl_num int,
                                                              air_time_bucket int,
                                                  		        id int,
                                                  		        carrier text,		
                                                  		        origin text,
                                                  		        origin_city_name text,
                                                  		        dest text,
                                                  		        dest_city_name text,
                                                  		        primary key(fl_num, air_time_bucket, id)) 
                                                  		        with clustering order by (air_time_bucket asc, id asc);""")
          }
  	  } catch {
          case e : Throwable => throw new Exception(s"creating flights arrtime table has failed due to $e")
      }
	}	
	
	
	/** method to drop the given airport's rows from flight_airtime table*/
	def dropFlightAirtimeRows(context : SparkContext, airportCode:String, arguments:RuntimeArguments) = {
  	  try{

          CassandraConnector(context).withSessionDo{
                  
                  session => session.execute(s"""delete from ${arguments.keySpaceName}.airport_departures where origin='${airportCode}';""")
                  //println(s"""XXXX: delete from ${arguments.keySpaceName}.airport_departures where origin='${airportCode}';""")
                  //session => session.execute("delete from dx_exercise.airport_departures where origin='TST';")
          }
  	  } catch {
          case e : Throwable => throw new Exception(s"failed to drop the given airport rows from flights_airtime table due to $e")
      }
	}		
}