package com.datastax.flights.batch.common

/** 
 * @author sureshsivva 
 * 
 * case class to represent general variables used by multiple modules in this project
 */

case class RuntimeArguments ( masterURL:String = "local[*]",
                              inputFilePath:String = "analysis/inputs/",
                              dseConnectionHost:String = "localhost",
                              keySpaceName:String = "dx_exercise",
                              flightTableName:String = "flights")