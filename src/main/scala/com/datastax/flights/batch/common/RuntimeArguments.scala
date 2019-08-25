package com.datastax.flights.batch.common

case class RuntimeArguments ( masterURL:String = "local[*]",
                              inputFilePath:String = "analysis/inputs/",
                              dseConnectionHost:String = "localhost",
                              keySpaceName:String = "test1",
                              flightTableName:String = "flights1")