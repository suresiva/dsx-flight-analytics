package com.datastax.flights.batch.dataload

case class RuntimeArguments ( masterURL:String = "local[*]",
                              inputFilePath:String = "analysis/inputs/",
                              dseConnectionHost:String = "localhost",
                              keySpaceName:String = "test",
                              flightTableName:String = "flights")