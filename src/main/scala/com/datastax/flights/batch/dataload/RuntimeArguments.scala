package com.datastax.flights.batch.dataload

case class RuntimeArguments ( masterURL:String = "local[*]",
                              inputFilePath:String = "analysis/inputs/")