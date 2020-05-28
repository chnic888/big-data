# Big Data
Hadoop/Hive/Spark Code Samples 

## Requirements
``` bash
* Java: 8
* Hadoop: 3.2.1
* Hive 3.1.2
* Spark: 3.0.0-preview2
```

## Running Steps
### Running m/r samples locally
```bash
#build jar file
./gradlew clean build

#export config file dir
export HADOOP_CONF_DIR=./src/main/resources/local

#run m/r
hadoop jar ./build/libs/big-data-1.0.jar com.chnic.mapreduce.NCDCMaxTemperature './src/test/resources/190*.gz' ./out
hadoop jar ./build/libs/big-data-1.0.jar com.chnic.mapreduce.NCDCMaxTemperatureCompression ./src/test/resources/1901 ./out
hadoop jar ./build/libs/big-data-1.0.jar com.chnic.mapreduce.NCDCFileConverter ./src/test/resources/1901 ./out
hadoop jar ./build/libs/big-data-1.0.jar com.chnic.mapreduce.NCDCMaxTemperatureSortedByMapper './src/test/resources/190*.gz' ./out
```

### Running Avro samples locally
```bash
#generate java file from avsc 
./gradlew clean generateAvroJava

#run avro test case only 
./gradlew clean test --tests com.chnic.avro.*
```

### Running Avro m/r samples locally
```bash
#build jar file
./gradlew clean build

#export config file dir
export HADOOP_CONF_DIR=./src/main/resources/local

#export LIBJARS
export LIBJARS=./build/libs/avro-mapred-1.7.7-hadoop2.jar

#export HADOOP_CLASSPATH
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

#run m/r
hadoop jar ./build/libs/big-data-1.0.jar com.chnic.mapreduce.NCDCAvroMaxTemperature -libjars ${LIBJARS} './src/test/resources/190*.gz' ./out
```

### Running Parquet samples locally
```bash
./gradlew clean test --tests com.chnic.parquet.*
```

### Running Spark samples locally
```bash
#build jar file
./gradlew clean build

#run spark RDD API sample
spark-submit --class com.chnic.spark.NCDCMaxTemperature --master local ./build/libs/big-data-1.0.jar ./src/test/resources/1901 ./out

#run spark Structured API sample
spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.0-preview2 --class com.chnic.spark.FlightDataStructuredQuery --master local[*] ./build/libs/big-data-1.0.jar ./src/test/resources/2015-summary.json ./out
spark-submit --class com.chnic.spark.RetailDataStructuredQuery --master local[*] ./build/libs/big-data-1.0.jar ./src/test/resources/2011-12-09.csv ./out
spark-submit --class com.chnic.spark.RetailDataAggregationQuery --master local[*] ./build/libs/big-data-1.0.jar './src/test/resources/2011-12-*.csv' ./out
spark-submit --class com.chnic.spark.PersonJoinQuery --master local[*] ./build/libs/big-data-1.0.jar ./src/test/resources/ ./out

#run spark Structured API sample to read/write jdbc
spark-submit --class com.chnic.spark.EmployeeDBQuery --master local[*] --driver-class-path ./build/libs/mysql-connector-java-8.0.20.jar --jars ./build/libs/mysql-connector-java-8.0.20.jar ./build/libs/big-data-1.0.jar 'jdbc:mysql://localhost:3306/test' root root
```

### Running Spark SQL sample locally
```bash
#build jar file
./gradlew clean build

#run spark sql samples
spark-submit --class com.chnic.spark.EmployeeSQLQuery --master local[*] ./build/libs/big-data-1.0.jar ./src/test/resources/employees.csv 'select * from employee where id = 10' ./out
spark-submit --class com.chnic.spark.EmployeeSQLQuery --master local[*] ./build/libs/big-data-1.0.jar ./src/test/resources/employees.csv 'select title, count(*) as count from employee group by title' ./out
spark-submit --class com.chnic.spark.FlightDataSQLQuery --master local[*] ./build/libs/big-data-1.0.jar ./src/test/resources/2015-summary.json
```

### Running Spark Streaming sample locally
```bash
#build jar file
./gradlew clean build

#run spark streaming samples
spark-submit --class com.chnic.spark.SparkStreamingQuery --master local[*] ./build/libs/big-data-1.0.jar ./src/test/resources/streaming-data/
```