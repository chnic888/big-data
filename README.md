# Hadoop
Code snippet of **_Hadoop: The Definitive Guide, 4th Edition_**

## Requirements:
``` bash
* Java: 8
* Hadoop: 3.2.1
* Hive 3.1.2
* Spark: 3.0.0-preview2
```

## Steps
### Running m/r samples locally
```bash
#build jar file
./gradlew clean build

#export config file dir
export HADOOP_CONF_DIR=./src/main/resources/local

#run m/r
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCMaxTemperature './src/test/resources/190*.gz' ./out
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCMaxTemperatureCompression ./src/test/resources/1901 ./out
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCFileConverter ./src/test/resources/1901 ./out
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCMaxTemperatureSortedByMapper './src/test/resources/190*.gz' ./out
```

### Avro
```bash
#generate java file from avsc 
./gradlew clean generateAvroJava

#run avro test case only 
./gradlew clean test --tests com.chnic.avro.*
```

### Running Avro m/r sample locally
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
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCAvroMaxTemperature -libjars ${LIBJARS} './src/test/resources/190*.gz' ./out
```

### Running Parquet sample locally
```bash
./gradlew clean test --tests com.chnic.parquet.*
```

### Running Spark sample locally
```bash
#build jar file
./gradlew clean build

#run spark
spark-submit --class com.chnic.spark.NCDCMaxTemperature --master local ./build/libs/ncdc-1.0.jar ./src/test/resources/1901 ./out
```

