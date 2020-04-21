## Hadoop
Code snippet of **_Hadoop: The Definitive Guide, 4th Edition_**

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

#### Running Avro m/r sample locally
```bash
#build jar file
./gradlew clean build

#export LIBJARS
export LIBJARS=./build/libs/avro-mapred-1.7.7-hadoop2.jar

#export HADOOP_CLASSPATH
export HADOOP_CLASSPATH=./build/libs/avro-mapred-1.7.7-hadoop2.jar

#run m/r
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCAvroMaxTemperature -libjars ${LIBJARS} './src/test/resources/190*.gz' ./out
```