## Hadoop
Code snippet of **_Hadoop: The Definitive Guide, 4th Edition_**

### Running m/r locally
```bash
#build jar file
./gradlew clean build

#export config file dir
export HADOOP_CONF_DIR=./src/main/resources/local

#run m/r
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCMaxTemperature './src/test/resources/190*.gz' ./out
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCMaxTemperatureCompression ./src/test/resources/1901 ./out
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapreduce.NCDCFileConverter ./src/test/resources/1901 ./out
```
