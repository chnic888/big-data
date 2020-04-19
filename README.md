## Hadoop

### Running m/r under local mode
```bash
#build jar file
./gradlew clean build

#export config file dir
export HADOOP_CONF_DIR=./src/main/resources/local

#run m/r
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapred.NCDCMaxTemperature './src/test/resources/190*.gz' ./out
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapred.NCDCMaxTemperatureCompression ./src/test/resources/1901 ./out
hadoop jar ./build/libs/ncdc-1.0.jar com.chnic.mapred.NCDCFileConverter ./src/test/resources/1901 ./out
```