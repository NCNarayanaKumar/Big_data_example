
%declare PARAM_DB_URL 'jdbc:mysql://localhost:3306/deneme'
 
 
%declare PARAM_DB_USERNAME 'root'
 
 
%declare PARAM_DB_PASSWORD 'cloudera'

REGISTER hdfs:///user/lib/mysql-connector-java-5.1.40-bin.jar
REGISTER hdfs:///user/lib/piggybank.jar

TYPED_DATA = LOAD '/user/pigdata/deneme2005' USING PigStorage('+') AS
(
	
	firstData: chararray,
	secondData: chararray,
	thirdData: chararray
	
	
);
 substring_data = FOREACH TYPED_DATA GENERATE  SUBSTRING (firstData, 15, 19),SUBSTRING (firstData, 19, 21),SUBSTRING (firstData, 21, 23);

DUMP substring_data

STORE substring_data INTO 'tarih' using org.apache.pig.piggybank.storage.DBStorage(
 'com.mysql.jdbc.Driver','$PARAM_DB_URL',
'$PARAM_DB_USERNAME','$PARAM_DB_PASSWORD','insert into tarih (year,month,day) values (?,?,?)'); 
