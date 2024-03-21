# Example maven project that compiles using Scala 2.13.13 and Spark 3

## Usage

This is a normal maven project. The fast way to start the project is load from intellij

Run 
```
mvn package
```
should create target/taskDemo-1.0-SNAPSHOT-jar-with-dependencies.jar
But it has some version compatible problem for Maven Assembly Plugin with error "exec-maven-plugin:3.2.0:java are missing or invalid"

Alternatively, run it with 
```
mvn package exec:java -Dexec.mainClass=org.openSky.example.App
```

This project take a sample zip file which contains a csv file as input, 
extract csv file from zip file into temporary folder, 
use Spark to load content of the extracted csv file as dataframe,
filter out status = "Shipped"
groupBy "YEAR_ID" and "PRODUCTLINE", calculate average sales' price by total sales price divide by total quantity. 

Then generate results to a temp folder, then merge all csv files into one file called "output.csv", saved under relative path 'result'

Input file located at 
src/main/resources/sales_data_sample.csv.zip

## Note:
1,File names, Path names, file format are all hardcoded, It just for demo purpose, it should be stored at properties files.
2,No unit test has implemented yet.
3,The program is manual test by a small sample "sales_data_sample2.csv", If use this file as input, the actual result should be only one line with 96.2 in column 'AVERAGE_SALES_AMT' in generated csv file