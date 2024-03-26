# Example maven project that compiles using Scala 2.13.13 and Spark 3

## Usage

This is a normal maven project. 
The fast way to start the project is load from intellij, configure project java version to be java 8, like temurin-1.8.0_402

Run it from command line, make sure the java version from command line is java 8, it will have compatible problem for higher java version with Maven Assembly Plugin with error "exec-maven-plugin:3.2.0:java are missing or invalid"
```
mvn clean install
```

Alternatively, run it with 
```
mvn package
```
Should create target/taskDemo-1.0-SNAPSHOT-jar-with-dependencies.jar
Then invoke the jar file by provide executable java8 full path if default java is other version
```
/Users/jiacansong/Library/Java/JavaVirtualMachines/temurin-1.8.0_402/Contents/Home/bin/java -jar target/taskDemo-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Run it in Docker
```
docker build -t scala-spark-demo .
```
After the image is built
```
docker run scala-spark-demo
```

This project take a sample zip file which contains a csv file as input, 
extract csv file from zip file into temporary folder, 
use Spark to load content of the extracted csv file as dataframe,
filter out status = "Shipped"
groupBy "YEAR_ID" and "PRODUCTLINE", calculate average sales' price by total sales price divide by total quantity. 

Then generate results to a temp folder, then merge all csv files into one file called "output.csv", saved under relative path 'result'

Put the Input file path to json path app.filePath.sourceZIP of application.conf
