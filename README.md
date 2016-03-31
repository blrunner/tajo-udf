# Tajo UDFs

Apache Tajo is a robust big data relational and distributed data warehouse system for Apache Hadoop. This project is a collection of user defined functions for Tajo. See the [User Manual](http://tajo.apache.org/docs/current/getting_started.html) for more details about Tajo.

This project supports following functions:

* NVL
* SYSDATE

## Requirements

* Mac OS X or Linux
* Java 1.7 or higher
* Maven 3.2.3+ (for building)

## License
[Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Building Tajo UDFs

It is a standard Maven project. Simply run the following command from the project root directory:

    mvn clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

It has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    mvn clean install -DskipTests

## Deploying UDFs
* Copy the UDF jar file into ``$TAJO_HOME/lib`` on all nodes
* Restart Tajo cluster.

## References
* [Nexr Hive UDF](https://github.com/nexr/hive-udf)
* [Oracle 11g SQL Functions](http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions001.htm#i88893)