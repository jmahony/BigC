BigC
====

Multi-threaded Crawler

# Dependencies
*    [MongoDB 2.2.0](https://www.mongodb.org/downloads)
*    [Redis 2.8.9](http://redis.io/download)
*    [Maven 3.2.1](http://maven.apache.org/)

The versions aren't necessarily correct, but they are the only version I have used with BigC.

# Documentation

TODO: Add documentation
# Building
Clone the repository
```
git clone git@github.com:jmahony/BigC.git
```

The Bloom Filter implementation is not in a public Maven repository, so we need
to make it available.
```
git clone git@github.com:DivineTraube/Orestes-Bloomfilter.git
```

Build the Bloom Filter
```
cd Orestes-Bloomfilter && ant && cd ..
```

Add the Orestes-Bloomfilter to your local Maven repository.
```
mvn install:install-file -Dfile=./Orestes-Bloomfilter/build/orestes-bf-with-all-deps.jar -DgroupId=orestes.bloomfilter -DartifactId=orestes-bf -Dversion=1.0 -Dpackaging=jar
```

Build with maven
```
cd BigC && mvn package
```

The uber jar along with an example seed file and log4j config file
```
target/build
```

# Running
## Normal
```
java -jar BigC-VERSION-SNAPSHOT-uber.jar ./politeTimes.json
```
## Logging
```
java -Dlog4j.configurationFile=log4j2.xml -jar BigC-VERSION-SNAPSHOT-uber.jar
```
