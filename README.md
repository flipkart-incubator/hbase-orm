# HBase Object Mapper

## Introduction
This compact utility library is an annotation based *object mapper* for HBase (written in Java) that helps you:

* convert objects of your bean-like classes to HBase rows and vice-versa
    * for use in Hadoop MapReduce jobs that read from and/or write to HBase tables
    * and write efficient unit-tests for `Mapper` and `Reducer` classes
* define *data access objects* for entities that map to HBase rows
    * for single/range/bulk access of rows of an HBase table

## Usage
Let's say you've an HBase table `citizens` with row-key format of `country_code#UID`. Now, let's say your table is created with three column families `main`, `optional` and `tracked`, which may have columns `uid`, `name`, `salary` etc.

This library enables to you represent your HBase table as a bean-like class, as below:

```java
@HBTable(name = "citizens", families = {@Family(name = "main"), @Family(name = "optional", versions = 3), @Family(name = "tracked", versions = 10)})
public class Citizen implements HBRecord<String> {

    @HBRowKey
    private String countryCode;
    
    @HBRowKey
    private Integer uid;
    
    @HBColumn(family = "main", column = "name")
    private String name;
    
    @HBColumn(family = "optional", column = "age")
    private Short age;
    
    @HBColumn(family = "optional", column = "salary")
    private Integer sal;
    
    @HBColumn(family = "optional", column = "custom_details")
    private Map<String, Integer> customDetails;
    
    @HBColumn(family = "optional", column = "dependents")
    private Dependents dependents;
    
    @HBColumnMultiVersion(family = "tracked", column = "phone_number")
    private NavigableMap<Long, Integer> phoneNumber;
    
    @HBColumn(family = "optional", column = "pincode", codecFlags = {@Flag(name = BestSuitCodec.SERIALIZE_AS_STRING, value = "true")})
    private Integer pincode;
    
    @Override
    public String composeRowKey() {
        return String.format("%s#%d", countryCode, uid);
    }

    @Override
    public void parseRowKey(String rowKey) {
        String[] pieces = rowKey.split("#");
        this.countryCode = pieces[0];
        this.uid = Integer.parseInt(pieces[1]);
    }
    
    // Constructors, getters and setters
} 
```
That is,

* The above class `Citizen` represents the HBase table `citizens`, using the `@HBTable` annotation.
* Logics for conversion of HBase row key to member variables of `Citizen` objects and vice-versa are implemented using `parseRowKey` and `composeRowKey` methods respectively.
* The data type representing row key is the type parameter to `HBRecord` generic interface (in above case, `String`). Fields that form row key are annotated with `@HBRowKey`.
* Names of columns and their column families are specified using `@HBColumn` or `@HBColumnMultiVersion` annotations.
* The class may contain fields of simple data types (e.g. `String`, `Integer`), generic data types (e.g. `Map`, `List`), custom class (e.g. `Dependents`) or even generics of custom class (e.g. `List<Dependent>`) 
* The `@HBColumnMultiVersion` annotation allows you to map multiple versions of column in a `NavigableMap<Long, ?>`. In above example, field `phoneNumber` is mapped to column `phone_number` within the column family `tracked` (which is configured for multiple versions)

See source files [Citizen.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/entities/Citizen.java) and [Employee.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/entities/Employee.java) for detailed examples.

### Serialization / Deserialization

* The default codec of this library has the following behavior:
    * uses HBase's native methods to serialize objects of data types `Boolean`, `Short`, `Integer`, `Long`, `Float`, `Double`, `String` and `BigDecimal`
    * uses [Jackson's JSON serializer](http://wiki.fasterxml.com/JacksonHome) for all other data types
    * serializes `null` as `null`
* To control/modify serialization/deserialization behavior, you may define your own codec (by implementing the `Codec` interface) or you may extend the default codec (`BestSuitCodec`).
* The optional parameter `codecFlag` (supported by both `@HBColumn` and `@HBColumnMultiVersion` annotations) can be used to pass custom flags to the underlying codec. (e.g. You may write your codec to serialize field `Integer id` in `Citizen` class differently from field `Integer id` in `Employee` class)
* The default codec class `BestSuitCodec` takes a flag `BestSuitCodec.SERIALIZE_AS_STRING`, whose value is "serializeAsString" (as in the above `Citizen` class example). When this flag is set to `true` on a field, the default codec serializes that field (even numerical fields) as `String`s.
    * Your custom codec may take other such flags to customize serialization/deserialization behavior at a class field level.

## MapReduce use-cases

### Mapper
If your MapReduce job is reading from an HBase table, in your `map()` method, HBase's `Result` object can be converted to object of your bean-like class using below method: 

```java
T readValue(ImmutableBytesWritable rowKey, Result result, Class<T> clazz)
```

For example:

```java
Citizen e = hbObjectMapper.readValue(key, value, Citizen.class);
```
See file [CitizenMapper.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/mr/samples/CitizenMapper.java) for full sample code.

### Reducer
If your MapReduce job is writing to an HBase table, in your `reduce()` method, object of your bean-like class can be converted to HBase's `Put` (for row contents) and `ImmutableBytesWritable` (for row key) using below methods:

```java
ImmutableBytesWritable getRowKey(HBRecord<R> obj)
```
```java
Put writeValueAsPut(HBRecord<R> obj)
```
For example, below code in Reducer writes your object as one HBase row with appropriate column families and columns:

```java
Citizen citizen = new Citizen(/*details*/);
context.write(hbObjectMapper.getRowKey(citizen), hbObjectMapper.writeValueAsPut(citizen));
```

See file [CitizenReducer.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/mr/samples/CitizenReducer.java) for full sample code.

### Unit-test for Mapper
If your MapReduce job is reading from an HBase table, you would want to unit-test your `map()` method as below.

Object of your bean-like class can be converted to HBase's `Result` (for row contents) and `ImmutableBytesWritable` (for row key) using below methods:

```java
ImmutableBytesWritable getRowKey(HBRecord<R> obj)
```
```java
Result writeValueAsResult(HBRecord<R> obj)
```
Below is an example of unit-test of a Mapper using [MRUnit](https://mrunit.apache.org/):

```java
Citizen citizen = new Citizen(/*params*/);
citizenMapDriver
.withInput(
	hbObjectMapper.getRowKey(citizen),
	hbObjectMapper.writeValueAsResult(citizen)
)
.withOutput(
	hbObjectMapper.toIbw("key"),
	new IntWritable(citizen.getAge())
)
.runTest();
```

See file [TestCitizenMR.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/mr/TestCitizenMR.java) for full sample code.

### Unit-test for Reducer
If your MapReduce job is writing to an HBase table, you would want to unit-test your `reduce()` method as below.

HBase's `Put` object can be converted to your object of you bean-like class using below method:
 
```java
T readValue(ImmutableBytesWritable rowKey, Put put, Class<T> clazz)
```

Below is an example of unit-test of a Reducer using [MRUnit](https://mrunit.apache.org/):

```java
Pair<ImmutableBytesWritable, Mutation> reducerResult = citizenReduceDriver
	.withInput(
		hbObjectMapper.toIbw("key"),
		inputList
		)
	.run()
.get(0);
CitizenSummary citizenSummary = hbObjectMapper.readValue(
	reducerResult.getFirst(),
	(Put) reducerResult.getSecond(),
	CitizenSummary.class
);
```

Again, see file [TestCitizenMR.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/mr/TestCitizenMR.java) for full sample code.

## HBase ORM
This library provides an abstract class to define your own *data access object*. For example you can create a *data access object* for `Citizen` class in the above example as follows:

```java
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CitizenDAO extends AbstractHBDAO<String, Citizen> {

    public CitizenDAO(Configuration conf) throws IOException {
        super(conf); // if you need to customize your codec, you may use super(conf, codec)
    }
}
```
(see [CitizenDAO.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/daos/CitizenDAO.java))

Once defined, you can access, manipulate and persist a row of `citizens` HBase table as below:

```java
Configuration configuration = getConf(); // this is org.apache.hadoop.conf.Configuration

// Create a data access object:
CitizenDAO citizenDao = new CitizenDAO(configuration);

// Fetch a row from "citizens" HBase table with row key "IND#1":
Citizen pe = citizenDao.get("IND#1");

Citizen[] ape = citizenDao.get(new String[] {"IND#1", "IND#2"}); //bulk get

// In below, note that "IND#1" is inclusive and "IND#5" is exclusive
List<Citizen> lpe = citizenDao.get("IND#1", "IND#5"); //range get
// ('versioned' variant above method is available)

// for row keys in range ["IND#1", "IND#5"), fetch 3 versions of field 'phoneNumber' as a NavigableMap<row key, NavigableMap<timestamp, column value>>:
NavigableMap<String, NavigableMap<Long, Object>> phoneNumberHistory 
	= citizenDao.fetchFieldValues("IND#1", "IND#5", "phoneNumber", 3);
// (bulk variants of above range method are also available)

pe.setPincode(560034); // change a field

citizenDao.persist(pe); // Save it back to HBase

citizenDao.delete(pe); // Delete a row by it's object reference

citizenDao.delete(Arrays.asList(pe1, pe2)); // Delete multiple rows by list of object references

citizenDao.delete("IND#2"); // Delete a row by it's row key

citizenDao.delete(new String[] {"IND#3", "IND#4"}); // Delete a bunch of rows by their row keys

citizenDao.getHBaseTable() // returns HTable instance (in case you want to directly play around) 

```
(see [TestsAbstractHBDAO.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/TestsAbstractHBDAO.java) for more detailed examples)

**Please note:** Since we're dealing with HBase (and not an OLTP data store), fitting a classical ORM paradigm may not make sense. So this library doesn't intend to evolve as a full-fledged ORM. However, if you do intend to use HBase via an ORM library, I suggest you use [Apache Phoenix](https://phoenix.apache.org/).


## Limitations

* Being an *object mapper*, this library works for pre-defined columns only. For example, this library doesn't provide ways to fetch:
 * columns matching a pattern or a regular expression
 * unmapped columns of a column family
* This library doesn't provide you a way to 'selectively fetch and populate fields of your bean-like class' when you `get` a row by it's key. (However, you can still fetch column values selectively for one or more rows by using `fetchFieldValue` and `fetchFieldValues` methods)

## Maven
Add below entry within the `dependencies` section of your `pom.xml`:

```xml
<dependency>
	<groupId>com.flipkart</groupId>
	<artifactId>hbase-object-mapper</artifactId>
	<version>1.8</version>
</dependency>
```
See artifact details: [com.flipkart:hbase-object-mapper on **Maven Central**](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.flipkart%22%20AND%20a%3A%22hbase-object-mapper%22) or
[com.flipkart:hbase-object-mapper on **MVN Repository**](https://mvnrepository.com/artifact/com.flipkart/hbase-object-mapper).
## How to build?
To build this project, follow below steps:

 * Do a `git clone` of this repository
 * Checkout latest stable version `git checkout v1.8`
 * Execute `mvn clean install` from shell

Currently, projects that use this library are running on [Hortonworks Data Platform v2.4](https://hortonworks.com/blog/apache-hadoop-2-4-0-released/) (corresponds to Hadoop 2.7 and HBase 1.1). However, if you're using a different distribution of Hadoop (like [Cloudera](http://www.cloudera.com/)) or if you are using a different version of Hadoop, you may change the versions in [pom.xml](./pom.xml) to desired ones and build the project.

**Please note**: Test cases are very comprehensive - they even spin an [in-memory HBase test cluster](https://github.com/apache/hbase/blob/master/hbase-server/src/test/java/org/apache/hadoop/hbase/HBaseTestingUtility.java) to run data access related test cases (near-realworld scenario). So, build times can sometimes be longer, depending on your machine configuration.

## Releases

The change log can be found in the [releases](//github.com/flipkart-incubator/hbase-object-mapper/releases) section.

## Feature requests and bug reporting

If you intend to request a feature or report a bug, you may use [Github Issues for hbase-object-mapper](//github.com/flipkart-incubator/hbase-object-mapper/issues).

## License

Copyright 2017 Flipkart Internet Pvt Ltd.

Licensed under the [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0) (the "License"). You may not use this product or it's source code except in compliance with the License.
