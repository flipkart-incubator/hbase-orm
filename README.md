# HBase Object Mapper

## Introduction
This compact utility library is an annotation based *object mapper* for HBase (written in Java) that helps you:

* convert your bean-like objects to HBase rows and vice-versa (for use in Map/Reduce jobs on HBase tables and their unit-tests)
* define *data access objects* for entities that map to HBase rows (for random single/range/bulk access of rows of an HBase table)

## Usage
Let's say you've an HBase table `citizens` with row-key format `country_code#UID`. Let's say your table is created with two column families `main` and `optional` which may have columns like `uid`, `name`, `salary` etc.

This library enables to you represent your HBase row as a class like below:

```java
@HBTable("citizens")
public class Citizen implements HBRecord {
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
    @HBColumn(family = "optional", column = "flags")
    private Map<String, Integer> extraFlags;
    @HBColumn(family = "optional", column = "dependents")
    private Dependents dependents;
    @HBColumnMultiVersion(family = "optional", column = "phone_number")
    private NavigableMap<Long, Integer> phoneNumber; // Multi-versioned column. This annotation enables you to fetch multiple versions of column values

    public String composeRowKey() {
        return String.format("%s#%d", countryCode, uid);
    }

    public void parseRowKey(String rowKey) {
        String[] pieces = rowKey.split("#");
        this.countryCode = pieces[0];
        this.uid = Integer.parseInt(pieces[1]);
    }
} 
```
(see [Citizen.java](./src/test/java/com/flipkart/hbaseobjectmapper/entities/Citizen.java) for a detailed example with more data types)

Now, for above definition of your `Citizen` class,

* you can use methods in `HBObjectMapper` class to convert `Citizen` objects to HBase's `Put` and `Result` objects and vice-versa
* you can inherit from class `AbstractHBDAO` that contains methods like `get` (for random single/bulk/range access of rows), `persist` (for writing rows) and `delete` (for deleting rows)

## Map/Reduce use-cases

### Use in `map()`
HBase's `Result` object can be converted to your bean-like object using below method: 

```java
<T extends HBRecord> T readValue(ImmutableBytesWritable rowKey, Result result, Class<T> clazz)
```

For example:

```java
Citizen e = hbObjectMapper.readValue(key, value, Citizen.class);
```
See file [CitizenMapper.java](./src/test/java/com/flipkart/hbaseobjectmapper/mr/samples/CitizenMapper.java) for full sample code.

### Use in `reduce()`
Your bean-like object can be converted to HBase's `Put` (for row contents) and `ImmutableBytesWritable` (for row key) using below methods:

```java
ImmutableBytesWritable getRowKey(HBRecord obj)
```
```java
Put writeValueAsPut(HBRecord obj)
```
For example, below code in reducer writes your object as one HBase row with appropriate column families and columns:

```java
Citizen citizen = new Citizen(/*details*/);
context.write(hbObjectMapper.getRowKey(citizen), hbObjectMapper.writeValueAsPut(citizen));
```

See file [CitizenReducer.java](./src/test/java/com/flipkart/hbaseobjectmapper/mr/samples/CitizenReducer.java) for full sample code.

### Unit-test for `map()`
Your bean-like object can be converted to HBase's `Put` (for row contents) and `ImmutableBytesWritable` (for row key) using below methods:

```java
ImmutableBytesWritable getRowKey(HBRecord obj)
```
```java
Result writeValueAsResult(HBRecord obj)
```
Below is an example of unit-test of a mapper using [MRUnit](https://mrunit.apache.org/):

```java
Citizen citizen = new Citizen(/*params*/);
mapDriver
    .withInput(
            hbObjectMapper.getRowKey(citizen),
            hbObjectMapper.writeValueAsResult(citizen)
    )
    .withOutput(Util.strToIbw("key"), new IntWritable(citizen.getAge()))
    .runTest();
```


See file [TestCitizenMapper.java](./src/test/java/com/flipkart/hbaseobjectmapper/mr/TestCitizenMapper.java) for full sample code.

### Unit-test for `reduce()`
HBase's `Put` object can be converted to your bean-like object using below method:
 
```java
<T extends HBRecord> T readValue(ImmutableBytesWritable rowKeyBytes, Put put, Class<T> clazz)
```

Below is an example of unit-test of a reducer using [MRUnit](https://mrunit.apache.org/):

```java
Pair<ImmutableBytesWritable, Writable> reducerResult = reducerDriver.withInput(Util.strToIbw("key"), Arrays.asList(new IntWritable(1), new IntWritable(5))).run().get(0);
Citizen citizen = hbObjectMapper.readValue(reducerResult.getFirst(), (Put) reducerResult.getSecond(), Citizen.class);
```

See file [TestCitizenReducer.java](./src/test/java/com/flipkart/hbaseobjectmapper/mr/TestCitizenReducer.java) for full sample code that unit-tests a reducer using [MRUnit](https://mrunit.apache.org/)

## HBase ORM
Since we're dealing with HBase (and not an OLTP system), fitting an ORM paradigm may not make sense. Nevertheless, you can use this library as an HBase-ORM too!

This library provides an abstract class to define your own *data access object*. For example you can create a *data access object* for `Citizen` class in the above example as follows:

```java
import org.apache.hadoop.conf.Configuration;

public class CitizenDAO extends AbstractHBDAO<Citizen> {
    
    public CitizenDAO(Configuration conf) throws IOException {
        super(conf);
    }
}
```
(see [CitizenDAO.java](./src/test/java/com/flipkart/hbaseobjectmapper/daos/CitizenDAO.java))

Once defined, you can access, manipulate and persist a row of `citizens` HBase table as below:

```java
Configuration configuration = getConf(); // this is org.apache.hadoop.conf.Configuration

// Create a data access object:
CitizenDAO citizenDao = new CitizenDAO(configuration);

// Fetch an row from "citizens" HBase table with row key "IND#1":
Citizen pe = citizenDao.get("IND#1");

List<Citizen> lpe = citizenDao.get("IND#1", "IND#5"); //range get

Citizen[] ape = citizenDao.get(new String[] {"IND#1", "IND#2"}); //bulk get

pe.setPincode(560034); // change a field

citizenDao.persist(pe); // Save it back to HBase

citizenDao.delete(pe); // Delete a row by it's object reference

citizenDao.delete("IND#2"); // Delete a row by it's row key
```
(see [TestsAbstractHBDAO.java](./src/test/java/com/flipkart/hbaseobjectmapper/TestsAbstractHBDAO.java) for a more detailed example)


## Maven
Add below entry within the `dependencies` section of your `pom.xml`:

```xml
<dependency>
	<groupId>com.flipkart</groupId>
	<artifactId>hbase-object-mapper</artifactId>
	<version>1.1</version>
</dependency>
```
(See artifact details for [com.flipkart:hbase-object-mapper:1.1]((http://search.maven.org/#artifactdetails%7Ccom.flipkart%7Chbase-object-mapper%7C1.1%7Cjar)) on **Maven Central**)

## How to build?
To build this project, follow below steps:

 * Do a `git clone` of this repository
 * Checkout latest stable version `git checkout v1.1`
 * Execute `mvn clean install` from shell

Currently, this library depends on Hadoop and HBase from Cloudera version 4. If you're using a different version (or even different distribution like [HortonWorks](http://hortonworks.com/)), change the versions in [pom.xml](./pom.xml) to desired ones and do a `mvn clean install`.

**Please note**: Test cases are very comprehensive - they even spin an [in-memory HBase test cluster](https://github.com/apache/hbase/blob/master/hbase-server/src/test/java/org/apache/hadoop/hbase/HBaseTestingUtility.java) to run data access related test cases (near-realworld scenario). So, build times can sometimes be longer.

## Releases

The change log can be found in the [releases](../../releases) section.

## Feature requests and bug reporting

If you intend to request a feature or report a bug, you may use [Github Issues for hbase-object-mapper](../../issues).

## License

Copyright 2015 Flipkart Internet Pvt Ltd.

Licensed under the [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0) (the "License"). You may not use this product or it's source code except in compliance with the License.
