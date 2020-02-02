# HBase ORM

## Introduction
An ultra-light-weight HBase ORM library that enables:

1. object-oriented access of HBase rows (Data Access Object) with minimal code and good testability
2. reading from and/or writing to HBase tables in Hadoop MapReduce jobs


## Usage
Let's say you've an HBase table `citizens` with row-key format of `country_code#UID`. Now, let's say this table is created with three column families `main`, `optional` and `tracked`, which may have columns (qualifiers) `uid`, `name`, `salary` etc.

This library enables to you represent your HBase table as a *bean-like class*, as below:

```java
@HBTable(name = "citizens",
  families = {
    @Family(name = "main"),
    @Family(name = "optional", versions = 3),
    @Family(name = "tracked", versions = 10)
  }
)
public class Citizen implements HBRecord<String> {

  private String countryCode;

  private Integer uid;

  @HBColumn(family = "main", column = "name")
  private String name;

  @HBColumn(family = "optional", column = "age")
  private Short age;

  @HBColumn(family = "optional", column = "salary")
  private Integer sal;

  @HBColumn(family = "optional", column = "counter")
  private Long counter;

  @HBColumn(family = "optional", column = "custom_details")
  private Map<String, Integer> customDetails;

  @HBColumn(family = "optional", column = "dependents")
  private Dependents dependents;

  @HBColumnMultiVersion(family = "tracked", column = "phone_number")
  private NavigableMap<Long, Integer> phoneNumber;

  @HBColumn(family = "optional", column = "pincode", codecFlags = {
    @Flag(name = BestSuitCodec.SERIALIZE_AS_STRING, value = "true")
  })
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
* The data type representing row key is the type parameter to `HBRecord` generic interface (in above case, `String`).
  * Note that `String` is both `Comparable` and `Serializable`.
* Names of columns and their column families are specified using `@HBColumn` or `@HBColumnMultiVersion` annotations.
* The class may contain fields of simple data types (e.g. `String`, `Integer`), generic data types (e.g. `Map`, `List`), custom class (e.g. `Dependents`) or even generics of custom class (e.g. `List<Dependent>`) 
* The `@HBColumnMultiVersion` annotation allows you to map multiple versions of column in a `NavigableMap<Long, ?>`. In above example, field `phoneNumber` is mapped to column `phone_number` within the column family `tracked` (which is configured for multiple versions)

Alternatively, you can model your class as below:

```java
...
class CitizenKey implements Serializable, Comparable<CitizenKey> {
    String countryCode;
    Integer uid;

    @Override
    public int compareTo(CitizenKey key) {
        // your custom logic involving countryCode and uid
    }
}

public class Citizen implements HBRecord<CitizenKey> {

    private CitizenKey rowKey;

    @Override
    public CitizenKey composeRowKey() {
        return rowKey;
    }

    @Override
    public void parseRowKey(CitizenKey rowKey) {
        this.rowKey = rowKey;
    }
}
```
See source files [Citizen.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/entities/Citizen.java) and [Employee.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/entities/Employee.java) for detailed examples. Specifically, [Employee.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/entities/Employee.java) demonstrates using "column inheritance" of this library, a useful feature if you have many HBase tables with common set of columns.

### Serialization / Deserialization mechanism

* The default codec (called [BestSuitCodec](./src/main/java/com/flipkart/hbaseobjectmapper/codec/BestSuitCodec.java)) included in this library has the following behavior:
  * uses HBase's native methods to serialize objects of data types `Boolean`, `Short`, `Integer`, `Long`, `Float`, `Double`, `String` and `BigDecimal` (see: [Bytes](https://hbase.apache.org/2.0/devapidocs/org/apache/hadoop/hbase/util/Bytes.html))
  * uses [Jackson's JSON serializer](https://en.wikipedia.org/wiki/Jackson_(API)) for all other data types
  * serializes `null` as `null`
* To customize serialization/deserialization behavior, you may define your own codec (by implementing the [Codec](./src/main/java/com/flipkart/hbaseobjectmapper/codec/Codec.java) interface) or you may extend the default codec.
* The optional parameter `codecFlags` (supported by both `@HBColumn` and `@HBColumnMultiVersion` annotations) can be used to pass custom flags to the underlying codec. (e.g. You may want your codec to serialize field `Integer id` in `Citizen` class differently from field `Integer id` in `Employee` class)
* The default codec class `BestSuitCodec` takes a flag `BestSuitCodec.SERIALIZE_AS_STRING`, whose value is "serializeAsString" (as in the above `Citizen` class example). When this flag is set to `true` on a field, the default codec serializes that field (even numerical fields) as strings.
  * Your custom codec may take other such flags to customize serialization/deserialization behavior at a **class field level**.

## Using this library for database access (DAO)
This library provides an abstract class to define your own [data access object](https://en.wikipedia.org/wiki/Data_access_object). For example, you can create one for `Citizen` class in the above example as follows:

```java
import org.apache.hadoop.hbase.client.Connection;
import java.io.IOException;

public class CitizenDAO extends AbstractHBDAO<String, Citizen> {
// in above, String is the row type of Citizen

  public CitizenDAO(Connection connection) throws IOException {
    super(connection); // if you need to customize your codec, you may use super(connection, codec)
    // alternatively, you can construct CitizenDAO by passing instance of 'org.apache.hadoop.conf.Configuration'
  }
}
```
(see [CitizenDAO.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/daos/CitizenDAO.java))

Once defined, you can instantiate your *data access object* as below:

```java
CitizenDAO citizenDao = new CitizenDAO(connection);
```
You can access, manipulate and persist records of `citizens` table as shown in below examples:

Create new record:

```java
String rowKey = citizenDao.persist(new Citizen("IND", 1, /* more params */));
// In above, output of 'persist' is a String, because Citizen class implements HBRecord<String>
```

Fetch a single record by its row key:

```java
// Fetch row from "citizens" HBase table whose row key is "IND#1":
Citizen pe = citizenDao.get("IND#1");
```

Fetch multiple records by their row keys:

```java
Citizen[] ape = citizenDao.get(new String[] {"IND#1", "IND#2"}); //bulk get
```

Fetch records by range of row keys (start row key, end row key):

```java
List<Citizen> lpe1 = citizenDao.get("IND#1", "IND#5");
// above uses default behavior: start key inclusive, end key exclusive, 1 version

List<Citizen> lpe2 = citizenDao.get("IND#1", true, "IND#9", true, 5, 10000);
// above fetches with: start key inclusive, end key inclusive, 5 versions, caching set to 10,000 rows 

```

Iterate over *large number of records* by range of row keys:

```java
try (Records<Citizen> citizens = citizenDao.records("IND#000000001", true, "IND#100000000", true, 1, 10000)) {
// using try-with-resources above to close the resources after iteration
  for (Citizen citizen : citizens) {
    // your code
  }
}
```
**Note:** All the `.records(...)` methods efficiently use iterators internally and do not load records upfront into memory. Hence, it's safe to fetch millions of records using them.

Fetch records by row key prefix:

```java
// For small number of records:
List<Citizen> lpe3 = citizenDao.getByPrefix(citizenDao.toBytes("IND#"));

// For large number of records:
try (Records<Citizen> citizens = citizenDao.recordsByPrefix(citizenDao.toBytes("IND#"))) {
  for (Citizen citizen : citizens) {
    // do something
  }
}
```

Fetch records by HBase's native `Scan` object: (for very advanced access patterns)

```java
Scan scan = new Scan().setAttribute(...)
  .setReadType(...)
  .setACL(...)
  .withStartRow(...)
  .withStopRow(...)
  .readAllVersions(...);
try (Records<Citizen> citizens = citizenDao.records(scan)) {
  for (Citizen citizen : citizens) {
    // do something
  }
}
```

Fetch specific field(s) for given row key(s):

```java
// for row keys in range ["IND#1", "IND#5"), fetch 3 versions of field 'phoneNumber':
NavigableMap<String, NavigableMap<Long, Object>> phoneNumberHistory 
  = citizenDao.fetchFieldValues("IND#1", "IND#5", "phoneNumber", 3);
// bulk variants of above range method are also available
```

Read data from HBase using HBase's native `Get`:

```java
Get get1 = citizenDao.getGet("IND#2"); // returns object of HBase's Get corresponding to row key "IND#2", to enable advanced read patterns
counterDAO.getOnGets(get1); 

Get get2 = citizenDao.getGet("IND#2").setTimeRange(1, 5).setMaxVersions(2); // Advanced HBase row fetch
counterDAO.getOnGets(get2);
```

Manipulate and persist an object back to HBase:

```java
// change a field:
pe.setPincode(560034);

// Save the record back to HBase:
citizenDao.persist(pe); 
```

Delete records in various ways:

```java
// Delete a row by it's object reference:
citizenDao.delete(pe);

// Delete multiple rows by list of object references:
citizenDao.delete(Arrays.asList(pe1, pe2)); 

// Delete a row by it's row key:
citizenDao.delete("IND#2"); 

 // Delete a bunch of rows by their row keys:
citizenDao.delete(new String[] {"IND#3", "IND#4"});
```

Increment a column in HBase:

```java
// Increment value of counter by 3:
citizenDao.increment("IND#2", "counter", 3L); 
```

Append to a column:

```java
citizenDao.append("IND#2", "name", " Kalam");
// there are 'bulk methods' available
```

Other operations:

```java
citizenDao.getHBaseTable() // returns HTable instance (in case you want to directly play around) 
```

(see [TestsAbstractHBDAO.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/TestsAbstractHBDAO.java) for more detailed examples)

**Please note:** Since we're dealing with HBase (and not an OLTP data store), fitting a classical (Hibernate-like) ORM paradigm may not make sense. So this library doesn't intend to evolve as a full-fledged ORM. However, if that's your intent, I suggest you use [Apache Phoenix](https://phoenix.apache.org/).


## Using this library for DDL operations
The provided `HBAdmin` class helps you programatically create/delete tables.

You may instantiate the class using `Connection` object:

```java
HBAdmin hbAdmin = new HBAdmin(connection);
```

Once instantiated, you may do the following DDL operations:

```java
hbAdmin.createTable(Citizen.class); 
// Above statement creates table with name and column families specification as per the @HBTable annotation on the Citizen class

hbAdmin.tableExists(Citizen.class); // returns true

hbAdmin.disableTable(Citizen.class);

hbAdmin.deleteTable(Citizen.class);

```

Note that **all** of the above are very heavy and time-consuming operations.

## Using this library in MapReduce jobs

### Mapper
If your MapReduce job is reading from an HBase table, in your `map()` method, HBase's `Result` object can be converted to object of your bean-like class using below method: 

```java
T readValue(ImmutableBytesWritable rowKey, Result result, Class<T> clazz)
```

For example:

```java
Citizen e = hbObjectMapper.readValue(key, value, Citizen.class);
```

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
### Unit-test for Mapper
If your MapReduce job is reading from an HBase table, you would want to unit-test your `map()` method as below.

Object of your bean-like class can be converted to HBase's `Result` (for row contents) and `ImmutableBytesWritable` (for row key) using below methods:

```java
ImmutableBytesWritable getRowKey(HBRecord<R> obj)
```
```java
Result writeValueAsResult(HBRecord<R> obj)
```
Below is an example of unit-test of a Mapper using [MRUnit](https://attic.apache.org/projects/mrunit.html):

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

### Unit-test for Reducer
If your MapReduce job is writing to an HBase table, you would want to unit-test your `reduce()` method as below.

HBase's `Put` object can be converted to your object of you bean-like class using below method:
 
```java
T readValue(ImmutableBytesWritable rowKey, Put put, Class<T> clazz)
```

Below is an example of unit-test of a Reducer using [MRUnit](https://attic.apache.org/projects/mrunit.html):

```java
Pair<ImmutableBytesWritable, Mutation> reducerResult = citizenReduceDriver
  .withInput(hbObjectMapper.toIbw("key"), inputList)
  .run()
.get(0);
CitizenSummary citizenSummary = hbObjectMapper.readValue(
  reducerResult.getFirst(),
  (Put) reducerResult.getSecond(),
  CitizenSummary.class
);
```

## Advantages
 * Your application code will be clean and minimal.
 * Your code need not worry about HBase methods or serialization/deserialization at all, thereby helping you maintain clear [separation of concerns](https://en.wikipedia.org/wiki/Separation_of_concerns).
 * Classes are **thread-safe**. You just have to instantiate your DAO classes once at the start of your application and use them anywhere!
 * Light weight: This library depends on just HBase Client and few other small libraries. It has very low overhead and hence is very fast.
 * Customizability/Extensibility: Want to use HBase native methods directly in some cases? No problem. Want to customize ser/deser in general or for a given class field? No problem. This library is high flexible.

## Limitations
Being an *object mapper*, this library works for pre-defined columns only. For example, this library doesn't provide ways to fetch:

  * columns matching a pattern or a regular expression
  * unmapped columns of a column family

## Maven
Add below entry within the `dependencies` section of your `pom.xml`:

```xml
<dependency>
  <groupId>com.flipkart</groupId>
  <artifactId>hbase-object-mapper</artifactId>
  <version>1.14</version>
</dependency>
```

See artifact details: [com.flipkart:hbase-object-mapper on **Maven Central**](https://search.maven.org/search?q=g:com.flipkart%20AND%20a:hbase-object-mapper&core=gav) or
[com.flipkart:hbase-object-mapper on **MVN Repository**](https://mvnrepository.com/artifact/com.flipkart/hbase-object-mapper).

## How to build?
To build this project, follow below simple steps:

 1. Do a `git clone` of this repository
 2. Checkout latest stable version `git checkout v1.14`
 3. Execute `mvn clean install` from shell

### Please note:

 * Currently, projects that use this library are running on [Hortonworks Data Platform v3.1](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.0/index.html) (corresponds to Hadoop 3.1 and HBase 2.0). However, if you are using a different version of Hadoop/HBase, you may change the versions in [pom.xml](./pom.xml) to desired ones and build the project.
 * Test cases are **very comprehensive**. So, `mvn` build times can sometimes be longer, depending on your machine configuration.
 * By default, test cases spin an [in-memory HBase test cluster](https://github.com/apache/hbase/blob/master/hbase-server/src/test/java/org/apache/hadoop/hbase/HBaseTestingUtility.java) to run data access related test cases (near-realworld scenario). 
    * If test cases are failing with time out errors, you may increase the timeout by setting environment variable `INMEMORY_CLUSTER_START_TIMEOUT` (seconds). For example, on Linux you may run the command `export INMEMORY_CLUSTER_START_TIMEOUT=8` on terminal, before running the aforementioned `mvn` command.
 * You may direct test cases to use an actual HBase cluster (instead of default in-memory one) by setting `USE_REAL_HBASE` environmental variable to `true`.
    * If you're using this option, ensure you've correct settings in your `hbase-site.xml`.
 * Test cases check for a lot of 'boundary conditions'. So, you'll see a lot of exceptions in logs. They are **not** failures.

## Releases

The change log can be found in the [releases](//github.com/flipkart-incubator/hbase-orm/releases) section.

## Feature requests and bug reporting

If you intend to request a feature or report a bug, you may use [Github Issues for hbase-orm](//github.com/flipkart-incubator/hbase-orm/issues).

## License

Copyright 2020 Flipkart Internet Pvt Ltd.

Licensed under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0) (the "License"). You may not use this product or it's source code except in compliance with the License.
