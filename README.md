# HBase ORM

[![Build Status](https://api.travis-ci.org/flipkart-incubator/hbase-orm.svg?branch=master&status=passed)](https://travis-ci.org/github/flipkart-incubator/hbase-orm)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/flipkart-incubator/hbase-orm.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/flipkart-incubator/hbase-orm/context:java)
[![Coverage Status](https://coveralls.io/repos/github/flipkart-incubator/hbase-orm/badge.svg?branch=master)](https://coveralls.io/github/flipkart-incubator/hbase-orm?branch=master)
[![Maven Central](https://img.shields.io/badge/sonatype-1.18-blue.svg)](https://oss.sonatype.org/content/repositories/releases/com/flipkart/hbase-object-mapper/1.18/)
[![License](https://img.shields.io/badge/License-Apache%202-blue.svg)](./LICENSE.txt)

## Introduction
HBase ORM is a light-weight, production-grade, thread-safe and performant library that enables:

1. object-oriented access of HBase rows (Data Access Object) with minimal code and good testability.
2. reading from and/or writing to HBase tables in Hadoop MapReduce jobs.

This can also be used as an ORM for Bigtable. Scroll down till the relevant section to know how.

## Usage
Let's say you've an HBase table `citizens` with row-key format of `country_code#UID`. Now, let's say this table is created with three column families `main`, `optional` and `tracked`, which may have columns (qualifiers) `uid`, `name`, `salary` etc.

This library enables to you represent your HBase table as a *bean-like class*, as below:

```java
@HBTable(namepsace = "govt", name = "citizens",
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

  @HBColumn(family = "optional", column = "income")
  private Integer annualIncome;

  @HBColumn(family = "optional", column = "registration_date")
  private LocalDateTime registrationDate;

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

* The above class `Citizen` represents the HBase table `citizens` in namespace `govt`, using the `@HBTable` annotation.
* Logics for conversion of HBase row key to member variables of `Citizen` objects and vice-versa are implemented using `parseRowKey` and `composeRowKey` methods respectively.
* The data type representing row key is the type parameter to `HBRecord` generic interface (in above case, `String`).
  * Note that `String` is both '`Comparable` with itself' and `Serializable`.
* Names of columns and their column families are specified using `@HBColumn` or `@HBColumnMultiVersion` annotations.
* The class may contain fields of simple data types (e.g. `String`, `Integer`), generic data types (e.g. `Map`, `List`), custom class (e.g. `Dependents`) or even generics of custom class (e.g. `List<Dependent>`) 
* The `@HBColumnMultiVersion` annotation allows you to map multiple versions of column in a `NavigableMap<Long, ?>`. In above example, field `phoneNumber` is mapped to column `phone_number` within the column family `tracked` (which is configured for multiple versions)

Alternatively, you can model your class as below:

```java
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

* Serialization and deserialization are handled through 'codecs'.
* The default codec (called [BestSuitCodec](./src/main/java/com/flipkart/hbaseobjectmapper/codec/BestSuitCodec.java)) included in this library has the following behavior:
  * uses HBase's native methods to serialize objects of data types `Boolean`, `Short`, `Integer`, `Long`, `Float`, `Double`, `String` and `BigDecimal` (see: [Bytes](https://hbase.apache.org/2.0/devapidocs/org/apache/hadoop/hbase/util/Bytes.html))
  * uses [Jackson's JSON serializer](https://github.com/FasterXML/jackson) for all other data types
  * serializes `null` as `null`
* To customize serialization/deserialization behavior, you may define your own codec (by implementing the [Codec](./src/main/java/com/flipkart/hbaseobjectmapper/codec/Codec.java) interface) or you may extend the default codec.
* The optional parameter `codecFlags` (supported by both `@HBColumn` and `@HBColumnMultiVersion` annotations) can be used to pass custom flags to the underlying codec. (e.g. You may want your codec to serialize field `Integer id` in `Citizen` class differently from field `Integer id` in `Employee` class)
* The default codec class `BestSuitCodec` takes a flag `BestSuitCodec.SERIALIZE_AS_STRING`, whose value is "serializeAsString" (as in the above `Citizen` class example). When this flag is set to `true` on a field, the default codec serializes that field (even numerical fields) as strings.
  * Your custom codec may take other such flags as inputs to customize serialization/deserialization behavior at a **class field level**.

## Using this library for database access (DAO)
This library provides an abstract class to define your own [data access object](https://en.wikipedia.org/wiki/Data_access_object). For example, you can create one for `Citizen` class in the above example as follows:

```java
import org.apache.hadoop.hbase.client.Connection;
import com.flipkart.hbaseobjectmapper.AbstractHBDAO;
import java.io.IOException;

public class CitizenDAO extends AbstractHBDAO<String, Citizen> {
// in above, String is the 'row type' of Citizen

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
**Side note**: As you'd know, HBase's `Connection` creation is a heavy-weight operation
(Details: [Connection](https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/client/Connection.html)).
So, it is recommended that you create `Connection` instance once and use it for the entire life cycle of your program across all the DAO classes that you create (such as above).

Now, you can access, manipulate and persist records of `citizens` table as shown in below examples:

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
// Read records from 'IND#000000001' (inclusive) to 'IND#100000000' (exclusive):
try (Records<Citizen> citizens = citizenDao.records("IND#000000001", "IND#100000000")) {
  for (Citizen citizen : citizens) {
    // your code
  }
}

// Read records from 'IND#000000001' (inclusive) to 'IND#100000000' (inclusive) with 
// 5 versions, caching set to 1,000 rows:
try (Records<Citizen> citizens = citizenDao.records("IND#000000001", true, "IND#100000000", true, 5, 1000)) {
  for (Citizen citizen : citizens) {
    // your code
  }
}
```
**Note:** All the `.records(...)` methods efficiently use iterators internally and do not load records upfront into memory. Hence, it is safe to fetch millions of records using them.

Fetch records by row key prefix:

```java
List<Citizen> lpe3 = citizenDao.getByPrefix(citizenDao.toBytes("IND#"));
```

Iterate over *large number of records* by row key prefix:

```java
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
Get get1 = citizenDao.getGet("IND#2");
// above returns object of HBase's Get corresponding to row key "IND#2", to enable advanced read patterns
Counter counter1 = counterDAO.getOnGet(get1);

Get get2 = citizenDao.getGet("IND#2").setTimeRange(1, 5).setMaxVersions(2); // Advanced HBase row fetch
Counter counter2 = counterDAO.getOnGet(get2);
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
// Delete a row by its object reference:
citizenDao.delete(pe);

// Delete multiple rows by list of object references:
citizenDao.delete(Arrays.asList(pe1, pe2)); 

// Delete a row by its row key:
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
Table citizenTable = citizenDao.getHBaseTable()
// in case you want to directly class HBase's native methods
```

(see [TestsAbstractHBDAO.java](./src/test/java/com/flipkart/hbaseobjectmapper/testcases/TestsAbstractHBDAO.java) for more detailed examples)

**Please note:** Since we're dealing with HBase (and not a classical RDBMS), fitting a Hibernate-like ORM may not make sense. So, this library does **not** intend to evolve as a full-fledged ORM. However, if that's your intent, I suggest you use [Apache Phoenix](https://phoenix.apache.org/).


## Using this library for DDL operations
The provided `HBAdmin` class helps you programatically create/delete tables.

You may instantiate the class using `Connection` object:

```java
import org.apache.hadoop.hbase.client.Connection;
import com.flipkart.hbaseobjectmapper.HBAdmin;

// some code

HBAdmin hbAdmin = new HBAdmin(connection);
```

Once instantiated, you may do the following DDL operations:

```java
hbAdmin.createTable(Citizen.class); 
// Above statement creates table with name and column families specification as per
// the @HBTable annotation on the Citizen class

hbAdmin.tableExists(Citizen.class); // returns true/false

hbAdmin.disableTable(Citizen.class);

hbAdmin.deleteTable(Citizen.class);

```

Note that DDL operations on HBase are typically heavy and time-consuming.

## Using this library to handle HBase data types
The `HBObjectMapper` class in this library provides the useful methods such as below:

```java
Result writeValueAsResult(T record)
```
```java
T readValue(Result result, Class<T> clazz)
```
where `T` is your bean-like class that extends this library's `HBRecord` interface (e.g. `Citizen` class above).

Using these, you can convert your object to HBase's `Result` and vice versa.

## Using this library in MapReduce jobs
Read [article](//github.com/flipkart-incubator/hbase-orm/wiki/Using-this-library-in-MapReduce-jobs).

## Advantages
 * Your application code will be **clean** and **minimal**.
 * Your code need not worry about HBase methods or serialization/deserialization at all, thereby helping you maintain clear [separation of concerns](https://en.wikipedia.org/wiki/Separation_of_concerns).
 * Classes are **thread-safe**. You just have to instantiate your DAO classes once at the start of your application and use them anywhere throughout the life-cycle of your application!
 * **Light weight**: This library depends on just [hbase-client](https://mvnrepository.com/artifact/org.apache.hbase/hbase-client) and few other small libraries. It has very low overhead and hence is very fast.
 * Customizability/Extensibility: Want to use HBase's native methods directly in some cases? You can do that. Want to customize serialization/deserialization for a given type or for a specific given class field? You can do that too. This library is very flexible.

## Limitations
Being an *object mapper*, this library works for predefined columns only. For example, this library doesn't provide ways to fetch:

  * columns matching a pattern or a regular expression
  * unmapped columns of a column family

## Adding to your build
If you are using Maven, add below entry within the `dependencies` section of your `pom.xml`:

```xml
<dependency>
  <groupId>com.flipkart</groupId>
  <artifactId>hbase-object-mapper</artifactId>
  <version>1.18</version>
</dependency>
```

See artifact details: [com.flipkart:hbase-object-mapper on **Maven Central**](https://search.maven.org/search?q=g:com.flipkart%20AND%20a:hbase-object-mapper&core=gav).

If you're using Gradle or Ivy or SBT, see how to include this library in your build:
[com.flipkart:hbase-object-mapper:1.18](https://mvnrepository.com/artifact/com.flipkart/hbase-object-mapper/1.18).

## How to build?
To build this project, follow below simple steps:

 1. Do a `git clone` of this repository
 2. Checkout latest stable version `git checkout v1.18`
 3. Execute `mvn clean install` from shell

### Please note:

 * Currently, systems that use this library are running on HBase 2.0. However, if you are using a different version, just change the version in [pom.xml](./pom.xml) to the desired one and build the project.
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

## Bigtable ORM
Google's [Cloud Bigtable](https://cloud.google.com/bigtable) provides first-class support for [accessing Bigtable using HBase client](https://cloud.google.com/bigtable/docs/reference/libraries#client-libraries-usage-hbase-java).

This library can be used as a **Bigtable ORM** in 3 simple steps:
1. Add following to your dependencies:
    * [bigtable-hbase-2.x](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-hbase-2.x) or [bigtable-hbase-2.x-shaded](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-hbase-2.x-shaded), depending on your requirement
    * This library
2. Instantiate HBase client's `Connection` class as below:
    ```java
    import com.google.cloud.bigtable.hbase.BigtableConfiguration;
    import org.apache.hadoop.hbase.client.Connection;
    // some code
    Connection connection = BigtableConfiguration.connect(projectId, instanceId);
    // some code
    ``` 
3. Use `Connection` instance as mentioned earlier in this README, to create your DAO class

That's it! Now you're all set to access Bigtable.

## License

Copyright 2020 Flipkart Internet Pvt Ltd.

Licensed under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0) (the "License"). You may not use this product or its source code except in compliance with the License.
