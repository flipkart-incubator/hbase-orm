# HBase Object Mapper

## Introduction
This small utility library is an annotation based *object mapper* for HBase that helps you:

* convert your bean-like objects to HBase's `Put` and `Result` objects and vice-versa (for use in Map/Reduce jobs involving HBase tables and their unit-tests)
* define *Data Access Objects* for entities that map to HBase rows (for random single/bulk access of rows from an HBase table)

## Usage
Let's say you've an HBase table `citizens` with row-key format `country_code#UID` and columns like `uid`, `name` and `salary`. This library helps you define a class like below:

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

Now, using above definition, we can access rows in the HBase table as objects, either through a Map/Reduce on the table or random access.


## Map/Reduce use-cases

### Use in `map()`
See file [CitizenMapper.java](./src/test/java/com/flipkart/hbaseobjectmapper/mr/samples/CitizenMapper.java) for sample code.

### Use in `reduce()`
See file [TestCitizenMapper.java](./src/test/java/com/flipkart/hbaseobjectmapper/mr/TestCitizenMapper.java) for sample code.

### Unit-test for `map()`
See file [CitizenMapper.java](./src/test/java/com/flipkart/hbaseobjectmapper/mr/samples/CitizenMapper.java) for sample code.

### Unit-test for `reduce()`
See file [CitizenReducer.java](./src/test/java/com/flipkart/hbaseobjectmapper/mr/samples/CitizenReducer.java) for sample code.

## HBase ORM
Since we're dealing with an OLAP (and not an OLTP) system, fitting an ORM paradigm to HBase may not make sense. Nevertheless, you can use this library as an HBase-ORM too.


