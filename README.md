# chlorine-hive
*Detect sensitive data in Hive using chlorine-finder*.

The chlorine-hive library defines a UDF which can be used to detect sensitive elements in Hive tables. It is Java based.

Chlorine-hive can detect different types of Credit card numbers, SSN, Phone Numbers, email adddresses, Ip Addresses, Street Addresses in Hive tables.


###To Download source code

*git clone https://github.com/dataApps/chlorine-hive.git*

###To build chlorine-hive

*mvn install*

###To use Chlorine-hive

- The following hive code snippet will show the statements to use the hive UDF. 

```
 hive> ADD JAR  /path/to/chlorine-finder-1.1.5.jar;
Added [/path/to/chlorine-finder-1.1.5.jar]
Added resources: [/path/to/chlorine-finder-1.1.5.jar]

hive> ADD JAR  /path/to/chlorine-hive-1.1.5.jar;
Added [/path/to/chlorine-hive-1.1.5.jar] to class path
Added resources: [/path/to/chlorine-hive-1.1.5.jar]

hive> CREATE TEMPORARY FUNCTION scan AS 'io.dataapps.chlorine.hive.ScanUDF';
OK
Time taken: 0.602 seconds

hive> select * from userinfo_avro ;
OK
1	dsdsdsi	tests@tetts.com	123.34.456.23	608-34-2345	1234 sdsfd dr san jose ca 45454	sssss	fsfsfsf	gdgdd	dgdgdddds	2015	8	31
2	dgddg	teret@dfdfd.com	123.33.234.13	604-13-4345	1234 gsfsffs pl san ramon ca 43435	dsdffdd	dgdfdfd	ddgdgdg	dddsdss	2015	8	31
3	three	three@three.com	131.34.456.23	609-34-2345	1234 sdsfd dr san rafael ca 45454	sssss	fsfsfsf	gdgdd	dgdgdddds	2015	9	1
4	four	four@four.com	153.33.234.13	607-13-4345	1234 gs pl san ana ca 43435	dsdffdd	dgdfdfd	ddgdgdg	dddsdss	2015	9	1
Time taken: 0.427 seconds, Fetched: 4 row(s)

hive> select scan(*) from table;
OK
Email	1	2	tests@tetts.com
IPV4	1	3	123.34.456.23
SSN-dashes	1	4	608-34-2345
Street Address	1	5	1234 sdsfd dr
Email	1	2	teret@dfdfd.com
IPV4	1	3	123.33.234.13
SSN-dashes	1	4	604-13-4345
Street Address	1	5	1234 gsfsffs pl
Email	1	2	three@three.com
IPV4	1	3	131.34.456.23
SSN-dashes	1	4	609-34-2345
Street Address	1	5	1234 sdsfd dr
Email	1	2	four@four.com
IPV4	1	3	153.33.234.13
SSN-dashes	1	4	607-13-4345
Street Address	1	5	1234 gs pl
Time taken: 0.628 seconds, Fetched: 16 row(s)

```

### Explanation of the output

One row will be emitted for each column value containing a sensitive element. If multiple types of sensitive elements are found in a column value, then a row is emitted per type per column.
The out schema is as follows:

| column position | name | description |
| --- | --- |--- | 
|1|Type| sensitive data type. eg: email, credit card. This will be the name of the finder|
|2|count| number of sensitive elements found in a specific column of a specific sensitive element type|
|3|field position|the column position in the Hive table|
|4|values| comma spearated list of sensitive data values found|


### Download library jar

The latest chlorine-finder and chlorine-hive libraries can be downloaded [here](https://dataapps.io/chlorine.html#Opensource).
 
###Further Documentation
[chlorine-hive wiki](https://github.com/dataApps/chlorine-hive/wiki)
  
###Related projects
 
###Java Docs
The java docs for chlorine-finder are available [here](https://dataApps.io/files/chlorine-hive/javadoc/index.html).

We welcome all contributions. You can contribute features, enhancements, bug fixes or new Finders.

##Want to contribute features, bug fixes, enhancements?

    Fork it
    Create your feature branch (git checkout -b my-new-feature)
    Take a look at the issues
    Commit your changes (git commit -am 'Add some feature')
    Push to the branch (git push origin my-new-feature)
    Create new Pull Request
    
 

 
 
