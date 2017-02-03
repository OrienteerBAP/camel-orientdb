# camel-orientdb
Camel component for OrientDB. For Camel since ***2.17.3*** and later.
For injecting Maven depedency use this code:

```
<dependency>
	<groupId>org.orienteer.camel.component</groupId>
	<artifactId>camel-orientdb</artifactId>
    <version>1.0-SNAPSHOT</version>
    <!-- Now we have just this version. It can be changed later. -->
</dependency>
```

##Initialization

DB connection variables can be set using CamelContext properties, like 
```
	context = new DefaultCamelContext();
	Map<String, String> properties = context.getProperties();
	properties.put(OrientDBComponent.DB_URL, dbSettings.getDBUrl());
	properties.put(OrientDBComponent.DB_USERNAME, session.getUsername());
	properties.put(OrientDBComponent.DB_PASSWORD, session.getPassword());
	context.setProperties(properties);
```

Also can see http://camel.apache.org/properties.html

Default properties names is

```
	DB_URL="orientdb.url"; 
	DB_USERNAME="orientdb.username"; 
	DB_PASSWORD="orientdb.password"; 
```
 
But it can be change


##URI format

The OrientDB component using following endpoint URI notation

```
orientdb:[OrientDBSQLQuery][?options]
```

And using OrientDB query format with minor changes:

- Instead ***?***  used ***:#*** for anonymous input parameters. Named parameters using OrientDB notation (:name) 

Example:
```
orientdb:select from testTable where name=:# 
```

Some URIs may not use queryes, only options, like: 

```
orientdb:?preload=true&makeNew=true 
```

This endpoint just load input Map`s and trying to create new objects from them.

##Options

|Name 	|Kind 	|Group 	|Required 	|Default 	|Type 	|Enum 	|Description|
|---|---|---|---|---|---|---|---|
|sqlQuery| 	path 	|common 	|false 		| |java.lang.String | 		|Sets the query to execute
|classField 	|parameter 	|common|false  		|class 	|java.lang.String | 		|Your "@class" renamed to classField value
|fetchAllEmbedded 	|parameter 	|common|false  		|true 	|boolean | 		|Fetch all embedded(not linked) objects, ignore "maxDepth". Only for "map" type.
|fetchPlan 	|parameter 	|common|false  			| |java.lang.String | 		|Set fetch plan (view OrientDB documentation, like http://orientdb.com/docs/2.0/orientdb.wiki/Fetching-Strategies.html)
|maxDepth 	|parameter 	|common|false 		|0 	|int | 		|Max fetch depth. Only for "map" type
|outputType 	|parameter 	|common|false  		|map 	|org.orienteer.camel.component.OrientDBCamelDataType |map/object/json/list 	|Output data type of single row.
|recordIdField 	|parameter 	|common|false 		|rid 	|java.lang.String 		| |Your "@rid" renamed to recordIdField value
|inputAsOClass 	|parameter 	|consumer|false 		| |java.lang.String 		| |Rewrite "@class" field value in root document(s)
|makeNew 	|parameter 	|consumer|false  		|true 	|boolean 		| |Clear ODocuments RID`s in PRELOAD phase BEFORE save.Works only if ***preload=true***
|preload 	|parameter 	|consumer|false 		|false 	|boolean 		| |Trying to save ODocument from input data BEFORE query

##Input

***NOTE*** All first-level lists split to elements and OrientDBSQLQuery evals for EVERY element 
If you need process only one parametrized element, use Map, ODocument or List<List> with one element in

|Type|Transfer to query input as|Can be preloaded|
|---|---|---|
|ODocument|Map<String,Object>|true
|Map<String,Object>|Map<String,Object>|true
|List<ODocument>|Map<String,Object>|true
|List<Map<String,Object>>|Map<String,Object>|true
|List<Object>|Object|false

##Output

***NOTE*** All output conversions are recursive, or convert output into flat structures
***IMPORTANT NOTE*** If result of sql query is list and before you sent List<ODocument>(as example) to input, then results of all queries will be joined into one big List  

|outputType/query result|ODocument|List<ODocument>|Object|
|---|---|---|---|
|map|Map<String,Object>|List<Map<String,Object>>|Object
|object|ODocument|List<ODocument>|Object
|json|(String)OrientDB Json|List<(String)OrientDB Json>|Object
|list|List<String>|List<List<String>>|Object







