
Sqoop Connector for Teradata 
============================

As the development of the integration between Teradata and Hadoop continues to
deepen, the need to use Hadoop centric operations increase. The need to leverage
a small subset of TDCH's import and export capabilities into Sqoop becomes 
prevalent in field usage.

Installation
------------

1. Install sqoop. For a MapR distribution, run "yum install mapr-sqoop" from
one of the nodes as the root user.

2. Install the Teradata jars in the sqoop lib.

	- MapReduce v1:
	
		1. For MRv1, install the XXXX.rpm
	
		2. Move tdgssconfig.jar, teradata-connector-1.3.3.jar, and terajdbc4.jar
		into the /opt/mapr/sqoop/sqoop-*/lib directory
	
		3. If you plan to use the Hive integration features, link the following 
		jars into the /opt/mapr/sqoop/sqoop-*/lib directory:
	
			- /opt/mapr/hive/hive-*/lib/hive-cli-*.jar
			- /opt/mapr/hive/hive-*/lib/hive-common-*.jar
			- /opt/mapr/hive/hive-*/lib/hive-exec-*.jar
			- /opt/mapr/hive/hive-*/lib/hive-metastore-*.jar

	- MapReduce v2:
	
3. Install the sqoop-connector-td-*.jar into the sqoop lib.


Usage
-----


# Mappings 
