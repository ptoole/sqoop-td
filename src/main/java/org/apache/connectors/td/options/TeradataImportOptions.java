/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.connectors.td.options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.orm.AvroSchemaGenerator;

import org.apache.avro.Schema;
import org.apache.connectors.td.TeradataManager;
import org.apache.connectors.td.types.TeradataDataType;

@SuppressWarnings("deprecation")
public class TeradataImportOptions extends Options {

	private ImportJobContext context;

	public TeradataImportOptions(ImportJobContext context) {
		this.context = context;
	}

	public static Options create(ImportJobContext context) {
		return new TeradataImportOptions(context)
			.add(new EnumOption("jobtype", "hdfs", false, null, null, "hdfs", "hive", "hcat"))
			.add(new EnumOption("fileformat", "textfile", false, null, null, "sequencefile", "textfile", "avrofile", "orcfile", "rcfile"))
			.add(new DefaultOption("method", "", true, null, null))
			.add(new DefaultOption("jobclientoutput", "", true, null, null))
			.add(new DefaultOption("nummappers", "", true, null, null))
			.add(new DefaultOption("numreducers", "", true, null, null))
			.add(new DefaultOption("debugoption", "", true, null, null))
			
			.add(new DefaultOption("sourcerecordschema", "", true, null, null))
			.add(new DefaultOption("targetrecordschema", "", true, null, null))

			.add(new DefaultOption("sourcedateformat", "", true, null, null))
			.add(new DefaultOption("targetdateformat", "", true, null, null))
			.add(new DefaultOption("sourcetimeformat", "", true, null, null))
			.add(new DefaultOption("targettimeformat", "", true, null, null))
			.add(new DefaultOption("sourcetimestampformat", "", true, null, null))
			.add(new DefaultOption("targettimestampformat", "", true, null, null))
			.add(new DefaultOption("sourcetimezoneid", "", true, null, null))
			.add(new DefaultOption("targettimezoneid", "", true, null, null))

			.add(new DefaultOption("classname", "", true, null, null))
			.add(new DefaultOption("url", "", true, null, null))
			.add(new DefaultOption("username", "", true, null, null))
			.add(new DefaultOption("password", "", true, null, null))

			.add(new NumberOption("batchsize", 10000, true, null, null))
			.add(new DefaultOption("queryband", "", true, null, null))
			.add(new BooleanOption("usexview", false, true, null, null))
			
			.add(new DefaultOption("separator", "", true, null, null))
			.add(new DefaultOption("lineseparator", "", true, null, null))
			.add(new DefaultOption("enclosedby", "", true, null, null))
			.add(new DefaultOption("escapedby", "", true, null, null))
			.add(new DefaultOption("nullstring", "", true, null, null))
			.add(new DefaultOption("nullnonstring", "", true, null, null))

			.add(new DefaultOption("avroschema", "", true, null, null))
			.add(new DefaultOption("avroschemafile", "", true, null, null))
			.add(new DefaultOption("hiveconf", "", true, null, null))

			.add(new DefaultOption("sourcedatabase", "", true, null, null))
			.add(new DefaultOption("sourcetable", "", true, null, null))
			.add(new DefaultOption("targetpaths", "", true, null, null))
			.add(new DefaultOption("targettableschema", "", true, null, null))
			.add(new DefaultOption("sourcepartitionschema", "", true, null, null))
			.add(new DefaultOption("sourcefieldnames", "", true, null, null))
			.add(new DefaultOption("sourcequery", "", true, null, null))
			.add(new DefaultOption("sourceconditions", "", true, null, null))
			.add(new DefaultOption("splitbycolumn", "", true, null, null))
			.add(new DefaultOption("forcestage", "", true, null, null))
			.add(new DefaultOption("stagedatabase", "", true, null, null))
			.add(new DefaultOption("stagetablename", "", true, null, null))
			.add(new BooleanOption("accesslock", false, true, null, null))
			.add(new DefaultOption("numpartitionsinstaging", "", true, null, null))

			;
	}

	@Override
	public ArrayList<String> prepare(Option opt) {
		ArrayList<String> vals = new ArrayList<String>();
		vals.add("-" + opt.getName());
		vals.add(opt.getValue());
		return  vals;
	}
	
	public void mapGenericOptions() throws ParameterValidationException {
		// Hive conf? There doesnt seem to be a way to find the conf file from Sqoop
		if (context.getOptions().getClassName() == null) 
			this.set("classname", "com.teradata.jdbc.TeraDriver");
		
		this.set("username", context.getOptions().getUsername());
		this.set("password", context.getOptions().getPassword());
		this.set("url", context.getOptions().getConnectString());
		
		if (context.getOptions().getNumMappers() > 0) {
			this.set("nummappers", 
					new Integer(context.getOptions().getNumMappers()).toString());
		}
		
		if (context.getOptions().getConf().get("mapred.reduce.tasks") != null) {
			this.set("numreducers", 
					context.getOptions().getConf().get("mapred.reduce.tasks") );
		}
	}
	
	public void mapImportOptions() throws Exception {
		this.set("sourcetable", context.getOptions().getTableName());
		this.set("targetpaths", context.getDestination().getName());
		
		if (context.getOptions().doHiveImport()) {
			this.set("jobtype", "hive");
			
			if (this.get("targettableschema").hasValue() == false) {
				System.out.println("SCHEMA SCHEMA SCHEMA");

				// Build the Schema from the TD table
				TeradataConnection c = new TeradataConnection(
						"com.teradata.jdbc.TeraDriver",
						context.getOptions().getConnectString(),
						context.getOptions().getUsername(),
						context.getOptions().getPassword(),
		                true);
				c.connect();
				
				TeradataColumnDesc[] descriptors = c.getColumnDescsForTable(context.getTableName(), 
							c.getColumnNamesForTable(context.getTableName()));
				
				ArrayList<String> schema = new ArrayList<String>();
				for (TeradataColumnDesc desc : descriptors) {
					try {
						schema.add(desc.getName() + " " + 
								TeradataDataType.find(desc.getType()).getHiveDataType().toString());
					} catch (NullPointerException e) {
						throw new ParameterValidationException("Unsupported data type " + desc.getTypeName() + " [" + desc.getType() + "] for column " + desc.getName());
					}
				}
				
				this.set("targettableschema", TeradataManager.join(schema.toArray(new String[1]), ","));
				
				c.close();
			}
		}
		/*
		if (context.getOptions().getFileLayout() 
				== SqoopOptions.FileLayout.AvroDataFile) {
			
			ConnManager connManager = context.getConnManager();
			AvroSchemaGenerator generator = new AvroSchemaGenerator(context.getOptions(),
					connManager, context.getTableName());
			Schema schema = generator.generate();

			this.set("avroschema", schema.toString());
		}
		
		if (context.getOptions().getOutputEnclosedBy() != 0) 
			this.set("enclosedby", String.valueOf(context.getOptions().getOutputEnclosedBy()) );
		
		if (context.getOptions().getOutputFieldDelim() != 0) 
			this.set("separator", String.valueOf(context.getOptions().getOutputFieldDelim()) );
		
		if (context.getOptions().getOutputRecordDelim() != 0) 
			this.set("lineseparator", String.valueOf(context.getOptions().getOutputRecordDelim()) );
				
		if (context.getOptions().getOutputEscapedBy() != 0) 
			this.set("escapedby", String.valueOf(context.getOptions().getOutputEscapedBy()) );
		*/
	}
	
	public void mapOptions() throws Exception {		
		mapGenericOptions();
		mapImportOptions();
	}

}
