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

package org.apache.connectors.td;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.teradata.connector.common.tool.ConnectorExportTool;
import com.teradata.connector.common.tool.ConnectorImportTool;
import com.teradata.connector.teradata.db.TeradataConnection;

import org.apache.connectors.td.options.Options;
import org.apache.connectors.td.options.ParameterValidationException;
import org.apache.connectors.td.options.TeradataImportOptions;
import org.apache.connectors.td.options.TeradataExportOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.CatalogQueryManager;
import org.apache.log4j.Logger;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * The Class TeradataManager is used as the entry point for Sqoop. Sqoop uses
 * this class to instantiate all other classes/operations it needs for usage.
 * 
 * Teradata utilities run in a pluggable mechanism. The source and the target
 * are plugins that can be exchanged as needed. For example, the same source
 * plug in (Teradata) can be used with different destinations (HDFS, Hive, etc).
 * 
 * The Sqoop connector simply chooses the correct plugin to execute.
 * 
 * Initial version note: there are several layers to the TDCH API. The first
 * is the command line API. By calling a tool with command line switches, a 
 * job is instantiated with the correct source and target plugins. The command
 * line options are passed to the Job (and subsequent plugins) via Job Context.
 * Ideally, this class should configure a Job Context and submit it to the TDCH
 * job. The initial version passes command line parameters to the Job's main to
 * perform the command line switch's conversion. Ultimately, this needs to be
 * altered.
 */
@SuppressWarnings("deprecation")
public class TeradataManager extends CatalogQueryManager {

	/** The LOG Object. */
	private static final Logger LOG = Logger.getLogger(TeradataManager.class.getName());

	/** The TD_PREFIX used for prefixing options that will get passed directly
	 * into the TDCH API. For example, setting "-D tdch.example=1" will pass "1"
	 * to the "-example" command line parameter. Note the mapping from "tdch." to
	 * a dash. */
	public static final String TD_PREFIX = "tdch.";
	
	/**
	 * Instantiates a new teradata manager. Simply calls super.
	 *
	 * @param opts the opts
	 */
	public TeradataManager(SqoopOptions opts) {
		super("com.teradata.jdbc.TeraDriver", opts);
	}

	/* (non-Javadoc)
	 * @see org.apache.sqoop.manager.CatalogQueryManager#getListColumnsQuery(java.lang.String)
	 */
	@Override
	protected String getListColumnsQuery(String tableName) {
		return TeradataConnection.getListColumnsSQL(tableName, true);
	}

	/* (non-Javadoc)
	 * @see org.apache.sqoop.manager.CatalogQueryManager#getListDatabasesQuery()
	 */
	@Override
	protected String getListDatabasesQuery() {
		return TeradataConnection.getDatabaseSQL();
	}

	/* (non-Javadoc)
	 * @see org.apache.sqoop.manager.CatalogQueryManager#getListTablesQuery()
	 */
	@Override
	protected String getListTablesQuery() {
		return TeradataConnection.getListTablesSQL(null, true);
	}

	/* (non-Javadoc)
	 * @see org.apache.sqoop.manager.CatalogQueryManager#getPrimaryKeyQuery(java.lang.String)
	 */
	@Override
	protected String getPrimaryKeyQuery(String tableName) {
		return TeradataConnection.getPrimaryKeySQL(tableName, true);
	}

	/* (non-Javadoc)
	 * @see org.apache.sqoop.manager.SqlManager#checkTableImportOptions(com.cloudera.sqoop.manager.ImportJobContext)
	 */
	@Override
	protected void checkTableImportOptions(ImportJobContext context)
			throws IOException, ImportException {
		
		SqoopOptions options = context.getOptions();
		
		
		// TODO Should we be validating the req? Currently, the problem is 
		// that TD owns the validation, so there's no great way for us to 
		// validate without launching the job.
		
		super.checkTableImportOptions(context);
	}

	/* (non-Javadoc)
	 * @see org.apache.sqoop.manager.SqlManager#exportTable(com.cloudera.sqoop.manager.ExportJobContext)
	 */
	@Override
	public void exportTable(ExportJobContext context) throws IOException,
			ExportException {
		
		Configuration conf = context.getOptions().getConf();
		Iterator<Map.Entry<String,String>> i = conf.iterator();
		
		Options tdOpts = TeradataExportOptions.create();
		try {
			tdOpts.set("classname", "com.teradata.jdbc.TeraDriver");
			tdOpts.set("username", context.getOptions().getUsername());
			tdOpts.set("password", context.getOptions().getPassword());
			tdOpts.set("url", context.getOptions().getConnectString());
			tdOpts.set("targetpaths", context.getOptions().getExportDir());
			tdOpts.set("targettable", context.getOptions().getTableName());
			
			if (context.getOptions().doHiveImport() ) {
				LOG.info("Hive target set.");
				tdOpts.set("jobtype", "hive");
			}
			while (i.hasNext()) {
				Map.Entry<String,String> val = i.next();
				LOG.debug(val.getKey() + " = " + val.getValue());
				
				if (val.getKey().startsWith(TD_PREFIX)) {
						tdOpts.set(val.getKey().substring(TD_PREFIX.length()), val.getValue());
				}
			}
		} catch (ParameterValidationException e) {
			throw new IOException(e);
		}
		
		// TODO: Next version should instantiate the job and its context.

		String[] parameters = tdOpts.build();
		LOG.debug("Parameters to TD: " + join(parameters," "));
		ConnectorExportTool.main(parameters);		
	}

	/* (non-Javadoc)
	 * @see org.apache.sqoop.manager.SqlManager#importTable(com.cloudera.sqoop.manager.ImportJobContext)
	 */
	@Override
	public void importTable(ImportJobContext context) throws IOException,
			ImportException {

		Configuration conf = context.getOptions().getConf();
		Iterator<Map.Entry<String,String>> i = conf.iterator();
		
		Options tdOpts = TeradataImportOptions.create();
		try {
			tdOpts.set("classname", "com.teradata.jdbc.TeraDriver");
			tdOpts.set("username", context.getOptions().getUsername());
			tdOpts.set("password", context.getOptions().getPassword());
			tdOpts.set("url", context.getOptions().getConnectString());
			tdOpts.set("sourcetable", context.getOptions().getTableName());
			tdOpts.set("targetpaths", context.getDestination().getName());
			
			if (context.getOptions().doHiveImport() ) {
				LOG.info("Hive target set.");
				tdOpts.set("jobtype", "hive");
			}
			while (i.hasNext()) {
				Map.Entry<String,String> val = i.next();
				LOG.debug(val.getKey() + " = " + val.getValue());
				
				if (val.getKey().startsWith(TD_PREFIX)) {
						tdOpts.set(val.getKey().substring(TD_PREFIX.length()), val.getValue());
				}
			}
		} catch (ParameterValidationException e) {
			throw new IOException(e);
		}
		
		String[] parameters = tdOpts.build();
		
		LOG.debug("Parameters to TD: " + join(parameters," "));
		
		ConnectorImportTool.main(parameters);		
	}
	
	/**
	 * Perl Join function. Convert a list to a String with a separator.
	 *
	 * @param aArr the list of Strings
	 * @param sSep the separator
	 * @return the combined string
	 */
	public static String join(String[] aArr, String sSep) {
	    StringBuilder sbStr = new StringBuilder();
	    for (int i = 0, il = aArr.length; i < il; i++) {
	        if (i > 0)
	            sbStr.append(sSep);
	        sbStr.append(aArr[i]);
	    }
	    return sbStr.toString();
	}

}
