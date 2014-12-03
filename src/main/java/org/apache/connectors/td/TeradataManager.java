package org.apache.connectors.td;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.teradata.connector.common.tool.ConnectorImportTool;
import com.teradata.connector.teradata.db.TeradataConnection;

import org.apache.connectors.td.options.Options;
import org.apache.connectors.td.options.ParameterValidationException;
import org.apache.connectors.td.options.TeradataImportOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.CatalogQueryManager;
import org.apache.log4j.Logger;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

@SuppressWarnings("deprecation")
public class TeradataManager extends CatalogQueryManager {

	private static final Logger LOG = Logger.getLogger(TeradataManager.class.getName());

	public static final String TD_PREFIX = "tdch.";
	
	public TeradataManager(SqoopOptions opts) {
		super("com.teradata.jdbc.TeraDriver", opts);
	}

	@Override
	protected String getListColumnsQuery(String tableName) {
		return TeradataConnection.getListColumnsSQL(tableName, true);
	}

	@Override
	protected String getListDatabasesQuery() {
		return TeradataConnection.getDatabaseSQL();
	}

	@Override
	protected String getListTablesQuery() {
		return TeradataConnection.getListTablesSQL(null, true);
	}

	@Override
	protected String getPrimaryKeyQuery(String tableName) {
		return TeradataConnection.getPrimaryKeySQL(tableName, true);
	}

	@Override
	protected void checkTableImportOptions(ImportJobContext context)
			throws IOException, ImportException {
		
		SqoopOptions options = context.getOptions();
		
		
		// TODO Auto-generated method stub
		super.checkTableImportOptions(context);
	}

	@Override
	public void exportTable(ExportJobContext context) throws IOException,
			ExportException {
		// TODO Auto-generated method stub
		super.exportTable(context);
	}

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
				// LOG.info(val.getKey() + " = " + val.getValue());
				
				if (val.getKey().startsWith(TD_PREFIX)) {
						tdOpts.set(val.getKey().substring(TD_PREFIX.length()), val.getValue());
				}
			}
		} catch (ParameterValidationException e) {
			throw new IOException(e);
		}
		
		String[] parameters = tdOpts.build();
		
		System.out.println(join(parameters," "));
		
		ConnectorImportTool.main(parameters);		
	}
	
	public static String join(String[] aArr, String sSep) {
	    StringBuilder sbStr = new StringBuilder();
	    for (int i = 0, il = aArr.length; i < il; i++) {
	        if (i > 0)
	            sbStr.append(sSep);
	        sbStr.append(aArr[i]);
	    }
	    return sbStr.toString();
	}


	@Override
	protected void initOptionDefaults() {
		// TODO Auto-generated method stub
		super.initOptionDefaults();
	}

	
	public static void main (String[] args) {
		System.out.println("before");
		for (String arg : args) {
			System.out.println(arg);
		}
		System.out.println("after");
	}

}
