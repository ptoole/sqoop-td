package org.apache.connectors.td;

import org.apache.sqoop.manager.DefaultManagerFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.metastore.JobData;

@SuppressWarnings("deprecation")
public class TeradataManagerFactory extends DefaultManagerFactory {

	@Override
	public ConnManager accept(JobData data) {
	    SqoopOptions options = data.getSqoopOptions();
	    String scheme = extractScheme(options);

	    if (scheme.startsWith("jdbc:teradata")) {
	      return new TeradataManager(options);
	    }

	    return null;
	}	
	
}
