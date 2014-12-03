package org.apache.connectors.td.options;

import org.apache.hadoop.conf.Configuration;

public interface Option {

	public String getName();
	
	public String getValue();

	public void setValue(String value) throws ParameterValidationException;

	public void configure(Configuration confObj) throws SetupException;
	
	public boolean hasValue();
}
