package org.apache.connectors.td.options;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;

public class EnumOption extends DefaultOption {

	String[] values;
	
	public EnumOption(String name, String defaultValue, boolean optional, Class configurationClass, String configMethod, String... values) {
		super(name, defaultValue, optional, configurationClass, configMethod);
		this.values = values;
	}
	
	@Override
	public boolean isValid(String value) {
		for (String val:values) {
			if(val.equals(value))
				return true;
		}
		return false;
	}
	

}
