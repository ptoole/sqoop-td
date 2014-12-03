package org.apache.connectors.td.options;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;

public class NumberOption extends DefaultOption {

	public NumberOption(String name, Integer defaultValue, boolean optional, Class configurationClass, String configMethod) {
		super(name, defaultValue.toString(), optional, configurationClass, configMethod);
	}

	@Override
	public boolean isValid(String value) {
		try {
			new Integer(value);
			return true;
		} catch (NumberFormatException e) {}
		return false;
	}

	@Override
	public void configure(Configuration confObj) throws SetupException {
		if (this.getValue() != null) {
			Method method;
			try {
				method = this.getConfigClass().getMethod(this.getConfigMethod(), this.getConfigClass());
				method.invoke(null, confObj, new Integer(getValue()).intValue());		
			} catch (Exception e) {
				throw new SetupException("Config could not find method " + this.getConfigMethod() + " in " + this.getConfigClass().getCanonicalName(), e);
			}
		}
	}

}
