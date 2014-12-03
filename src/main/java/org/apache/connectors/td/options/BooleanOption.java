package org.apache.connectors.td.options;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;

public class BooleanOption extends EnumOption {

	public BooleanOption(String name, boolean defaultValue, boolean optional, Class configurationClass, String configMethod) {
		super(name, new Boolean(defaultValue).toString(), optional, configurationClass, configMethod, "true", "false");
	}
	
	@Override
	public void configure(Configuration confObj) throws SetupException {
		if (this.getValue() != null) {
			Method method;
			try {
				method = this.getConfigClass().getMethod(this.getConfigMethod(), this.getConfigClass());
				Object o = method.invoke(null, confObj, new Boolean(getValue()).booleanValue());		
			} catch (Exception e) {
				throw new SetupException("Config could not find method " + this.getConfigMethod() + " in " + this.getConfigClass().getCanonicalName(), e);
			}
		}
	}

}
