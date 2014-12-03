package org.apache.connectors.td.options;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;

public class DefaultOption implements Option {

	private String name;
	private String value;
	private String defaultValue;
	private boolean optional;
	private boolean isSet = false;
	private Class configurationClass;
	private String configMethod;
	
	public DefaultOption(String name, String defaultValue, boolean optional, Class configurationClass, String configMethod) {
		this.name = name;
		this.defaultValue = defaultValue;
		this.optional = optional;
		this.configurationClass = configurationClass;
	}

	public String getName() {
		return name;
	}

	public String getValue() {
		if (isSet) {
			return value;
		} else if (!optional) {
			return defaultValue;
		}
		return null;
	}
	
	public void setValue(String value) throws ParameterValidationException {
		if (!this.isValid(value)) {
			throw new ParameterValidationException (getName() + " was set to " + value + " which is an invalid value.");
		}
		this.isSet = true;
		this.value = value;
	}
	
	public boolean isValid(String value) {
		return true;
	}
	
	
	protected String getConfigMethod() {
		return this.configMethod;
	}
	
	protected Class getConfigClass() {
		return this.configurationClass;
	}
	
	public void configure(Configuration confObj) throws SetupException {
		if (this.getValue() != null) {
			Method method;
			try {
				method = this.getConfigClass().getMethod(this.getConfigMethod(), this.getConfigClass());
				Object o = method.invoke(null, confObj, getValue());		
			} catch (Exception e) {
				throw new SetupException("Config could not find method " + this.getConfigMethod() + " in " + this.getConfigClass().getCanonicalName(), e);
			}
		}
	}

	public boolean hasValue() {
		return (isSet) || (!optional);
	}
	
}
