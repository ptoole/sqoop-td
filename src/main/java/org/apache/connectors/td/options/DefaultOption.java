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

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;

/**
 * The Class DefaultOption. String implementation of the Option interface. 
 * All subclasses will likely come back to this because it is a reference
 * implementation.
 * 
 */
public class DefaultOption implements Option {

	/** The name. */
	private String name;
	
	/** The value. */
	private String value;
	
	/** The default value. */
	private String defaultValue;
	
	/** The optional. */
	private boolean optional;
	
	/** The is set. */
	private boolean isSet = false;
	
	/** The configuration class. */
	private Class configurationClass;
	
	/** The config method. */
	private String configMethod;
	
	/**
	 * Instantiates a new default option.
	 *
	 * @param name the name
	 * @param defaultValue the default value
	 * @param optional the optional
	 * @param configurationClass the configuration class
	 * @param configMethod the config method
	 */
	public DefaultOption(String name, String defaultValue, boolean optional, Class configurationClass, String configMethod) {
		this.name = name;
		this.defaultValue = defaultValue;
		this.optional = optional;
		this.configurationClass = configurationClass;
	}

	/* (non-Javadoc)
	 * @see org.apache.connectors.td.options.Option#getName()
	 */
	public String getName() {
		return name;
	}

	/* (non-Javadoc)
	 * @see org.apache.connectors.td.options.Option#getValue()
	 */
	public String getValue() {
		if (isSet) {
			return value;
		} else if (!optional) {
			return defaultValue;
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.connectors.td.options.Option#setValue(java.lang.String)
	 */
	public void setValue(String value) throws ParameterValidationException {
		if (!this.isValid(value)) {
			throw new ParameterValidationException (getName() + " was set to " + value + " which is an invalid value.");
		}
		this.isSet = true;
		this.value = value;
	}
	
	/**
	 * Checks if is valid.
	 *
	 * @param value the value
	 * @return true, if is valid
	 */
	public boolean isValid(String value) {
		return true;
	}
	
	
	/**
	 * Gets the config method.
	 *
	 * @return the config method
	 */
	protected String getConfigMethod() {
		return this.configMethod;
	}
	
	/**
	 * Gets the config class.
	 *
	 * @return the config class
	 */
	protected Class getConfigClass() {
		return this.configurationClass;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.connectors.td.options.Option#configure(org.apache.hadoop.conf.Configuration)
	 */
	public void configure(Configuration confObj) throws SetupException {
		
		/* This should not be called in the initial implementation; TDCH uses
		 * different utility classes to translate the command line option to
		 * the conf object property name. These are set up as static configurations
		 * that this reflection class will process.
		 * 
		 * Again, unused in the first version.
		 */
		
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

	/* (non-Javadoc)
	 * @see org.apache.connectors.td.options.Option#hasValue()
	 */
	public boolean hasValue() {
		return (isSet) || (!optional);
	}
	
}
