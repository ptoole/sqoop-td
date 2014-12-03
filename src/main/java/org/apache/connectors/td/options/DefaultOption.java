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
