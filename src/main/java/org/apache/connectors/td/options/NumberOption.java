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
