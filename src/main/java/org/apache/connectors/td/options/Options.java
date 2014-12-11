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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public abstract class Options {

	private Map<String, Option> options = new HashMap<String, Option>();
	
	protected Options() {
	}
	
	protected Options add(Option newOption) {
		options.put(newOption.getName(), newOption);
		return this;
	}
	
	public void set(String name, String value) throws ParameterValidationException {
		Option opt = options.get(name);
		if (opt == null) 
			throw new ParameterValidationException("No such option named " + name);
		opt.setValue(value);
	}
	
	public Option get(String name) {
		return options.get(name);
	}
	
	public abstract Collection<String> prepare(Option opt);

	public abstract void mapOptions() throws Exception;

	public String[] build() {
		ArrayList<String> vals = new ArrayList<String>();
		for (Option opt : options.values()) {
			if (opt.hasValue())
				vals.addAll(prepare(opt));
		}
		String[] values = new String[0];
		return vals.toArray(values);
	}
}
