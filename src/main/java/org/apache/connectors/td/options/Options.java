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
	
	public String[] build() {
		ArrayList<String> vals = new ArrayList<String>();
		for (Option opt : options.values()) {
			System.out.println("Preparing " + opt.getName() + " " + opt.hasValue() + " " + opt.getValue());
			if (opt.hasValue())
				vals.addAll(prepare(opt));
		}
		String[] values = new String[0];
		return vals.toArray(values);
	}
}
