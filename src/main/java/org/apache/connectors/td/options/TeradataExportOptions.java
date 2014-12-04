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

public class TeradataExportOptions extends Options {

	public static Options create() {
		return new TeradataExportOptions()
			.add(new EnumOption("jobtype", "hdfs", false, null, null, "hdfs", "hive", "hcat"))
			.add(new EnumOption("fileformat", "textfile", false, null, null, "sequencefile", "textfile", "avrofile", "orcfile", "rcfile"))
			.add(new DefaultOption("classname", "", true, null, null))
			.add(new DefaultOption("url", "", true, null, null))
			.add(new DefaultOption("username", "", true, null, null))
			.add(new DefaultOption("password", "", true, null, null))
			.add(new NumberOption("batchsize", 10000, true, null, null))
			.add(new BooleanOption("accesslock", false, true, null, null))
			.add(new DefaultOption("queryband", "", true, null, null))
			.add(new DefaultOption("targetpaths", "", true, null, null))
			.add(new DefaultOption("sourcetable", "", true, null, null))
			.add(new DefaultOption("sourceconditions", "", true, null, null))
			.add(new DefaultOption("sourcefieldnames", "", true, null, null))
			.add(new DefaultOption("sourcerecordschema", "", true, null, null))
			.add(new DefaultOption("targetrecordschema", "", true, null, null))
			.add(new DefaultOption("sourcequery", "", true, null, null))
			.add(new DefaultOption("sourcecountquery", "", true, null, null))
			.add(new DefaultOption("targetdatabase", "", true, null, null))
			.add(new DefaultOption("targettable", "", true, null, null))
			.add(new DefaultOption("targetfieldnames", "", true, null, null))
			.add(new DefaultOption("targettableschema", "", true, null, null))
			.add(new DefaultOption("targetpartitionschema", "", true, null, null))
			.add(new DefaultOption("separator", "", true, null, null))
			.add(new DefaultOption("lineseparator", "", true, null, null))
			.add(new DefaultOption("enclosedby", "", true, null, null))
			.add(new DefaultOption("escapedby", "", true, null, null))
			.add(new DefaultOption("nullstring", "", true, null, null))
			.add(new DefaultOption("nullnonstring", "", true, null, null))
			.add(new DefaultOption("method", "", true, null, null))
			.add(new DefaultOption("nummappers", "", true, null, null))
			.add(new DefaultOption("splitbycolumn", "", true, null, null))
			.add(new DefaultOption("forcestage", "", true, null, null))
			.add(new DefaultOption("stagetablename", "", true, null, null))
			.add(new DefaultOption("stagedatabase", "", true, null, null))
			.add(new DefaultOption("numpartitionsinstaging", "", true, null, null))
			.add(new DefaultOption("hiveconf", "", true, null, null))
			.add(new BooleanOption("usexview", false, true, null, null))
			.add(new DefaultOption("avroschema", "", true, null, null))
			.add(new DefaultOption("avroschemafile", "", true, null, null))
			.add(new DefaultOption("debugoption", "", true, null, null))
			.add(new DefaultOption("sourcedateformat", "", true, null, null))
			.add(new DefaultOption("targetdateformat", "", true, null, null))
			.add(new DefaultOption("sourcetimeformat", "", true, null, null))
			.add(new DefaultOption("targettimeformat", "", true, null, null))
			.add(new DefaultOption("sourcetimestampformat", "", true, null, null))
			.add(new DefaultOption("targettimestampformat", "", true, null, null))
			.add(new DefaultOption("sourcetimezoneid", "", true, null, null))
			.add(new DefaultOption("targettimezoneid", "", true, null, null))
			;
	}

	@Override
	public ArrayList<String> prepare(Option opt) {
		ArrayList<String> vals = new ArrayList<String>();
		vals.add("-" + opt.getName());
		vals.add(opt.getValue());
		return  vals;
	}

}
