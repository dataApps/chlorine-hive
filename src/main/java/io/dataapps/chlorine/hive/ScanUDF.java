/*
 * Copyright 2016, DataApps Corporation (http://dataApps.io) .
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dataapps.chlorine.hive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import io.dataapps.chlorine.finder.FinderEngine;

public class ScanUDF  extends GenericUDTF {
	static final Log LOG = LogFactory.getLog(ScanUDF.class);

	MapredContext context;
	private transient ObjectInspectorConverters.Converter[] converters;
	private static FinderEngine engine ;
	Object[] forwardObj = null;
	long totalRecords = 0;
	long totalMatches  = 0;
	long matchedRecords = 0;
	long totalSize = 0;

	public void configure(MapredContext context)  {
		this.context = context;
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		engine = new FinderEngine();

		// take care of input first
		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getCategory() == Category.PRIMITIVE) {
					converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
							PrimitiveObjectInspectorFactory.writableStringObjectInspector);
			} else  {
				converters[i] = new JsonConverter(arguments[i]);
			}
		}

		// take care of output second
		this.forwardObj = new Object[4];
		List<String> fieldNames = new ArrayList<>();
		List<ObjectInspector> fieldOIs = new ArrayList<>();

		fieldNames.add("type");
		fieldOIs.add(
				PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
						PrimitiveCategory.STRING));

		fieldNames.add("count");
		fieldOIs.add(
				PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
						PrimitiveCategory.LONG));

		fieldNames.add("fieldpos");
		fieldOIs.add(
				PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
						PrimitiveCategory.INT));

		fieldNames.add("value");
		fieldOIs.add(
				PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
						PrimitiveCategory.STRING));

		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
		
		
	}

	@Override
	public void close() throws HiveException {	
		forward("TotalRecords", totalRecords, -2, "");
		forward("TotalMatches", totalMatches, -2, "");
		forward("MatchedRecords", matchedRecords, -2, "");
		forward("TotalSize", totalSize, -2, "");
	}

	@Override
	public void process(Object[] arguments) throws HiveException {
		long matches = 0;		
		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i] != null && converters[i] != null) {
				Text value = (Text) converters[i].convert(arguments[i]);
				Map<String, List<String>> matchesByType = engine.findWithType(value.toString());
				totalSize += value.getLength();
				for (Map.Entry<String, List<String>> entry : matchesByType.entrySet()) {
					Collection<String> result = entry.getValue();
					if (result.size() > 0) {
						matches += result.size();
						forward(entry.getKey(), result.size(), i, StringUtils.join(result, ','));	
					}
				}
			}
		}
		totalRecords ++;
		if (matches > 0) {
			totalMatches += matches;
			matchedRecords ++;
		}
		
	}

	private void forward( String name, long count, int columnPos, String value)
			throws HiveException {
		forwardObj[0]= name;
		forwardObj[1]= count;
		forwardObj[2]= columnPos;
		forwardObj[3]= value;
		this.forward(forwardObj);
	}

}
