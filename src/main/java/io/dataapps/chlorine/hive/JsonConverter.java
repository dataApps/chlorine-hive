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

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class JsonConverter  implements Converter {
	static final Log LOG = LogFactory.getLog(JsonConverter.class);

    private InspectorHandle inspHandle;
    private static JsonFactory jsonFactory = new JsonFactory(); // threadsafe


    private interface InspectorHandle {
        abstract public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException;
    }

    private class MapInspectorHandle implements InspectorHandle {
        private MapObjectInspector mapInspector;
        private StringObjectInspector keyObjectInspector;
        private InspectorHandle valueInspector;


        public MapInspectorHandle(MapObjectInspector mInsp) throws UDFArgumentException {
            mapInspector = mInsp;
            try {
                keyObjectInspector = (StringObjectInspector) mInsp.getMapKeyObjectInspector();
            } catch (ClassCastException castExc) {
                throw new UDFArgumentException("Only Maps with strings as keys can be converted to valid JSON");
            }
            valueInspector = generateInspectorHandle(mInsp.getMapValueObjectInspector());
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                Map map = mapInspector.getMap(obj);
                Iterator<Map.Entry> iter = map.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = iter.next();
                    String keyJson = keyObjectInspector.getPrimitiveJavaObject(entry.getKey());
                    gen.writeFieldName(keyJson);
                    valueInspector.generateJson(gen, entry.getValue());
                }
                gen.writeEndObject();
            }
        }

    }

    private class StructInspectorHandle implements InspectorHandle {
        private StructObjectInspector structInspector;
        private List<String> fieldNames;
        private List<InspectorHandle> fieldInspectorHandles;

        public StructInspectorHandle(StructObjectInspector insp) throws UDFArgumentException {
            structInspector = insp;
            List<? extends StructField> fieldList = insp.getAllStructFieldRefs();
            this.fieldNames = new ArrayList<String>();
            this.fieldInspectorHandles = new ArrayList<InspectorHandle>();
            for (StructField sf : fieldList) {
                fieldNames.add(sf.getFieldName());
                fieldInspectorHandles.add(generateInspectorHandle(sf.getFieldObjectInspector()));
            }
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            //// Interpret a struct as a map ...
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                List structObjs = structInspector.getStructFieldsDataAsList(obj);

                for (int i = 0; i < fieldNames.size(); ++i) {
                    String fieldName = fieldNames.get(i);
                    gen.writeFieldName(fieldName);
                    fieldInspectorHandles.get(i).generateJson(gen, structObjs.get(i));
                }
                gen.writeEndObject();
            }
        }

    }

    private class ArrayInspectorHandle implements InspectorHandle {
        private ListObjectInspector arrayInspector;
        private InspectorHandle valueInspector;


        public ArrayInspectorHandle(ListObjectInspector lInsp) throws UDFArgumentException {
            arrayInspector = lInsp;
            valueInspector = generateInspectorHandle(arrayInspector.getListElementObjectInspector());
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartArray();
                List list = arrayInspector.getList(obj);
                for (Object listObj : list) {
                    valueInspector.generateJson(gen, listObj);
                }
                gen.writeEndArray();
            }
        }

    }

    private class StringInspectorHandle implements InspectorHandle {
        private StringObjectInspector strInspector;


        public StringInspectorHandle(StringObjectInspector insp) {
            strInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                String str = strInspector.getPrimitiveJavaObject(obj);
                gen.writeString(str);
            }
        }

    }

    private class IntInspectorHandle implements InspectorHandle {
        private IntObjectInspector intInspector;

        public IntInspectorHandle(IntObjectInspector insp) {
            intInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null)
                gen.writeNull();
            else {
                int num = intInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class DoubleInspectorHandle implements InspectorHandle {
        private DoubleObjectInspector dblInspector;

        public DoubleInspectorHandle(DoubleObjectInspector insp) {
            dblInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                double num = dblInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class LongInspectorHandle implements InspectorHandle {
        private LongObjectInspector longInspector;

        public LongInspectorHandle(LongObjectInspector insp) {
            longInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                long num = longInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class ShortInspectorHandle implements InspectorHandle {
        private ShortObjectInspector shortInspector;

        public ShortInspectorHandle(ShortObjectInspector insp) {
            shortInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                short num = shortInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class ByteInspectorHandle implements InspectorHandle {
        private ByteObjectInspector byteInspector;

        public ByteInspectorHandle(ByteObjectInspector insp) {
            byteInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                byte num = byteInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class FloatInspectorHandle implements InspectorHandle {
        private FloatObjectInspector floatInspector;

        public FloatInspectorHandle(FloatObjectInspector insp) {
            floatInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                float num = floatInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class BooleanInspectorHandle implements InspectorHandle {
        private BooleanObjectInspector boolInspector;

        public BooleanInspectorHandle(BooleanObjectInspector insp) {
            boolInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                boolean tf = boolInspector.get(obj);
                gen.writeBoolean(tf);
            }
        }
    }

    private class BinaryInspectorHandle implements InspectorHandle {
        private BinaryObjectInspector binaryInspector;

        public BinaryInspectorHandle(BinaryObjectInspector insp) {
            binaryInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                byte[] bytes = binaryInspector.getPrimitiveJavaObject(obj);
                gen.writeBinary(bytes);
            }
        }
    }

    private class TimestampInspectorHandle implements InspectorHandle {
        private TimestampObjectInspector timestampInspector;
        private DateTimeFormatter isoFormatter = ISODateTimeFormat.dateTimeNoMillis();

        public TimestampInspectorHandle(TimestampObjectInspector insp) {
            timestampInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws JsonGenerationException, IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                Timestamp timestamp = timestampInspector.getPrimitiveJavaObject(obj);
                String timeStr = isoFormatter.print(timestamp.getTime());
                gen.writeString(timeStr);
            }
        }
    }


    private InspectorHandle generateInspectorHandle(ObjectInspector insp) throws UDFArgumentException {
        Category cat = insp.getCategory();
        if (cat == Category.MAP) {
            return new MapInspectorHandle((MapObjectInspector) insp);
        } else if (cat == Category.LIST) {
            return new ArrayInspectorHandle((ListObjectInspector) insp);
        } else if (cat == Category.STRUCT) {
            return new StructInspectorHandle((StructObjectInspector) insp);
        } else if (cat == Category.PRIMITIVE) {
            PrimitiveObjectInspector primInsp = (PrimitiveObjectInspector) insp;
            PrimitiveCategory primCat = primInsp.getPrimitiveCategory();
            if (primCat == PrimitiveCategory.STRING) {
                return new StringInspectorHandle((StringObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.INT) {
                return new IntInspectorHandle((IntObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.LONG) {
                return new LongInspectorHandle((LongObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.SHORT) {
                return new ShortInspectorHandle((ShortObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.BOOLEAN) {
                return new BooleanInspectorHandle((BooleanObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.FLOAT) {
                return new FloatInspectorHandle((FloatObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.DOUBLE) {
                return new DoubleInspectorHandle((DoubleObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.BYTE) {
                return new ByteInspectorHandle((ByteObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.BINARY) {
                return new BinaryInspectorHandle((BinaryObjectInspector) primInsp);
            } else if (primCat == PrimitiveCategory.TIMESTAMP) {
                return new TimestampInspectorHandle((TimestampObjectInspector) primInsp);
            }
        }
        throw new UDFArgumentException("Don't know how to handle object inspector " + insp);
    }
    
    
    public JsonConverter (ObjectInspector insp) {
    	try {
			inspHandle = generateInspectorHandle(insp);
		} catch (UDFArgumentException e) {
            LOG.error(e);
		}
    }


	@Override
	public Object convert(Object arg0) {
		try {
            StringWriter writer = new StringWriter();
            JsonGenerator gen = jsonFactory.createJsonGenerator(writer);
            inspHandle.generateJson(gen, arg0);
            gen.close();
            writer.close();
            return new Text(writer.toString());
        } catch (IOException e) {
            LOG.error(e);
            return null;
        }
	}


}
