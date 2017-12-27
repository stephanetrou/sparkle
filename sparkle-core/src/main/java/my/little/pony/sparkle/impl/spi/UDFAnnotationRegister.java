/*
 * Copyright 2017 stephanetrou
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package my.little.pony.sparkle.impl.spi;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.CaseFormat;
import my.little.pony.sparkle.impl.spi.util.annotation.AnnotationScanner;
import my.little.pony.sparkle.job.JobContext;
import my.little.pony.sparkle.spi.Register;
import my.little.pony.sparkle.udf.UDF;
import my.little.pony.sparkle.udf.UDFReturnType;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import my.little.pony.sparkle.Sparkle;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class UDFAnnotationRegister implements Register {

    private final static Logger LOGGER = LogManager.getLogger(UDFAnnotationRegister.class);
    private final static Map<Class, DataType> MAP_TYPES;

    static {
        Map<Class, DataType> map = new HashMap<>();

        map.put(String.class, DataTypes.StringType);
        //map.put(java.sql.Blob.class, DataTypes.BinaryType);
        map.put(Boolean.class, DataTypes.BooleanType);
        map.put(java.sql.Date.class, DataTypes.DateType);
        map.put(java.sql.Timestamp.class, DataTypes.TimestampType);
        map.put(java.util.Calendar.class, DataTypes.CalendarIntervalType);
        map.put(Double.class, DataTypes.DoubleType);
        map.put(Float.class, DataTypes.FloatType);
        map.put(Byte.class, DataTypes.ByteType);
        map.put(Integer.class, DataTypes.IntegerType);
        map.put(Long.class, DataTypes.LongType);
        map.put(Short.class, DataTypes.ShortType);
        map.put(Void.class, DataTypes.NullType);

        MAP_TYPES = Collections.unmodifiableMap(map);

    }

    @Override
    public void register(JobContext jobContext) {
        AnnotationScanner annotationScanner = new AnnotationScanner(jobContext.getJobClass());

        try {
            Set<Class<?>> classes = annotationScanner.scan(UDF.class, jobContext);

            for(Class<?> clasz : classes) {
                registerUDF(clasz);
            }

        } catch (IOException e) {
            LOGGER.error("Error while registering UDF ", e);
        }

    }

    private void registerUDF(Class<?> clasz) {
        UDF udf = clasz.getAnnotation(UDF.class);
        DataType dataType = null;

        if (udf.type().equals(UDFReturnType.Undefined)) {
            for (Method m : clasz.getDeclaredMethods()) {
                if ("call".equals(m.getName()) && !m.getReturnType().equals(Object.class)) {
                    dataType = map(m.getReturnType());
                    break;
                }
            }
        } else {
            dataType = map(udf.type());
        }
        String name = udf.name();

        if (StringUtils.isBlank(name)) {
            name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, clasz.getSimpleName());
        }

        LOGGER.info(String.format("Registering udf %s (from %s) returning %s", name, clasz, dataType));

        Sparkle.spark().udf().registerJava(name, clasz.getName(), dataType);
    }

    private static DataType map(UDFReturnType type) {
        DataType dataType = DataTypes.NullType;

        switch (type) {
            case String: {
                dataType = DataTypes.StringType;
                break;
            }
            case Binary: {
                dataType = DataTypes.BinaryType;
                break;
            }
            case Boolean: {
                dataType = DataTypes.BooleanType;
                break;
            }
            case Date: {
                dataType = DataTypes.DateType;
                break;
            }
            case Timestamp: {
                dataType = DataTypes.TimestampType;
                break;
            }
            case CalendarInterval: {
                dataType = DataTypes.CalendarIntervalType;
                break;
            }
            case Double: {
                dataType = DataTypes.DoubleType;
                break;
            }
            case Float: {
                dataType = DataTypes.FloatType;
                break;
            }
            case Byte: {
                dataType = DataTypes.ByteType;
                break;
            }
            case Integer: {
                dataType = DataTypes.IntegerType;
                break;
            }
            case Long: {
                dataType = DataTypes.LongType;
                break;
            }
            case Short: {
                dataType = DataTypes.ShortType;
                break;
            }
            case Null: {
                dataType = DataTypes.NullType;
                break;
            }
        }

        return dataType;
    }

    private static DataType map(Class clasz) {
        return MAP_TYPES.getOrDefault(clasz, DataTypes.NullType);
    }

}
