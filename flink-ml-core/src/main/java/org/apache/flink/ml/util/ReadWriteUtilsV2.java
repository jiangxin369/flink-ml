/*
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

package org.apache.flink.ml.util;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.WithParams;
import org.apache.flink.ml.servable.TransformerServable;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class ReadWriteUtilsV2 {
    public static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().enable(JsonParser.Feature.ALLOW_COMMENTS);

    /**
     * Loads the metadata from the metadata file under the given path.
     *
     * <p>The method throws RuntimeException if the expectedClassName is not empty AND it does not
     * match the className of the previously saved stage.
     *
     * @param path The parent directory of the metadata file to read from.
     * @param expectedClassName The expected class name of the stage.
     * @return A map from metadata name to metadata value.
     */
    public static Map<String, ?> loadMetadata(String path, String expectedClassName)
            throws IOException {
        Path metadataPath = new Path(path, "metadata");
        FileSystem fs = metadataPath.getFileSystem();

        StringBuilder buffer = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(metadataPath)))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("#")) {
                    buffer.append(line);
                }
            }
        }

        @SuppressWarnings("unchecked")
        Map<String, ?> result = OBJECT_MAPPER.readValue(buffer.toString(), Map.class);

        String className = (String) result.get("className");
        if (!expectedClassName.isEmpty() && !expectedClassName.equals(className)) {
            throw new RuntimeException(
                    "Class name "
                            + className
                            + " does not match the expected class name "
                            + expectedClassName
                            + ".");
        }

        return result;
    }

    public static <T extends TransformerServable<T>> T loadServableParam(
            String path, Class<T> clazz) throws IOException {
        T instance = InstantiationUtil.instantiate(clazz);

        Map<String, Param<?>> nameToParam = new HashMap<>();
        for (Param<?> param : ParamUtils.getPublicFinalParamFields(instance)) {
            nameToParam.put(param.name, param);
        }

        Map<String, ?> jsonMap = loadMetadata(path, "");
        if (jsonMap.containsKey("paramMap")) {
            Map<String, Object> paramMap = (Map<String, Object>) jsonMap.get("paramMap");
            for (Map.Entry<String, Object> entry : paramMap.entrySet()) {
                Param<?> param = nameToParam.get(entry.getKey());
                setParam(instance, param, param.jsonDecode(entry.getValue()));
            }
        }

        return instance;
    }

    // A helper method that sets WithParams object's parameter value. We can not call
    // WithParams.set(param, value)
    // directly because WithParams::set(...) needs the actual type of the value.
    @SuppressWarnings("unchecked")
    public static <T> void setParam(WithParams<?> instance, Param<T> param, Object value) {
        instance.set(param, (T) value);
    }

    /** Returns a subdirectory of the given path for saving/loading model data. */
    public static Path getDataPath(String path) {
        return new Path(path, "data");
    }
}
