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

package org.apache.flink.ml.servable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.ml.type.DataType;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * A DataFrame consists of some number of rows, each of which has the same list of column names and
 * data types.
 *
 * <p>All values in a column must have the same data type: integer, float, string etc.
 */
@PublicEvolving
public class DataFrame {

    private final List<String> columnNames;

    private final List<DataType> dataTypes;

    private final List<Row> rows;

    public DataFrame(List<String> columnNames, List<DataType> dataTypes, List<Row> rows) {
        Preconditions.checkArgument(columnNames.size() == dataTypes.size());
        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
        this.rows = rows;
    }

    /** Returns a list of the names of all the columns in this DataFrame. */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /** Returns a list of the data types of all the columns in this DataFrame. */
    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    /**
     * Returns the index of the column with the given name.
     *
     * @throws IllegalArgumentException if the column is not present in this table
     */
    public int getIndex(String name) {
        return columnNames.indexOf(name);
    }

    /**
     * Returns the data type of the column with the given name.
     *
     * @throws IllegalArgumentException if the column is not present in this table
     */
    public DataType getDataType(String name) {
        return dataTypes.get(getIndex(name));
    }

    /**
     * Adds to this DataFrame a column with the given name, data type, and values.
     *
     * @throws IllegalArgumentException if the number of values is different from the number of
     *     rows.
     */
    public DataFrame addColumn(String columnName, DataType dataType, List<Object> values) {
        columnNames.add(columnName);
        dataTypes.add(dataType);

        assert rows.size() == values.size();

        for (int i = 0; i < values.size(); i++) {
            rows.get(i).add(values.get(i));
        }
        return this;
    }

    /** Returns all rows of this table. */
    public List<Row> collect() {
        return rows;
    }
}
