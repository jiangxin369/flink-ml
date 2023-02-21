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

package org.apache.flink.ml.servable.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.ml.servable.types.DataType;

import java.util.Iterator;
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
        int numColumns = columnNames.size();
        if (dataTypes.size() != numColumns) {
            throw new IllegalArgumentException(
                    "The number of data types is different from the number of column names.");
        }
        for (Row row : rows) {
            if (row.size() != numColumns) {
                throw new IllegalArgumentException(
                        "The number of values in some row is different from the number of column names");
            }
        }

        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
        this.rows = rows;
    }

    /** Returns a list of the names of all the columns in this DataFrame. */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /**
     * Returns the index of the column with the given name.
     *
     * @throws IllegalArgumentException if the column is not present in this table
     */
    public int getIndex(String name) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Failed to find the column with the given name.");
    }

    /**
     * Returns the data type of the column with the given name.
     *
     * @throws IllegalArgumentException if the column is not present in this table
     */
    public DataType getDataType(String name) {
        int index = getIndex(name);
        return dataTypes.get(index);
    }

    /**
     * Adds to this DataFrame a column with the given name, data type, and values.
     *
     * @throws IllegalArgumentException if the number of values is different from the number of
     *     rows.
     */
    public DataFrame addColumn(String columnName, DataType dataType, List<Object> values) {
        if (values.size() != rows.size()) {
            throw new RuntimeException(
                    "The number of values is different from the number of rows.");
        }
        columnNames.add(columnName);
        dataTypes.add(dataType);

        int rowSize = -1;
        Iterator<Object> iter = values.iterator();
        for (Row row : rows) {
            if (rowSize < 0) {
                rowSize = row.size();
            } else if (rowSize != row.size()) {
                throw new RuntimeException("The size of rows are different.");
            }

            Object value = iter.next();
            row.add(value);
        }
        return this;
    }

    /** Returns all rows of this table. */
    public List<Row> collect() {
        return rows;
    }
}
