################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Simple program that creates an CountVectorizer instance and uses it for feature
# engineering.

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.ml.feature.countvectorizer import CountVectorizer
from pyflink.table import StreamTableEnvironment

# Creates a new StreamExecutionEnvironment.
env = StreamExecutionEnvironment.get_execution_environment()

# Creates a StreamTableEnvironment.
t_env = StreamTableEnvironment.create(env)

# Generates input training and prediction data.
input_table = t_env.from_data_stream(
    env.from_collection([
        (1, ['a', 'c', 'b', 'c'],),
        (2, ['c', 'd', 'e'],),
        (3, ['a', 'b', 'c'],),
        (4, ['e', 'f'],),
        (5, ['a', 'c', 'a'],),
    ],
        type_info=Types.ROW_NAMED(
            ['id', 'input', ],
            [Types.INT(), Types.OBJECT_ARRAY(Types.STRING())])
    ))

# Creates an CountVectorizer object and initializes its parameters.
count_vectorizer = CountVectorizer()

# Trains the CountVectorizer Model.
model = count_vectorizer.fit(input_table)

# Uses the CountVectorizer Model for predictions.
output = model.transform(input_table)[0]

# Extracts and displays the results.
field_names = output.get_schema().get_field_names()
for result in t_env.to_data_stream(output).execute_and_collect():
    input_index = field_names.index(count_vectorizer.get_input_col())
    output_index = field_names.index(count_vectorizer.get_output_col())
    print('Input Value: %-20s Output Value: %10s' %
          (str(result[input_index]), str(result[output_index])))
