# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(fastmem_benchmark FastMemBenchmark.cpp)
target_include_directories(fastmem_benchmark PRIVATE ${CMAKE_HOME_DIRECTORY}/src)
target_link_libraries(fastmem_benchmark PRIVATE benchmark::benchmark)
install(TARGETS fastmem_benchmark DESTINATION benchmark)

add_executable(prepared_field_reader_benchmark PreparedFieldReaderBenchmark.cpp)
target_include_directories(
    prepared_field_reader_benchmark
    PRIVATE
        ${CMAKE_HOME_DIRECTORY}/src
        ${CMAKE_HOME_DIRECTORY}/thirdparty/tantivy
        ${CMAKE_HOME_DIRECTORY}/thirdparty/tantivy/tantivy-binding/include
        ${CMAKE_HOME_DIRECTORY}/unittest
        ${CMAKE_HOME_DIRECTORY}/../parser/planparserv2/cwrapper
)
target_link_libraries(
    prepared_field_reader_benchmark
    PRIVATE
        benchmark::benchmark
        milvus_core
        milvus_conan_deps
        knowhere
        milvus-storage
)
install(TARGETS prepared_field_reader_benchmark DESTINATION benchmark)
