/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/BloomFilterFunctions.h"

namespace facebook::velox::functions {

void registerBloomFilterFunctions(const std::string& prefix) {
  const std::vector<std::string> names = {
      prefix + "bloom_filter_might_contain"};

  // Numeric types: hash the raw storage bytes of the value.
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, bool>(
      names);
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, int8_t>(
      names);
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, int16_t>(
      names);
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, int32_t>(
      names);
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, int64_t>(
      names);
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, float>(
      names);
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, double>(
      names);

  // String types: hash the content bytes.
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, Varchar>(
      names);
  registerFunction<
      BloomFilterMightContainFunction,
      bool,
      Varbinary,
      Varbinary>(names);
}

} // namespace facebook::velox::functions
