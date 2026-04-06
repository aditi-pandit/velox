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

#include <cstring>
#include <optional>
#include <string>
#include <vector>

#define XXH_INLINE_ALL
#include <xxhash.h>

#include "velox/common/base/SplitBlockBloomFilter.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::test {
namespace {

/// Builds a varbinary string containing an SBBF with the given hashes.
/// Uses the same default sizing as the aggregate (fpp=0.01, 10 000 elements).
static std::string buildBloomFilter(const std::vector<uint64_t>& hashes) {
  constexpr double kFpp = 0.01;
  constexpr int64_t kDefaultElements = 10'000;
  const int32_t numBlocks = static_cast<int32_t>(
      SplitBlockBloomFilter::numBlocks(kDefaultElements, kFpp));
  constexpr size_t kBlockSize = sizeof(SplitBlockBloomFilter::Block);
  const size_t numBytes = static_cast<size_t>(numBlocks) * kBlockSize;

  auto* blocks = static_cast<SplitBlockBloomFilter::Block*>(
      ::aligned_alloc(kBlockSize, numBytes));
  std::memset(blocks, 0, numBytes);
  SplitBlockBloomFilter filter(
      std::span<SplitBlockBloomFilter::Block>(blocks, numBlocks));
  for (uint64_t hash : hashes) {
    filter.insert(hash);
  }
  std::string result(reinterpret_cast<const char*>(blocks), numBytes);
  ::free(blocks);
  return result;
}

/// Hash helpers matching BloomFilterFunctions.h (XXH64, seed 0).
template <typename T>
static uint64_t hashValue(T value) {
  return XXH64(&value, sizeof(value), /*seed=*/0);
}

static uint64_t hashString(const std::string& value) {
  return XXH64(value.data(), value.size(), /*seed=*/0);
}

class BloomFilterFunctionsTest : public FunctionBaseTest {};

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – bigint
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, bigintPresent) {
  const int64_t kValue = 42LL;
  const std::string filter = buildBloomFilter({hashValue(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), BIGINT()},
      std::optional<std::string>{filter},
      std::optional<int64_t>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

TEST_F(BloomFilterFunctionsTest, bigintAbsent) {
  // 42 was inserted; 99 should not produce a false positive for this filter.
  const std::string filter = buildBloomFilter({hashValue(int64_t{42})});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), BIGINT()},
      std::optional<std::string>{filter},
      std::optional<int64_t>{99LL});
  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(*result);
}

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – integer
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, integerPresent) {
  const int32_t kValue = 7;
  const std::string filter = buildBloomFilter({hashValue(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), INTEGER()},
      std::optional<std::string>{filter},
      std::optional<int32_t>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – smallint
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, smallintPresent) {
  const int16_t kValue = 100;
  const std::string filter = buildBloomFilter({hashValue(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), SMALLINT()},
      std::optional<std::string>{filter},
      std::optional<int16_t>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – tinyint
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, tinyintPresent) {
  const int8_t kValue = 5;
  const std::string filter = buildBloomFilter({hashValue(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), TINYINT()},
      std::optional<std::string>{filter},
      std::optional<int8_t>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – boolean
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, booleanPresent) {
  const bool kValue = true;
  const std::string filter = buildBloomFilter({hashValue(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), BOOLEAN()},
      std::optional<std::string>{filter},
      std::optional<bool>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – real (float)
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, realPresent) {
  const float kValue = 3.14f;
  const std::string filter = buildBloomFilter({hashValue(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), REAL()},
      std::optional<std::string>{filter},
      std::optional<float>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – double
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, doublePresent) {
  const double kValue = 2.718281828;
  const std::string filter = buildBloomFilter({hashValue(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), DOUBLE()},
      std::optional<std::string>{filter},
      std::optional<double>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – varchar
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, varcharPresent) {
  const std::string kValue = "hello";
  const std::string filter = buildBloomFilter({hashString(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), VARCHAR()},
      std::optional<std::string>{filter},
      std::optional<std::string>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

TEST_F(BloomFilterFunctionsTest, varcharAbsent) {
  const std::string filter = buildBloomFilter({hashString("hello")});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), VARCHAR()},
      std::optional<std::string>{filter},
      std::optional<std::string>{"world"});
  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(*result);
}

// ---------------------------------------------------------------------------
// bloom_filter_might_contain – varbinary
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, varbinaryPresent) {
  const std::string kValue = "\x01\x02\x03";
  const std::string filter = buildBloomFilter({hashString(kValue)});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), VARBINARY()},
      std::optional<std::string>{filter},
      std::optional<std::string>{kValue});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
}

// ---------------------------------------------------------------------------
// Multiple values present in the same filter
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, multipleValuesSameFilter) {
  const std::string filter = buildBloomFilter({
      hashValue(int64_t{1}),
      hashValue(int64_t{2}),
      hashValue(int64_t{3}),
  });

  for (int64_t value : {1LL, 2LL, 3LL}) {
    auto result = evaluateOnce<bool>(
        "bloom_filter_might_contain(c0, c1)",
        {VARBINARY(), BIGINT()},
        std::optional<std::string>{filter},
        std::optional<int64_t>{value});
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(*result) << "Expected value " << value << " to be in the filter";
  }

  // 999 is extremely unlikely to collide with {1, 2, 3} in a large filter.
  auto absent = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), BIGINT()},
      std::optional<std::string>{filter},
      std::optional<int64_t>{999LL});
  ASSERT_TRUE(absent.has_value());
  EXPECT_FALSE(*absent);
}

// ---------------------------------------------------------------------------
// Null bloom filter: initialize() sees nullptr; call() should return false.
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, nullBloomFilterReturnsFalse) {
  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), BIGINT()},
      std::optional<std::string>{std::nullopt},
      std::optional<int64_t>{42LL});
  // A null bloom filter means the filter was never initialized; return false.
  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(*result);
}

// ---------------------------------------------------------------------------
// Null value argument propagates as null result.
// ---------------------------------------------------------------------------

TEST_F(BloomFilterFunctionsTest, nullValueReturnsNull) {
  const std::string filter = buildBloomFilter({hashValue(int64_t{42})});

  auto result = evaluateOnce<bool>(
      "bloom_filter_might_contain(c0, c1)",
      {VARBINARY(), BIGINT()},
      std::optional<std::string>{filter},
      std::optional<int64_t>{std::nullopt});
  EXPECT_FALSE(result.has_value());
}

} // namespace
} // namespace facebook::velox::functions::test
