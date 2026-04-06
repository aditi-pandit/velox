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
#include <span>

#define XXH_INLINE_ALL
#include <xxhash.h>

#include "velox/common/base/SplitBlockBloomFilter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {
namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Hash helpers using XXH64, seed 0 — same algorithm as bloom_filter_agg.
template <typename T>
static uint64_t hashValue(T value) {
  return XXH64(&value, sizeof(value), /*seed=*/0);
}

static uint64_t hashString(const std::string& value) {
  return XXH64(value.data(), value.size(), /*seed=*/0);
}

/// Parses a raw varbinary as an SBBF and checks whether the given hash is
/// present.  Requires the byte count to be an exact multiple of Block size.
static bool bloomContains(const StringView& raw, uint64_t hash) {
  constexpr size_t kBlockSize = sizeof(SplitBlockBloomFilter::Block);
  VELOX_CHECK_EQ(raw.size() % kBlockSize, 0);
  const int32_t numBlocks = static_cast<int32_t>(raw.size() / kBlockSize);
  // Copy into aligned storage before handing to the filter.
  const size_t numBytes = static_cast<size_t>(numBlocks) * kBlockSize;
  auto* blocks = static_cast<SplitBlockBloomFilter::Block*>(
      ::aligned_alloc(kBlockSize, numBytes));
  std::memcpy(blocks, raw.data(), numBytes);
  const bool result = SplitBlockBloomFilter(
                          std::span<SplitBlockBloomFilter::Block>(blocks, numBlocks))
                          .mayContain(hash);
  ::free(blocks);
  return result;
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

class BloomFilterAggregateFunctionTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    prestosql::registerAllAggregateFunctions(
        /*prefix=*/"", /*withCompanionFunctions=*/false);
  }

  /// Runs bloom_filter_agg over a single column and returns the result bytes.
  /// Asserts that the result is non-null and non-empty.
  StringView runBloomFilterAgg(const VectorPtr& column) {
    auto input = makeRowVector({column});
    auto plan = PlanBuilder()
                    .values({input})
                    .singleAggregation({}, {"bloom_filter_agg(c0)"})
                    .planNode();
    auto result =
        AssertQueryBuilder(plan).copyResults(pool());
    EXPECT_EQ(result->size(), 1);
    auto* flat = result->childAt(0)->asFlatVector<StringView>();
    EXPECT_FALSE(flat->isNullAt(0));
    return flat->valueAt(0);
  }
};

// ---------------------------------------------------------------------------
// bloom_filter_agg – bigint
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, bigint) {
  auto values = makeFlatVector<int64_t>({1LL, 2LL, 3LL, 42LL});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashValue(int64_t{1})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int64_t{2})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int64_t{3})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int64_t{42})));
  // 999 is not in the filter.
  EXPECT_FALSE(bloomContains(raw, hashValue(int64_t{999})));
}

// ---------------------------------------------------------------------------
// bloom_filter_agg – integer
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, integer) {
  auto values = makeFlatVector<int32_t>({10, 20, 30});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashValue(int32_t{10})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int32_t{20})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int32_t{30})));
  EXPECT_FALSE(bloomContains(raw, hashValue(int32_t{99})));
}

// ---------------------------------------------------------------------------
// bloom_filter_agg – smallint
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, smallint) {
  auto values = makeFlatVector<int16_t>({5, 10});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashValue(int16_t{5})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int16_t{10})));
  EXPECT_FALSE(bloomContains(raw, hashValue(int16_t{100})));
}

// ---------------------------------------------------------------------------
// bloom_filter_agg – tinyint
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, tinyint) {
  auto values = makeFlatVector<int8_t>({1, 2, 3});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashValue(int8_t{1})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int8_t{2})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int8_t{3})));
  EXPECT_FALSE(bloomContains(raw, hashValue(int8_t{100})));
}

// ---------------------------------------------------------------------------
// bloom_filter_agg – boolean
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, boolean) {
  auto values = makeFlatVector<bool>({true, false});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashValue(bool{true})));
  EXPECT_TRUE(bloomContains(raw, hashValue(bool{false})));
}

// ---------------------------------------------------------------------------
// bloom_filter_agg – real (float)
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, real) {
  auto values = makeFlatVector<float>({1.0f, 2.5f});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashValue(float{1.0f})));
  EXPECT_TRUE(bloomContains(raw, hashValue(float{2.5f})));
  EXPECT_FALSE(bloomContains(raw, hashValue(float{9.9f})));
}

// ---------------------------------------------------------------------------
// bloom_filter_agg – double
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, doubleType) {
  auto values = makeFlatVector<double>({3.14, 2.718});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashValue(double{3.14})));
  EXPECT_TRUE(bloomContains(raw, hashValue(double{2.718})));
  EXPECT_FALSE(bloomContains(raw, hashValue(double{0.0})));
}

// ---------------------------------------------------------------------------
// bloom_filter_agg – varchar
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, varchar) {
  auto values = makeFlatVector<StringView>({"hello"_sv, "world"_sv});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashString("hello")));
  EXPECT_TRUE(bloomContains(raw, hashString("world")));
  EXPECT_FALSE(bloomContains(raw, hashString("foo")));
}

// ---------------------------------------------------------------------------
// bloom_filter_agg – varbinary
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, varbinary) {
  auto values = makeFlatVector<StringView>(
      {StringView("\x01\x02"), StringView("\x03\x04")}, VARBINARY());
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashString(std::string("\x01\x02"))));
  EXPECT_TRUE(bloomContains(raw, hashString(std::string("\x03\x04"))));
  EXPECT_FALSE(bloomContains(raw, hashString(std::string("\x05\x06"))));
}

// ---------------------------------------------------------------------------
// Null inputs are ignored; the group is not null as long as one value is
// non-null.
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, nullInputsIgnored) {
  auto values = makeNullableFlatVector<int64_t>(
      {std::nullopt, 7LL, std::nullopt, 8LL});
  auto raw = runBloomFilterAgg(values);

  EXPECT_TRUE(bloomContains(raw, hashValue(int64_t{7})));
  EXPECT_TRUE(bloomContains(raw, hashValue(int64_t{8})));
}

// ---------------------------------------------------------------------------
// All-null input returns a null result for the group.
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, allNullInputReturnsNull) {
  auto values = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt});
  auto input = makeRowVector({values});

  auto plan = PlanBuilder()
                  .values({input})
                  .singleAggregation({}, {"bloom_filter_agg(c0)"})
                  .planNode();
  auto result = AssertQueryBuilder(plan).copyResults(pool());

  ASSERT_EQ(result->size(), 1);
  auto* flat = result->childAt(0)->asFlatVector<StringView>();
  EXPECT_TRUE(flat->isNullAt(0));
}

// ---------------------------------------------------------------------------
// Group-by: each group gets its own independent filter.
// ---------------------------------------------------------------------------

TEST_F(BloomFilterAggregateFunctionTest, groupBy) {
  // Group 0: values 1, 2 | Group 1: values 3, 4
  auto keys = makeFlatVector<int64_t>({0LL, 0LL, 1LL, 1LL});
  auto vals = makeFlatVector<int64_t>({1LL, 2LL, 3LL, 4LL});
  auto input = makeRowVector({keys, vals});

  auto plan = PlanBuilder()
                  .values({input})
                  .singleAggregation({"c0"}, {"bloom_filter_agg(c1)"})
                  .planNode();
  auto result = AssertQueryBuilder(plan).copyResults(pool());

  ASSERT_EQ(result->size(), 2);
  auto* groupCol = result->childAt(0)->asFlatVector<int64_t>();
  auto* filterCol = result->childAt(1)->asFlatVector<StringView>();

  // Identify which row is group 0 and which is group 1.
  int group0Row = -1;
  int group1Row = -1;
  for (int i = 0; i < 2; ++i) {
    if (groupCol->valueAt(i) == 0) {
      group0Row = i;
    } else {
      group1Row = i;
    }
  }
  ASSERT_NE(group0Row, -1);
  ASSERT_NE(group1Row, -1);

  auto raw0 = filterCol->valueAt(group0Row);
  auto raw1 = filterCol->valueAt(group1Row);

  // Group 0 should contain 1 and 2, not 3 or 4.
  EXPECT_TRUE(bloomContains(raw0, hashValue(int64_t{1})));
  EXPECT_TRUE(bloomContains(raw0, hashValue(int64_t{2})));
  EXPECT_FALSE(bloomContains(raw0, hashValue(int64_t{3})));
  EXPECT_FALSE(bloomContains(raw0, hashValue(int64_t{4})));

  // Group 1 should contain 3 and 4, not 1 or 2.
  EXPECT_TRUE(bloomContains(raw1, hashValue(int64_t{3})));
  EXPECT_TRUE(bloomContains(raw1, hashValue(int64_t{4})));
  EXPECT_FALSE(bloomContains(raw1, hashValue(int64_t{1})));
  EXPECT_FALSE(bloomContains(raw1, hashValue(int64_t{2})));
}

} // namespace
} // namespace facebook::velox::aggregate::test
