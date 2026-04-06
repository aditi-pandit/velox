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

#include "velox/functions/prestosql/aggregates/BloomFilterAggregateFunction.h"

#include <cstdlib>
#include <cstring>

#define XXH_INLINE_ALL
#include <xxhash.h>

#include "velox/common/base/SplitBlockBloomFilter.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/type/Type.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

/// Default parameters used to size the bloom filter when no capacity hint is
/// provided by the caller.
constexpr double kDefaultFpp = 0.01;
constexpr int64_t kDefaultExpectedElements = 10'000;

// ---------------------------------------------------------------------------
// Accumulator
// ---------------------------------------------------------------------------

/// Per-group accumulator for bloom_filter_agg.
///
/// Holds a pointer to a properly-aligned SplitBlockBloomFilter::Block array
/// allocated via ::aligned_alloc.  The allocator is not used here because
/// HashStringAllocator does not guarantee the 16/32-byte alignment required
/// by SplitBlockBloomFilter::Block on SSE/AVX2 platforms.
struct BloomFilterAccumulator {
  /// Pointer to the block array, or nullptr before the first non-null value.
  SplitBlockBloomFilter::Block* blocks{nullptr};
  /// Number of blocks in the filter.
  int32_t numBlocks{0};

  BloomFilterAccumulator() = default;

  // Not copyable or movable: lifetime is managed via placement-new /
  // explicit destructor call by the Aggregate framework.
  BloomFilterAccumulator(const BloomFilterAccumulator&) = delete;
  BloomFilterAccumulator& operator=(const BloomFilterAccumulator&) = delete;

  ~BloomFilterAccumulator() {
    // free(nullptr) is a no-op, so this is safe for uninitialized groups.
    ::free(blocks);
  }

  bool isInitialized() const {
    return blocks != nullptr;
  }

  /// Allocates and zero-initializes the block array on the first insertion.
  void init(int32_t nb) {
    VELOX_DCHECK(!isInitialized());
    constexpr size_t kBlockSize = sizeof(SplitBlockBloomFilter::Block);
    blocks = static_cast<SplitBlockBloomFilter::Block*>(
        ::aligned_alloc(kBlockSize, static_cast<size_t>(nb) * kBlockSize));
    VELOX_CHECK_NOT_NULL(
        blocks, "Failed to allocate {} bloom filter blocks", nb);
    std::memset(blocks, 0, static_cast<size_t>(nb) * kBlockSize);
    numBlocks = nb;
  }

  /// Inserts a pre-computed hash into the filter.
  void insert(uint64_t hash) {
    VELOX_DCHECK(isInitialized());
    SplitBlockBloomFilter filter(
        std::span<SplitBlockBloomFilter::Block>(blocks, numBlocks));
    filter.insert(hash);
  }
};

// ---------------------------------------------------------------------------
// Hash helpers
// ---------------------------------------------------------------------------

/// Hashes a string value (covers both Varchar and Varbinary).
FOLLY_ALWAYS_INLINE uint64_t
bloomFilterHash(const StringView& value) noexcept {
  return XXH64(value.data(), value.size(), /*seed=*/0);
}

/// Hashes a numeric value using its raw storage bytes.
template <typename T>
FOLLY_ALWAYS_INLINE uint64_t bloomFilterHash(T value) noexcept {
  return XXH64(&value, sizeof(value), /*seed=*/0);
}

// ---------------------------------------------------------------------------
// Aggregate class
// ---------------------------------------------------------------------------

/// Aggregate that inserts all non-null input values into a Split-Block Bloom
/// Filter and returns the filter's raw block bytes as varbinary.
///
/// This is a single-phase (final-only) aggregate.  Partial aggregation is not
/// supported; calling addIntermediateResults() throws VELOX_UNSUPPORTED.
///
/// Template parameter T is the C++ native type of the input column
/// (e.g. int64_t for BIGINT, StringView for VARCHAR/VARBINARY).
template <typename T>
class BloomFilterAggregate final : public exec::Aggregate {
 public:
  explicit BloomFilterAggregate(int32_t numBlocks)
      : exec::Aggregate(VARBINARY()), numBlocks_(numBlocks) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(BloomFilterAccumulator);
  }

  /// Tells the framework that each group owns external heap memory.
  bool isFixedSize() const override {
    return false;
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* flatResult = (*result)->asUnchecked<FlatVector<StringView>>();
    flatResult->resize(numGroups);
    flatResult->clearAllNulls();

    for (int32_t i = 0; i < numGroups; ++i) {
      if (isNull(groups[i])) {
        flatResult->setNull(i, true);
        continue;
      }
      const auto* acc = value<BloomFilterAccumulator>(groups[i]);
      if (!acc->isInitialized()) {
        flatResult->setNull(i, true);
        continue;
      }
      const size_t numBytes =
          static_cast<size_t>(acc->numBlocks) *
          sizeof(SplitBlockBloomFilter::Block);
      // getRawStringBufferWithSpace allocates space inside the result vector's
      // own string buffer so the data outlives this function.
      char* rawBuf = flatResult->getRawStringBufferWithSpace(numBytes);
      std::memcpy(rawBuf, acc->blocks, numBytes);
      flatResult->setNoCopy(i, StringView(rawBuf, numBytes));
    }
  }

  /// No partial aggregation; the intermediate and final representations are
  /// identical.
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushDown*/) override {
    decodedInput_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedInput_.isNullAt(row)) {
        return;
      }
      auto* acc = value<BloomFilterAccumulator>(groups[row]);
      if (FOLLY_UNLIKELY(!acc->isInitialized())) {
        acc->init(numBlocks_);
      }
      clearNull(groups[row]);
      acc->insert(bloomFilterHash(decodedInput_.valueAt<T>(row)));
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushDown*/) override {
    decodedInput_.decode(*args[0], rows);
    auto* acc = value<BloomFilterAccumulator>(group);
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedInput_.isNullAt(row)) {
        return;
      }
      if (FOLLY_UNLIKELY(!acc->isInitialized())) {
        acc->init(numBlocks_);
      }
      clearNull(group);
      acc->insert(bloomFilterHash(decodedInput_.valueAt<T>(row)));
    });
  }

  void addIntermediateResults(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushDown*/) override {
    VELOX_UNSUPPORTED(
        "bloom_filter_agg is a final-only aggregate and does not support "
        "partial aggregation");
  }

  void addSingleGroupIntermediateResults(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushDown*/) override {
    VELOX_UNSUPPORTED(
        "bloom_filter_agg is a final-only aggregate and does not support "
        "partial aggregation");
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) BloomFilterAccumulator();
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    destroyAccumulators<BloomFilterAccumulator>(groups);
  }

 private:
  /// Number of SplitBlockBloomFilter::Block elements in each group's filter.
  const int32_t numBlocks_;
  /// Reused across calls to addRawInput / addSingleGroupRawInput.
  DecodedVector decodedInput_;
};

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

template <TypeKind Kind>
std::unique_ptr<exec::Aggregate> createBloomFilterAggregate(int32_t numBlocks) {
  using T = typename TypeTraits<Kind>::NativeType;
  return std::make_unique<BloomFilterAggregate<T>>(numBlocks);
}

} // namespace

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

void registerBloomFilterAggregateFunction(
    const std::vector<std::string>& names,
    bool withCompanionFunctions,
    bool overwrite) {
  // One signature per supported scalar type so that callers get a clear error
  // at planning time when an unsupported type is used.
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto* typeName :
       {"boolean",
        "tinyint",
        "smallint",
        "integer",
        "bigint",
        "real",
        "double",
        "varchar",
        "varbinary"}) {
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .returnType("varbinary")
            .intermediateType("varbinary")
            .argumentType(typeName)
            .build());
  }

  const int32_t numBlocks = static_cast<int32_t>(
      SplitBlockBloomFilter::numBlocks(kDefaultExpectedElements, kDefaultFpp));

  exec::registerAggregateFunction(
      names,
      std::move(signatures),
      [names, numBlocks](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& /*resultType*/,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(), 1, "{} takes one argument", names.front());
        return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createBloomFilterAggregate, argTypes[0]->kind(), numBlocks);
      },
      {.orderSensitive = false},
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
