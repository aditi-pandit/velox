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
#pragma once

#define XXH_INLINE_ALL
#include <xxhash.h>

#include "velox/common/base/SplitBlockBloomFilter.h"
#include "velox/functions/Macros.h"

namespace facebook::velox::functions {

/// Checks whether a value might be present in a Split-Block Bloom Filter.
///
/// The first argument is the raw serialized bytes of a
/// SplitBlockBloomFilter::Block array (no header). The size must be a
/// positive multiple of sizeof(SplitBlockBloomFilter::Block). The value to
/// probe is hashed with XXH64 (seed 0) on its native byte representation:
/// numeric types are hashed as their raw storage bytes, and string types
/// are hashed as their content bytes.
///
/// Returns false when the value is definitely absent, and true when it might
/// be present. Returns false conservatively when the bloom filter is not a
/// constant expression at plan time.
///
/// Registered for all scalar types: boolean, tinyint, smallint, integer,
/// bigint, real, double, varchar, and varbinary.
template <typename T>
struct BloomFilterMightContainFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  /// Deserializes the bloom filter blocks when the filter is a constant.
  template <typename TValue>
  void initialize(
      const std::vector<velox::TypePtr>& /*inputTypes*/,
      const velox::core::QueryConfig& /*config*/,
      const arg_type<Varbinary>* bloomFilter,
      const TValue* /*value*/) {
    if (bloomFilter != nullptr) {
      initBloomFilter(bloomFilter->data(), bloomFilter->size());
    }
  }

  /// Returns true if the value might be in the filter, false if it is
  /// definitely absent.
  template <typename TValue>
  FOLLY_ALWAYS_INLINE void call(
      bool& result,
      const arg_type<Varbinary>& /*bloomFilter*/,
      const TValue& value) {
    if (!initialized_) {
      result = false;
      return;
    }
    const uint64_t hash = computeHash(value);
    SplitBlockBloomFilter filter(
        std::span<SplitBlockBloomFilter::Block>(blocks_));
    result = filter.mayContain(hash);
  }

 private:
  /// Hashes a string value (covers both Varchar and Varbinary).
  static FOLLY_ALWAYS_INLINE uint64_t computeHash(const StringView& value) {
    return XXH64(value.data(), value.size(), /*seed=*/0);
  }

  /// Hashes a numeric value using its raw storage bytes.
  template <typename TValue>
  static FOLLY_ALWAYS_INLINE uint64_t computeHash(TValue value) {
    return XXH64(&value, sizeof(value), /*seed=*/0);
  }

  void initBloomFilter(const char* data, size_t numBytes) {
    constexpr size_t kBlockSize = sizeof(SplitBlockBloomFilter::Block);
    VELOX_CHECK_GT(numBytes, 0, "Bloom filter varbinary must not be empty");
    VELOX_CHECK_EQ(
        numBytes % kBlockSize,
        0,
        "Bloom filter size {} is not a multiple of block size {}",
        numBytes,
        kBlockSize);
    blocks_.resize(numBytes / kBlockSize);
    std::memcpy(blocks_.data(), data, numBytes);
    initialized_ = true;
  }

  /// True once the bloom filter blocks have been deserialized.
  bool initialized_{false};
  /// Owns a copy of the bloom filter block data; alignment is guaranteed by
  /// SplitBlockBloomFilter::Block carrying an alignas specifier.
  std::vector<SplitBlockBloomFilter::Block> blocks_;
};

} // namespace facebook::velox::functions
