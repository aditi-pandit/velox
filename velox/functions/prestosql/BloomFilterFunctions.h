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
/// positive multiple of sizeof(SplitBlockBloomFilter::Block). The value is
/// hashed with XXH64 (seed 0) to match the Parquet specification.
///
/// Returns false when the value is definitely absent, and true when it might
/// be present. Returns false conservatively when the bloom filter is not a
/// constant expression at plan time.
template <typename T>
struct BloomFilterMightContainFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  /// Initializes the bloom filter blocks from the constant filter argument.
  void initialize(
      const std::vector<velox::TypePtr>& /*inputTypes*/,
      const velox::core::QueryConfig& /*config*/,
      const arg_type<Varbinary>* bloomFilter,
      const arg_type<int64_t>* /*value*/) {
    if (bloomFilter != nullptr) {
      constexpr size_t kBlockSize = sizeof(SplitBlockBloomFilter::Block);
      const size_t numBytes = bloomFilter->size();
      VELOX_CHECK_GT(numBytes, 0, "Bloom filter varbinary must not be empty");
      VELOX_CHECK_EQ(
          numBytes % kBlockSize,
          0,
          "Bloom filter size {} is not a multiple of block size {}",
          numBytes,
          kBlockSize);
      blocks_.resize(numBytes / kBlockSize);
      std::memcpy(blocks_.data(), bloomFilter->data(), numBytes);
      initialized_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      bool& result,
      const arg_type<Varbinary>& /*bloomFilter*/,
      const int64_t& value) {
    if (!initialized_) {
      result = false;
      return;
    }
    const uint64_t hash = XXH64(&value, sizeof(value), /*seed=*/0);
    SplitBlockBloomFilter filter(
        std::span<SplitBlockBloomFilter::Block>(blocks_));
    result = filter.mayContain(hash);
  }

 private:
  bool initialized_{false};
  /// Owns a copy of the bloom filter block data; alignment is guaranteed by
  /// SplitBlockBloomFilter::Block carrying an alignas specifier.
  std::vector<SplitBlockBloomFilter::Block> blocks_;
};

} // namespace facebook::velox::functions
