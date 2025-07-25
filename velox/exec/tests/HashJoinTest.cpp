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

#include <re2/re2.h>

#include <fmt/format.h>
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HashJoinTestBase.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/VectorTestUtil.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

using facebook::velox::test::BatchMaker;

namespace facebook::velox::exec {
namespace {

class HashJoinTest : public HashJoinTestBase {
 public:
  HashJoinTest() : HashJoinTestBase(TestParam(1)) {}

  explicit HashJoinTest(const TestParam& param) : HashJoinTestBase(param) {}
};

class MultiThreadedHashJoinTest
    : public HashJoinTest,
      public testing::WithParamInterface<TestParam> {
 public:
  MultiThreadedHashJoinTest() : HashJoinTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return std::vector<TestParam>({TestParam{1}, TestParam{3}});
  }
};

TEST_P(MultiThreadedHashJoinTest, bigintArray) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, outOfJoinKeyColumnOrder) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeType(probeType_)
      .probeKeys({"t_k2"})
      .probeVectors(5, 10)
      .buildType(buildType_)
      .buildKeys({"u_k2"})
      .buildVectors(64, 15)
      .joinOutputLayout({"t_k1", "t_k2", "u_k1", "u_k2", "u_v1"})
      .referenceQuery(
          "SELECT t_k1, t_k2, u_k1, u_k2, u_v1 FROM t, u WHERE t_k2 = u_k2")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, joinWithCancellation) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .injectTaskCancellation(true)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        auto stats = task->taskStats();
        EXPECT_GT(stats.terminationTimeMs, 0);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, testJoinWithSpillenabledCancellation) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .injectTaskCancellation(true)
      .injectSpill(false)
      // Need spill directory so that canSpill() is true for HashProbe
      .spillDirectory(spillDirectory->getPath())
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, emptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .keyTypes({BIGINT()})
        .probeVectors(1600, 5)
        .buildVectors(0, 5)
        .referenceQuery(
            "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          // Check the hash probe has processed probe input rows.
          if (finishOnEmpty) {
            ASSERT_EQ(getInputPositions(task, 1), 0);
          } else {
            ASSERT_GT(getInputPositions(task, 1), 0);
          }
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, emptyProbe) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(0, 5)
      .buildVectors(1500, 5)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0")
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        const auto statsPair = taskSpilledStats(*task);
        if (hasSpill) {
          ASSERT_GT(statsPair.first.spilledRows, 0);
          ASSERT_GT(statsPair.first.spilledBytes, 0);
          ASSERT_GT(statsPair.first.spilledPartitions, 0);
          ASSERT_GT(statsPair.first.spilledFiles, 0);
          // There is no spilling at empty probe side.
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_GT(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
        } else {
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
        }
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, normalizedKey) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT(), VARCHAR()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, normalizedKeyOverflow) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .keyTypes({BIGINT(), VARCHAR(), BIGINT(), BIGINT(), BIGINT(), BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5")
      .run();
}

DEBUG_ONLY_TEST_P(MultiThreadedHashJoinTest, parallelJoinBuildCheck) {
  std::atomic<bool> isParallelBuild{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashTable::parallelJoinBuild",
      std::function<void(void*)>([&](void*) { isParallelBuild = true; }));
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT(), VARCHAR()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1")
      .injectSpill(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        auto joinStats = task->taskStats()
                             .pipelineStats.back()
                             .operatorStats.back()
                             .runtimeStats;
        ASSERT_GT(joinStats["hashtable.buildWallNanos"].sum, 0);
        ASSERT_GE(joinStats["hashtable.buildWallNanos"].count, 1);
      })
      .run();
  ASSERT_EQ(numDrivers_ == 1, !isParallelBuild);
}

DEBUG_ONLY_TEST_P(
    MultiThreadedHashJoinTest,
    raceBetweenTaskTerminateAndTableBuild) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(Operator*)>([&](Operator* op) {
        auto task = op->operatorCtx()->task();
        task->requestAbort();
      }));
  VELOX_ASSERT_THROW(
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .keyTypes({BIGINT(), VARCHAR()})
          .probeVectors(1600, 5)
          .buildVectors(1500, 5)
          .referenceQuery(
              "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1")
          .injectSpill(false)
          .run(),
      "Aborted for external error");
}

TEST_P(MultiThreadedHashJoinTest, allTypes) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .keyTypes(
          {BIGINT(),
           VARCHAR(),
           REAL(),
           DOUBLE(),
           INTEGER(),
           SMALLINT(),
           TINYINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_k6, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_k6, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5 AND t_k6 = u_k6")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, filter) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .joinFilter("((t_k0 % 100) + (u_k0 % 100)) % 40 < 20")
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0 AND ((t_k0 % 100) + (u_k0 % 100)) % 40 < 20")
      .run();
}

DEBUG_ONLY_TEST_P(MultiThreadedHashJoinTest, filterSpillOnFirstProbeInput) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  std::atomic_bool injectProbeSpillOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        HashProbe* probeOp = static_cast<HashProbe*>(op);
        if (!probeOp->testingHasPendingInput()) {
          return;
        }
        if (!injectProbeSpillOnce.exchange(false)) {
          return;
        }
        testingRunArbitration(op->pool());
        ASSERT_EQ(op->pool()->usedBytes(), 40960);
        ASSERT_EQ(op->pool()->reservedBytes(), 1048576);
      }));

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .numDrivers(1)
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .injectSpill(false)
      .spillDirectory(spillDirectory->getPath())
      .joinFilter("((t_k0 % 100) + (u_k0 % 100)) % 40 < 20")
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0 AND ((t_k0 % 100) + (u_k0 % 100)) % 40 < 20")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        const auto statsPair = taskSpilledStats(*task);
        ASSERT_EQ(statsPair.first.spilledRows, 0);
        ASSERT_EQ(statsPair.first.spilledBytes, 0);
        ASSERT_EQ(statsPair.first.spilledPartitions, 0);
        ASSERT_EQ(statsPair.first.spilledFiles, 0);
        ASSERT_GT(statsPair.second.spilledRows, 0);
        ASSERT_GT(statsPair.second.spilledBytes, 0);
        ASSERT_GT(statsPair.second.spilledPartitions, 0);
        ASSERT_GT(statsPair.second.spilledFiles, 0);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithNull) {
  struct {
    double probeNullRatio;
    double buildNullRatio;

    std::string debugString() const {
      return fmt::format(
          "probeNullRatio: {}, buildNullRatio: {}",
          probeNullRatio,
          buildNullRatio);
    }
  } testSettings[] = {
      {0.0, 1.0}, {0.0, 0.1}, {0.1, 1.0}, {0.1, 0.1}, {1.0, 1.0}, {1.0, 0.1}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<RowVectorPtr> probeVectors =
        makeBatches(5, 3, probeType_, pool_.get(), testData.probeNullRatio);

    // The first half number of build batches having no nulls to trigger it
    // later during the processing.
    std::vector<RowVectorPtr> buildVectors = mergeBatches(
        makeBatches(5, 6, buildType_, pool_.get(), 0.0),
        makeBatches(5, 6, buildType_, pool_.get(), testData.buildNullRatio));

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeType(probeType_)
        .probeKeys({"t_k2"})
        .probeVectors(std::move(probeVectors))
        .buildType(buildType_)
        .buildKeys({"u_k2"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinOutputLayout({"t_k1", "t_k2"})
        .referenceQuery(
            "SELECT t_k1, t_k2 FROM t WHERE t.t_k2 NOT IN (SELECT u_k2 FROM u)")
        // NOTE: we might not trigger spilling at build side if we detect the
        // null join key in the build rows early.
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilterWithLargeOutput) {
  // Build the identical left and right vectors to generate large join
  // outputs.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(4, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {makeFlatVector<int32_t>(2048, [](auto row) { return row; }),
             makeFlatVector<int32_t>(2048, [](auto row) { return row; })});
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(4, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {makeFlatVector<int32_t>(2048, [](auto row) { return row; }),
             makeFlatVector<int32_t>(2048, [](auto row) { return row; })});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kRightSemiFilter)
      .joinOutputLayout({"u1"})
      .referenceQuery("SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)")
      .run();
}

/// Test hash join where build-side keys come from a small range and allow for
/// array-based lookup instead of a hash table.
TEST_P(MultiThreadedHashJoinTest, arrayBasedLookup) {
  auto oddIndices = makeIndices(500, [](auto i) { return 2 * i + 1; });

  std::vector<RowVectorPtr> probeVectors = {
      // Join key vector is flat.
      makeRowVector({
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is a match in the build side.
      makeRowVector({
          makeConstant(4, 2'000),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is no match.
      makeRowVector({
          makeConstant(5, 2'000),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is a dictionary.
      makeRowVector({
          wrapInDictionary(
              oddIndices,
              500,
              makeFlatVector<int32_t>(1'000, [](auto row) { return row * 4; })),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      })};

  // 100 key values in [0, 198] range.
  std::vector<RowVectorPtr> buildVectors = {
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row / 2; })}),
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row * 2; })}),
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row; })})};

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"c0"})
      .buildVectors(std::move(buildVectors))
      .joinOutputLayout({"c1"})
      .outputProjections({"c1 + 1"})
      .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        if (hasSpill) {
          return;
        }
        auto joinStats = task->taskStats()
                             .pipelineStats.back()
                             .operatorStats.back()
                             .runtimeStats;
        ASSERT_EQ(151, joinStats["distinctKey0"].sum);
        ASSERT_EQ(200, joinStats["rangeKey0"].sum);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, joinSidesDifferentSchema) {
  // In this join, the tables have different schema. LHS table t has schema
  // {INTEGER, VARCHAR, INTEGER}. RHS table u has schema {INTEGER, REAL,
  // INTEGER}. The filter predicate uses
  // a column from the right table  before the left and the corresponding
  // columns at the same channel number(1) have different types. This has been
  // a source of crashes in the join logic.
  size_t batchSize = 100;

  std::vector<std::string> stringVector = {"aaa", "bbb", "ccc", "ddd", "eee"};
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
            makeFlatVector<StringView>(
                batchSize,
                [&](auto row) {
                  return StringView(stringVector[row % stringVector.size()]);
                }),
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
        });
      });
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
            makeFlatVector<double>(
                batchSize, [](auto row) { return row * 5.0; }),
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
        });
      });

  // In this hash join the 2 tables have a common key which is the
  // first channel in both tables.
  const std::string referenceQuery =
      "SELECT t.c0 * t.c2/2 FROM "
      "  t, u "
      "  WHERE t.c0 = u.c0 AND "
      // TODO: enable ltrim test after the race condition in expression
      // execution gets fixed.
      //"  u.c2 > 10 AND ltrim(t.c1) = 'aaa'";
      "  u.c2 > 10";

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t_c0"})
      .probeVectors(std::move(probeVectors))
      .probeProjections({"c0 AS t_c0", "c1 AS t_c1", "c2 AS t_c2"})
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1", "c2 AS u_c2"})
      //.joinFilter("u_c2 > 10 AND ltrim(t_c1) == 'aaa'")
      .joinFilter("u_c2 > 10")
      .joinOutputLayout({"t_c0", "t_c2"})
      .outputProjections({"t_c0 * t_c2/2"})
      .referenceQuery(referenceQuery)
      .run();
}

TEST_P(MultiThreadedHashJoinTest, innerJoinWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    std::vector<RowVectorPtr> probeVectors = makeBatches(5, [&](int32_t batch) {
      return makeRowVector({
          makeFlatVector<int32_t>(
              123,
              [batch](auto row) { return row * 11 / std::max(batch, 1); },
              nullEvery(13)),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      });
    });
    std::vector<RowVectorPtr> buildVectors =
        makeBatches(10, [&](int32_t batch) {
          return makeRowVector({makeFlatVector<int32_t>(
              123,
              [batch](auto row) { return row % std::max(batch, 1); },
              nullEvery(7))});
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(buildVectors))
        .buildFilter("c0 < 0")
        .joinOutputLayout({"c1"})
        .referenceQuery("SELECT null LIMIT 0")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
          // Check the hash probe has processed probe input rows.
          if (finishOnEmpty) {
            ASSERT_EQ(getInputPositions(task, 1), 0);
          } else {
            ASSERT_GT(getInputPositions(task, 1), 0);
          }
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftSemiJoinFilter) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeType(probeType_)
      .probeVectors(174, 5)
      .probeKeys({"t_k1"})
      .buildType(buildType_)
      .buildVectors(133, 4)
      .buildKeys({"u_k1"})
      .joinType(core::JoinType::kLeftSemiFilter)
      .joinOutputLayout({"t_k2"})
      .referenceQuery("SELECT t_k2 FROM t WHERE t_k1 IN (SELECT u_k1 FROM u)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftSemiJoinFilterWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    std::vector<RowVectorPtr> probeVectors =
        makeBatches(10, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  1'234, [](auto row) { return row % 11; }, nullEvery(13)),
              makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
          });
        });
    std::vector<RowVectorPtr> buildVectors =
        makeBatches(10, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  123, [](auto row) { return row % 5; }, nullEvery(7)),
          });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kLeftSemiFilter)
        .joinFilter("c0 < 0")
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u WHERE c0 < 0)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftSemiJoinFilterWithExtraFilter) {
  std::vector<RowVectorPtr> probeVectors = makeBatches(5, [&](int32_t batch) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(
                250, [batch](auto row) { return row % (11 + batch); }),
            makeFlatVector<int32_t>(
                250, [batch](auto row) { return row * batch; }),
        });
  });

  std::vector<RowVectorPtr> buildVectors = makeBatches(5, [&](int32_t batch) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(
                123, [batch](auto row) { return row % (5 + batch); }),
            makeFlatVector<int32_t>(
                123, [batch](auto row) { return row * batch; }),
        });
  });

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kLeftSemiFilter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0)")
        .run();
  }

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kLeftSemiFilter)
        .joinFilter("t1 != u1")
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE EXISTS (SELECT u0, u1 FROM u WHERE t0 = u0 AND t1 <> u1)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilter) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeType(probeType_)
      .probeVectors(133, 3)
      .probeKeys({"t_k1"})
      .buildType(buildType_)
      .buildVectors(174, 4)
      .buildKeys({"u_k1"})
      .joinType(core::JoinType::kRightSemiFilter)
      .joinOutputLayout({"u_k2"})
      .referenceQuery("SELECT u_k2 FROM u WHERE u_k1 IN (SELECT t_k1 FROM t)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilterWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    // probeVectors size is greater than buildVector size.
    std::vector<RowVectorPtr> probeVectors =
        makeBatches(5, [&](uint32_t /*unused*/) {
          return makeRowVector(
              {"t0", "t1"},
              {makeFlatVector<int32_t>(
                   431, [](auto row) { return row % 11; }, nullEvery(13)),
               makeFlatVector<int32_t>(431, [](auto row) { return row; })});
        });

    std::vector<RowVectorPtr> buildVectors =
        makeBatches(5, [&](uint32_t /*unused*/) {
          return makeRowVector(
              {"u0", "u1"},
              {
                  makeFlatVector<int32_t>(
                      434, [](auto row) { return row % 5; }, nullEvery(7)),
                  makeFlatVector<int32_t>(434, [](auto row) { return row; }),
              });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(buildVectors))
        .buildFilter("u0 < 0")
        .joinType(core::JoinType::kRightSemiFilter)
        .joinOutputLayout({"u1"})
        .referenceQuery(
            "SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t) AND u.u0 < 0")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
          // Check the hash probe has processed probe input rows.
          if (finishOnEmpty) {
            ASSERT_EQ(getInputPositions(task, 1), 0);
          } else {
            ASSERT_GT(getInputPositions(task, 1), 0);
          }
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilterWithAllMatches) {
  // Make build side larger to test all rows are returned.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(3, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {
                makeFlatVector<int32_t>(
                    123, [](auto row) { return row % 5; }, nullEvery(7)),
                makeFlatVector<int32_t>(123, [](auto row) { return row; }),
            });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {makeFlatVector<int32_t>(
                 314, [](auto row) { return row % 11; }, nullEvery(13)),
             makeFlatVector<int32_t>(314, [](auto row) { return row; })});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kRightSemiFilter)
      .joinOutputLayout({"u1"})
      .referenceQuery("SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinFilterWithExtraFilter) {
  auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(345, [](auto row) { return row; }),
            makeFlatVector<int32_t>(345, [](auto row) { return row; }),
        });
  });

  auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(250, [](auto row) { return row; }),
            makeFlatVector<int32_t>(250, [](auto row) { return row; }),
        });
  });

  // Always true filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemiFilter)
        .joinFilter("t1 > -1")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 > -1)")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          ASSERT_EQ(
              getOutputPositions(task, "HashProbe"), 200 * 5 * numDrivers_);
        })
        .run();
  }

  // Always false filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemiFilter)
        .joinFilter("t1 > 100000")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 > 100000)")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          ASSERT_EQ(getOutputPositions(task, "HashProbe"), 0);
        })
        .run();
  }

  // Selective filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemiFilter)
        .joinFilter("t1 % 5 = 0")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 % 5 = 0)")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          ASSERT_EQ(
              getOutputPositions(task, "HashProbe"), 200 / 5 * 5 * numDrivers_);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, semiFilterOverLazyVectors) {
  auto probeVectors = makeBatches(1, [&](auto /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
            makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
        });
  });

  auto buildVectors = makeBatches(3, [&](auto /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(
                1'000, [](auto row) { return -100 + (row / 5); }),
            makeFlatVector<int64_t>(
                1'000, [](auto row) { return -1000 + (row / 5) * 10; }),
        });
  });

  std::shared_ptr<TempFilePath> probeFile = TempFilePath::create();
  writeToFile(probeFile->getPath(), probeVectors);

  std::shared_ptr<TempFilePath> buildFile = TempFilePath::create();
  writeToFile(buildFile->getPath(), buildVectors);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(probeVectors[0]->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(asRowType(buildVectors[0]->type()))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      "",
                      {"t0", "t1"},
                      core::JoinType::kLeftSemiFilter)
                  .planNode();

  SplitInput splitInput = {
      {probeScanId,
       {exec::Split(makeHiveConnectorSplit(probeFile->getPath()))}},
      {buildScanId,
       {exec::Split(makeHiveConnectorSplit(buildFile->getPath()))}},
  };

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT t0, t1 FROM t WHERE t0 IN (SELECT u0 FROM u)")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT t0, t1 FROM t WHERE t0 IN (SELECT u0 FROM u)")
      .run();

  // With extra filter.
  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plan = PlanBuilder(planNodeIdGenerator)
             .tableScan(asRowType(probeVectors[0]->type()))
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator)
                     .tableScan(asRowType(buildVectors[0]->type()))
                     .capturePlanNodeId(buildScanId)
                     .planNode(),
                 "(t1 + u1) % 3 = 0",
                 {"t0", "t1"},
                 core::JoinType::kLeftSemiFilter)
             .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1 FROM t WHERE t0 IN (SELECT u0 FROM u WHERE (t1 + u1) % 3 = 0)")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1 FROM t WHERE t0 IN (SELECT u0 FROM u WHERE (t1 + u1) % 3 = 0)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoin) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                1'000, [](auto row) { return row % 11; }, nullEvery(13)),
            makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
        });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                1'234, [](auto row) { return row % 5; }, nullEvery(7)),
        });
      });

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildFilter("c0 IS NOT NULL")
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 IS NOT NULL)")
        .checkSpillStats(false)
        .run();
  }

  // Empty build side.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildFilter("c0 < 0")
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 < 0)")
        .checkSpillStats(false)
        .run();
  }

  // Build side with nulls. Null-aware Anti join always returns nothing.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u)")
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilter) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {
                makeFlatVector<int32_t>(128, [](auto row) { return row % 11; }),
                makeFlatVector<int32_t>(128, [](auto row) { return row; }),
            });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {
                makeFlatVector<int32_t>(123, [](auto row) { return row % 5; }),
                makeFlatVector<int32_t>(123, [](auto row) { return row; }),
            });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kAnti)
      .nullAware(true)
      .joinFilter("t1 != u1")
      .joinOutputLayout({"t0", "t1"})
      .referenceQuery(
          "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE t0 = u0 AND t1 <> u1)")
      .checkSpillStats(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        // Verify spilling is not triggered in case of null-aware anti-join
        // with filter.
        const auto statsPair = taskSpilledStats(*task);
        ASSERT_EQ(statsPair.first.spilledRows, 0);
        ASSERT_EQ(statsPair.first.spilledBytes, 0);
        ASSERT_EQ(statsPair.first.spilledPartitions, 0);
        ASSERT_EQ(statsPair.first.spilledFiles, 0);
        ASSERT_EQ(statsPair.second.spilledRows, 0);
        ASSERT_EQ(statsPair.second.spilledBytes, 0);
        ASSERT_EQ(statsPair.second.spilledPartitions, 0);
        ASSERT_EQ(statsPair.second.spilledFiles, 0);
        verifyTaskSpilledRuntimeStats(*task, false);
        ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilterAndEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
              makeFlatVector<int32_t>({0, 1, 2}),
          });
    });
    auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeNullableFlatVector<int32_t>({3, 2, 3}),
              makeFlatVector<int32_t>({0, 2, 3}),
          });
    });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::vector<RowVectorPtr>(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::vector<RowVectorPtr>(buildVectors))
        .buildFilter("u0 < 0")
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter("u1 > t1")
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u0 < 0 AND u.u0 = t.t0)")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          // Verify spilling is not triggered in case of null-aware anti-join
          // with filter.
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilterAndNullKey) {
  auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
            makeFlatVector<int32_t>({0, 1, 2}),
        });
  });
  auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 2, 3}),
            makeFlatVector<int32_t>({0, 2, 3}),
        });
  });

  std::vector<std::string> filters({"u1 > t1", "u1 * t1 > 0"});
  for (const std::string& filter : filters) {
    const auto referenceSql = fmt::format(
        "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE {})",
        filter);

    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter(filter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          // Verify spilling is not triggered in case of null-aware anti-join
          // with filter.
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }
}

TEST_P(
    MultiThreadedHashJoinTest,
    hashModeNullAwareAntiJoinWithFilterAndNullKey) {
  // Use float type keys to trigger hash mode table.
  auto probeVectors = makeBatches(50, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<float>({std::nullopt, 1, 2}),
            makeFlatVector<int32_t>({1, 1, 2}),
        });
  });
  auto buildVectors = makeBatches(5, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<float>({std::nullopt, 2, 3}),
            makeFlatVector<int32_t>({0, 2, 3}),
        });
  });

  std::vector<std::string> filters({"u1 < t1", "u1 + t1 = 0"});
  for (const std::string& filter : filters) {
    const auto referenceSql = fmt::format(
        "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE {})",
        filter);

    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter(filter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilterOnNullableColumn) {
  const std::string referenceSql =
      "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE t1 <> u1)";
  const std::string joinFilter = "t1 <> u1";
  {
    SCOPED_TRACE("null filter column");
    auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeFlatVector<int32_t>(200, [](auto row) { return row % 11; }),
              makeFlatVector<int32_t>(200, folly::identity, nullEvery(97)),
          });
    });
    auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeFlatVector<int32_t>(234, [](auto row) { return row % 5; }),
              makeFlatVector<int32_t>(234, folly::identity, nullEvery(91)),
          });
    });
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter(joinFilter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          // Verify spilling is not triggered in case of null-aware anti-join
          // with filter.
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }

  {
    SCOPED_TRACE("null filter and key column");
    auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeFlatVector<int32_t>(
                  200, [](auto row) { return row % 11; }, nullEvery(23)),
              makeFlatVector<int32_t>(200, folly::identity, nullEvery(29)),
          });
    });
    auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeFlatVector<int32_t>(
                  234, [](auto row) { return row % 5; }, nullEvery(31)),
              makeFlatVector<int32_t>(234, folly::identity, nullEvery(37)),
          });
    });
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kAnti)
        .nullAware(true)
        .joinFilter(joinFilter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          // Verify spilling is not triggered in case of null-aware anti-join
          // with filter.
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, antiJoin) {
  auto probeVectors = makeBatches(64, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
            makeFlatVector<int32_t>({0, 1, 2}),
        });
  });
  auto buildVectors = makeBatches(64, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 2, 3}),
            makeFlatVector<int32_t>({0, 2, 3}),
        });
  });
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::vector<RowVectorPtr>(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::vector<RowVectorPtr>(buildVectors))
      .joinType(core::JoinType::kAnti)
      .joinOutputLayout({"t0", "t1"})
      .referenceQuery(
          "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.u0 = t.t0)")
      .run();

  std::vector<std::string> filters({
      "u1 > t1",
      "u1 * t1 > 0",
      // This filter is true on rows without a match. It should not prevent
      // the row from being returned.
      "coalesce(u1, t1, 0::integer) is not null",
      // This filter throws if evaluated on rows without a match. The join
      // should not evaluate filter on those rows and therefore should not
      // fail.
      "t1 / coalesce(u1, 0::integer) is not null",
      // This filter triggers memory pool allocation at
      // HashBuild::setupFilterForAntiJoins, which should not be invoked in
      // operator's constructor.
      "contains(array[1, 2, NULL], 1)",
  });
  for (const std::string& filter : filters) {
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::vector<RowVectorPtr>(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::vector<RowVectorPtr>(buildVectors))
        .joinType(core::JoinType::kAnti)
        .joinFilter(filter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(fmt::format(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.u0 = t.t0 AND {})",
            filter))
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, antiJoinWithFilterAndEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
              makeFlatVector<int32_t>({0, 1, 2}),
          });
    });
    auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeNullableFlatVector<int32_t>({3, 2, 3}),
              makeFlatVector<int32_t>({0, 2, 3}),
          });
    });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::vector<RowVectorPtr>(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::vector<RowVectorPtr>(buildVectors))
        .buildFilter("u0 < 0")
        .joinType(core::JoinType::kAnti)
        .joinFilter("u1 > t1")
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u0 < 0 AND u.u0 = t.t0)")
        .checkSpillStats(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_EQ(statsPair.first.spilledRows, 0);
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_EQ(statsPair.first.spilledFiles, 0);
          ASSERT_EQ(statsPair.second.spilledRows, 0);
          ASSERT_EQ(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 0);
          ASSERT_EQ(statsPair.second.spilledFiles, 0);
          verifyTaskSpilledRuntimeStats(*task, false);
          ASSERT_EQ(maxHashBuildSpillLevel(*task), -1);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftJoin) {
  // Left side keys are [0, 1, 2,..20].
  // Use 3-rd column as row number to allow for asserting the order of
  // results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 21; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 21; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "c1", "u_c0"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, t.c1, u.c0 FROM t LEFT JOIN u ON t.c0 = u.c0")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        int nullJoinBuildKeyCount = 0;
        int nullJoinProbeKeyCount = 0;

        for (auto& pipeline : task->taskStats().pipelineStats) {
          for (auto op : pipeline.operatorStats) {
            if (op.operatorType == "HashBuild") {
              nullJoinBuildKeyCount += op.numNullKeys;
            }
            if (op.operatorType == "HashProbe") {
              nullJoinProbeKeyCount += op.numNullKeys;
            }
          }
        }
        ASSERT_EQ(nullJoinBuildKeyCount, 33 * GetParam().numDrivers);
        ASSERT_EQ(nullJoinProbeKeyCount, 34 * GetParam().numDrivers);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, nullStatsWithEmptyBuild) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(1, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"c0", "c1", "row_number"},
            {
                makeFlatVector<int32_t>(
                    77, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                makeFlatVector<int32_t>(77, [](auto row) { return row; }),
            });
      });

  // All null keys on build side.
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(1, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                1, [](auto row) { return row % 5; }, nullEvery(1)),
            makeFlatVector<int32_t>(
                1, [](auto row) { return -111 + row * 2; }, nullEvery(1)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "c1", "u_c0"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, t.c1, u.c0 FROM t LEFT JOIN u ON t.c0 = u.c0")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        int nullJoinBuildKeyCount = 0;
        int nullJoinProbeKeyCount = 0;

        for (auto& pipeline : task->taskStats().pipelineStats) {
          for (auto op : pipeline.operatorStats) {
            if (op.operatorType == "HashBuild") {
              nullJoinBuildKeyCount += op.numNullKeys;
            }
            if (op.operatorType == "HashProbe") {
              nullJoinProbeKeyCount += op.numNullKeys;
            }
          }
        }
        // Due to inaccurate stats tracking in case of empty build side,
        // we will report 0 null keys on probe side.
        ASSERT_EQ(nullJoinProbeKeyCount, 0);
        ASSERT_EQ(nullJoinBuildKeyCount, 1 * GetParam().numDrivers);
      })
      .checkSpillStats(false)
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    // Left side keys are [0, 1, 2,..10].
    // Use 3-rd column as row number to allow for asserting the order of
    // results.
    std::vector<RowVectorPtr> probeVectors = mergeBatches(
        makeBatches(
            3,
            [&](int32_t /*unused*/) {
              return makeRowVector(
                  {"c0", "c1", "row_number"},
                  {
                      makeFlatVector<int32_t>(
                          77, [](auto row) { return row % 11; }, nullEvery(13)),
                      makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                      makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                  });
            }),
        makeBatches(
            2,
            [&](int32_t /*unused*/) {
              return makeRowVector(
                  {"c0", "c1", "row_number"},
                  {
                      makeFlatVector<int32_t>(
                          97,
                          [](auto row) { return (row + 3) % 11; },
                          nullEvery(13)),
                      makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                      makeFlatVector<int32_t>(
                          97, [](auto row) { return 97 + row; }),
                  });
            }),
        true);

    std::vector<RowVectorPtr> buildVectors =
        makeBatches(3, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  73, [](auto row) { return row % 5; }, nullEvery(7)),
              makeFlatVector<int32_t>(
                  73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
          });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(buildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .buildFilter("c0 < 0")
        .joinType(core::JoinType::kLeft)
        .joinOutputLayout({"row_number", "c1"})
        .referenceQuery(
            "SELECT t.row_number, t.c1 FROM t LEFT JOIN (SELECT c0 FROM u WHERE c0 < 0) u ON t.c0 = u.c0")
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithNoJoin) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of
  // results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 - 123::INTEGER AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "u_c1"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, u.c1 FROM t LEFT JOIN (SELECT c0 - 123::INTEGER AS u_c0, c1 FROM u) u ON t.c0 = u.u_c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithAllMatch) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of
  // results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .probeFilter("c0 < 5")
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, t.c1, u.c1 FROM (SELECT * FROM t WHERE c0 < 5) t LEFT JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithFilter) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of
  // results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  // Additional filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kLeft)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // No rows pass the additional filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kLeft)
        .joinFilter("(c1 + u_c1) % 2  = 3")
        .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

/// Tests left join with a filter that may evaluate to true, false or null.
/// Makes sure that null filter results are handled correctly, e.g. as if the
/// filter returned false.
TEST_P(MultiThreadedHashJoinTest, leftJoinWithNullableFilter) {
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          5,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
                makeNullableFlatVector<int32_t>(
                    {10, std::nullopt, 30, std::nullopt, 50}),
            });
          }),
      makeBatches(
          5,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
                makeNullableFlatVector<int32_t>(
                    {std::nullopt, 20, 30, std::nullopt, 50}),
            });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {makeFlatVector<int32_t>(128, [](vector_size_t row) {
              if (row < 3) {
                return row;
              }
              return row + 10;
            })});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0"})
      .joinType(core::JoinType::kLeft)
      .joinFilter("c1 + u_c0 > 0")
      .joinOutputLayout({"c0", "c1", "u_c0"})
      .referenceQuery(
          "SELECT * FROM t LEFT JOIN u ON (t.c0 = u.c0 AND t.c1 + u.c0 > 0)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoin) {
  // Left side keys are [0, 1, 2,..20].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 21; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kRight)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoinWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    // Left side keys are [0, 1, 2,..10].
    std::vector<RowVectorPtr> probeVectors = mergeBatches(
        makeBatches(
            3,
            [&](int32_t /*unused*/) {
              return makeRowVector({
                  makeFlatVector<int32_t>(
                      137, [](auto row) { return row % 11; }, nullEvery(13)),
                  makeFlatVector<int32_t>(137, [](auto row) { return row; }),
              });
            }),
        makeBatches(
            3,
            [&](int32_t /*unused*/) {
              return makeRowVector({
                  makeFlatVector<int32_t>(
                      234,
                      [](auto row) { return (row + 3) % 11; },
                      nullEvery(13)),
                  makeFlatVector<int32_t>(234, [](auto row) { return row; }),
              });
            }),
        true);

    // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
    std::vector<RowVectorPtr> buildVectors =
        makeBatches(3, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
              makeFlatVector<int32_t>(
                  123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
          });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(buildVectors))
        .buildFilter("c0 > 100")
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kRight)
        .joinOutputLayout({"c1"})
        .referenceQuery("SELECT null LIMIT 0")
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightJoinWithAllMatch) {
  // Left side keys are [0, 1, 2,..20].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 21; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("c0 >= 0")
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kRight)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN (SELECT * FROM u WHERE c0 >= 0) u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoinWithFilter) {
  // Left side keys are [0, 1, 2,..20].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 21; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  // Filter with passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kRight)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // Filter without passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kRight)
        .joinFilter("(c1 + u_c1) % 2 = 3")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, fullJoin) {
  // Left side keys are [0, 1, 2,..20].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 21; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 21; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1,
  // 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kFull)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithEmptyBuild) {
  const std::vector<bool> finishOnEmptys = {false, true};
  for (const auto finishOnEmpty : finishOnEmptys) {
    SCOPED_TRACE(fmt::format("finishOnEmpty: {}", finishOnEmpty));

    // Left side keys are [0, 1, 2,..10].
    std::vector<RowVectorPtr> probeVectors = mergeBatches(
        makeBatches(
            3,
            [&](int32_t /*unused*/) {
              return makeRowVector({
                  makeFlatVector<int32_t>(
                      213, [](auto row) { return row % 11; }, nullEvery(13)),
                  makeFlatVector<int32_t>(213, [](auto row) { return row; }),
              });
            }),
        makeBatches(
            2,
            [&](int32_t /*unused*/) {
              return makeRowVector({
                  makeFlatVector<int32_t>(
                      137,
                      [](auto row) { return (row + 3) % 11; },
                      nullEvery(13)),
                  makeFlatVector<int32_t>(137, [](auto row) { return row; }),
              });
            }),
        true);

    // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
    std::vector<RowVectorPtr> buildVectors =
        makeBatches(3, [&](int32_t /*unused*/) {
          return makeRowVector({
              makeFlatVector<int32_t>(
                  123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
              makeFlatVector<int32_t>(
                  123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
          });
        });

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .hashProbeFinishEarlyOnEmptyBuild(finishOnEmpty)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(buildVectors))
        .buildFilter("c0 > 100")
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kFull)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 > 100) u ON t.c0 = u.c0")
        .checkSpillStats(false)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithNoMatch) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("c0 < 0")
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kFull)
      .joinOutputLayout({"c1"})
      .referenceQuery(
          "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 < 0) u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithFilters) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  // Filter with passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kFull)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // Filter without passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kFull)
        .joinFilter("(c1 + u_c1) % 2 = 3")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, noSpillLevelLimit) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .keyTypes({INTEGER()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .maxSpillLevel(-1)
      .config(core::QueryConfig::kSpillStartPartitionBit, "51")
      .config(core::QueryConfig::kSpillNumPartitionBits, "3")
      .checkSpillStats(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        if (!hasSpill) {
          return;
        }
        ASSERT_EQ(maxHashBuildSpillLevel(*task), 3);
      })
      .run();
}

// Verify that dynamic filter pushed down is turned off for null-aware right
// semi project join.
TEST_F(HashJoinTest, nullAwareRightSemiProjectOverScan) {
  std::vector<RowVectorPtr> probes;
  std::vector<RowVectorPtr> builds;
  // Matches present:
  probes.push_back(makeRowVector(
      {"t0"},
      {
          makeNullableFlatVector<int32_t>({1, std::nullopt, 2}),
      }));
  builds.push_back(makeRowVector(
      {"u0"},
      {
          makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt}),
      }));

  // No matches present:
  probes.push_back(makeRowVector(
      {"t0"},
      {
          makeFlatVector<int32_t>({5, 6}),
      }));
  builds.push_back(makeRowVector(
      {"u0"},
      {
          makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt}),
      }));

  for (int i = 0; i < probes.size(); i++) {
    RowVectorPtr& probe = probes[i];
    RowVectorPtr& build = builds[i];
    std::shared_ptr<TempFilePath> probeFile = TempFilePath::create();
    writeToFile(probeFile->getPath(), {probe});

    std::shared_ptr<TempFilePath> buildFile = TempFilePath::create();
    writeToFile(buildFile->getPath(), {build});

    createDuckDbTable("t", {probe});
    createDuckDbTable("u", {build});

    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .tableScan(asRowType(probe->type()))
                    .capturePlanNodeId(probeScanId)
                    .hashJoin(
                        {"t0"},
                        {"u0"},
                        PlanBuilder(planNodeIdGenerator)
                            .tableScan(asRowType(build->type()))
                            .capturePlanNodeId(buildScanId)
                            .planNode(),
                        "",
                        {"u0", "match"},
                        core::JoinType::kRightSemiProject,
                        true /*nullAware*/)
                    .planNode();

    SplitInput splitInput = {
        {probeScanId,
         {exec::Split(makeHiveConnectorSplit(probeFile->getPath()))}},
        {buildScanId,
         {exec::Split(makeHiveConnectorSplit(buildFile->getPath()))}},
    };

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .inputSplits(splitInput)
        .checkSpillStats(false)
        .referenceQuery("SELECT u0, u0 IN (SELECT t0 FROM t) FROM u")
        .run();
  }
}

TEST_F(HashJoinTest, duplicateJoinKeys) {
  auto leftVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector({
        makeNullableFlatVector<int64_t>(
            {1, 2, 2, 3, 3, std::nullopt, 4, 5, 5, 6, 7}),
        makeNullableFlatVector<int64_t>(
            {1, 2, 2, std::nullopt, 3, 3, 4, 5, 5, 6, 8}),
    });
  });

  auto rightVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector({
        makeNullableFlatVector<int64_t>({1, 1, 3, 4, std::nullopt, 5, 7, 8}),
        makeNullableFlatVector<int64_t>({1, 1, 3, 4, 5, std::nullopt, 7, 8}),
    });
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", rightVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto assertPlan = [&](const std::vector<std::string>& leftProject,
                        const std::vector<std::string>& leftKeys,
                        const std::vector<std::string>& rightProject,
                        const std::vector<std::string>& rightKeys,
                        const std::vector<std::string>& outputLayout,
                        core::JoinType joinType,
                        const std::string& query) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(leftVectors)
                    .project(leftProject)
                    .hashJoin(
                        leftKeys,
                        rightKeys,
                        PlanBuilder(planNodeIdGenerator)
                            .values(rightVectors)
                            .project(rightProject)
                            .planNode(),
                        "",
                        outputLayout,
                        joinType)
                    .planNode();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .referenceQuery(query)
        .run();
  };

  std::vector<std::pair<core::JoinType, std::string>> joins = {
      {core::JoinType::kInner, "INNER JOIN"},
      {core::JoinType::kLeft, "LEFT JOIN"},
      {core::JoinType::kRight, "RIGHT JOIN"},
      {core::JoinType::kFull, "FULL OUTER JOIN"}};

  for (const auto& [joinType, joinTypeSql] : joins) {
    // Duplicate keys on the build side.
    assertPlan(
        {"c0 AS t0", "c1 as t1"}, // leftProject
        {"t0", "t1"}, // leftKeys
        {"c0 AS u0"}, // rightProject
        {"u0", "u0"}, // rightKeys
        {"t0", "t1", "u0"}, // outputLayout
        joinType,
        "SELECT t.c0, t.c1, u.c0 FROM t " + joinTypeSql +
            " u ON t.c0 = u.c0 and t.c1 = u.c0");
  }

  for (const auto& [joinType, joinTypeSql] : joins) {
    // Duplicated keys on the probe side.
    assertPlan(
        {"c0 AS t0"}, // leftProject
        {"t0", "t0"}, // leftKeys
        {"c0 AS u0", "c1 AS u1"}, // rightProject
        {"u0", "u1"}, // rightKeys
        {"t0", "u0", "u1"}, // outputLayout
        joinType,
        "SELECT t.c0, u.c0, u.c1 FROM t " + joinTypeSql +
            " u ON t.c0 = u.c0 and t.c0 = u.c1");
  }
}

TEST_F(HashJoinTest, semiProject) {
  // Some keys have multiple rows: 2, 3, 5.
  auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector({
        makeFlatVector<int64_t>({1, 2, 2, 3, 3, 3, 4, 5, 5, 6, 7}),
        makeFlatVector<int64_t>({10, 20, 21, 30, 31, 32, 40, 50, 51, 60, 70}),
    });
  });

  // Some keys are missing: 2, 6.
  // Some have multiple rows: 1, 5.
  // Some keys are not present on probe side: 8.
  auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector({
        makeFlatVector<int64_t>({1, 1, 3, 4, 5, 5, 7, 8}),
        makeFlatVector<int64_t>({100, 101, 300, 400, 500, 501, 700, 800}),
    });
  });

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors)
                  .project({"c0 AS t0", "c1 AS t1"})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors)
                          .project({"c0 AS u0", "c1 AS u1"})
                          .planNode(),
                      "",
                      {"t0", "t1", "match"},
                      core::JoinType::kLeftSemiProject)
                  .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0) FROM t")
      .run();

  // With extra filter.
  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plan = PlanBuilder(planNodeIdGenerator)
             .values(probeVectors)
             .project({"c0 AS t0", "c1 AS t1"})
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator)
                     .values(buildVectors)
                     .project({"c0 AS u0", "c1 AS u1"})
                     .planNode(),
                 "t1 * 10 <> u1",
                 {"t0", "t1", "match"},
                 core::JoinType::kLeftSemiProject)
             .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0 AND t.c1 * 10 <> u.c1) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0 AND t.c1 * 10 <> u.c1) FROM t")
      .run();

  // Empty build side.
  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plan = PlanBuilder(planNodeIdGenerator)
             .values(probeVectors)
             .project({"c0 AS t0", "c1 AS t1"})
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator)
                     .values(buildVectors)
                     .project({"c0 AS u0", "c1 AS u1"})
                     .filter("u0 < 0")
                     .planNode(),
                 "",
                 {"t0", "t1", "match"},
                 core::JoinType::kLeftSemiProject)
             .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE u.c0 < 0 AND t.c0 = u.c0) FROM t")
      // NOTE: there is no spilling in empty build test case as all the
      // build-side rows have been filtered out.
      .checkSpillStats(false)
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t.c0, t.c1, EXISTS (SELECT * FROM u WHERE u.c0 < 0 AND t.c0 = u.c0) FROM t")
      // NOTE: there is no spilling in empty build test case as all the
      // build-side rows have been filtered out.
      .checkSpillStats(false)
      .run();
}

TEST_F(HashJoinTest, semiProjectWithNullKeys) {
  // Some keys have multiple rows: 2, 3, 5.
  auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int64_t>(
                {1, 2, 2, 3, 3, 3, 4, std::nullopt, 5, 5, 6, 7}),
            makeFlatVector<int64_t>(
                {10, 20, 21, 30, 31, 32, 40, -1, 50, 51, 60, 70}),
        });
  });

  // Some keys are missing: 2, 6.
  // Some have multiple rows: 1, 5.
  // Some keys are not present on probe side: 8.
  auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int64_t>(
                {1, 1, 3, 4, std::nullopt, 5, 5, 7, 8}),
            makeFlatVector<int64_t>(
                {100, 101, 300, 400, -100, 500, 501, 700, 800}),
        });
  });

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto makePlan = [&](bool nullAware,
                      const std::string& probeFilter = "",
                      const std::string& buildFilter = "") {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    return PlanBuilder(planNodeIdGenerator)
        .values(probeVectors)
        .optionalFilter(probeFilter)
        .hashJoin(
            {"t0"},
            {"u0"},
            PlanBuilder(planNodeIdGenerator)
                .values(buildVectors)
                .optionalFilter(buildFilter)
                .planNode(),
            "",
            {"t0", "t1", "match"},
            core::JoinType::kLeftSemiProject,
            nullAware)
        .planNode();
  };

  // Null join keys on both sides.
  auto plan = makePlan(false /*nullAware*/);

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0) FROM t")
      .run();

  plan = makePlan(true /*nullAware*/);

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery("SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery("SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t")
      .run();

  // Null join keys on build side-only.
  plan = makePlan(false /*nullAware*/, "t0 IS NOT NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0) FROM t WHERE t0 IS NOT NULL")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0) FROM t WHERE t0 IS NOT NULL")
      .run();

  plan = makePlan(true /*nullAware*/, "t0 IS NOT NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t WHERE t0 IS NOT NULL")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t WHERE t0 IS NOT NULL")
      .run();

  // Null join keys on probe side-only.
  plan = makePlan(false /*nullAware*/, "", "u0 IS NOT NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 IS NOT NULL) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 IS NOT NULL) FROM t")
      .run();

  plan = makePlan(true /*nullAware*/, "", "u0 IS NOT NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 IS NOT NULL) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 IS NOT NULL) FROM t")
      .run();

  // Empty build side.
  plan = makePlan(false /*nullAware*/, "", "u0 < 0");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(plan)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 < 0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(flipJoinSides(plan))
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 < 0) FROM t")
      .run();

  plan = makePlan(true /*nullAware*/, "", "u0 < 0");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(plan)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 < 0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(flipJoinSides(plan))
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 < 0) FROM t")
      .run();

  // Build side with all rows having null join keys.
  plan = makePlan(false /*nullAware*/, "", "u0 IS NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(plan)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 IS NULL) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(flipJoinSides(plan))
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE u0 = t0 AND u0 IS NULL) FROM t")
      .run();

  plan = makePlan(true /*nullAware*/, "", "u0 IS NULL");

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(plan)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 IS NULL) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, executor_.get())
      .planNode(flipJoinSides(plan))
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE u0 IS NULL) FROM t")
      .run();
}

TEST_F(HashJoinTest, semiProjectWithFilter) {
  auto probeVectors = makeBatches(3, [&](auto /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt, 5}),
            makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
        });
  });

  auto buildVectors = makeBatches(3, [&](auto /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt}),
            makeFlatVector<int64_t>({11, 22, 33, 44}),
        });
  });

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto makePlan = [&](bool nullAware, const std::string& filter) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    return PlanBuilder(planNodeIdGenerator)
        .values(probeVectors)
        .hashJoin(
            {"t0"},
            {"u0"},
            PlanBuilder(planNodeIdGenerator).values(buildVectors).planNode(),
            filter,
            {"t0", "t1", "match"},
            core::JoinType::kLeftSemiProject,
            nullAware)
        .planNode();
  };

  std::vector<std::string> filters = {
      "t1 <> u1",
      "t1 < u1",
      "t1 > u1",
      "t1 is not null AND u1 is not null",
      "t1 is null OR u1 is null",
  };
  for (const auto& filter : filters) {
    auto plan = makePlan(true /*nullAware*/, filter);

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .referenceQuery(fmt::format(
            "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE {}) FROM t", filter))
        .injectSpill(false)
        .run();

    plan = makePlan(false /*nullAware*/, filter);

    // DuckDB Exists operator returns NULL when u0 or t0 is NULL. We exclude
    // these values.
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .referenceQuery(fmt::format(
            "SELECT t0, t1, EXISTS (SELECT * FROM u WHERE (u0 is not null OR t0 is not null) AND u0 = t0 AND {}) FROM t",
            filter))
        .injectSpill(false)
        .run();
  }
}

TEST_F(HashJoinTest, nullAwareRightSemiProjectWithFilterNotAllowed) {
  auto probe = makeRowVector(ROW({"t0", "t1"}, {INTEGER(), BIGINT()}), 10);
  auto build = makeRowVector(ROW({"u0", "u1"}, {INTEGER(), BIGINT()}), 10);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  VELOX_ASSERT_THROW(
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "t1 > u1",
              {"u0", "u1", "match"},
              core::JoinType::kRightSemiProject,
              true /* nullAware */),
      "Null-aware right semi project join doesn't support extra filter");
}

TEST_F(HashJoinTest, leftSemiJoinWithExtraOutputCapacity) {
  std::vector<RowVectorPtr> probeVectors;
  std::vector<RowVectorPtr> buildVectors;
  probeVectors.push_back(makeRowVector(
      {"t0", "t1"},
      {
          makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
          makeFlatVector<int64_t>({10, 10, 10, 10, 10, 10}),
      }));

  buildVectors.push_back(makeRowVector(
      {"u0", "u1"},
      {
          makeFlatVector<int32_t>({1, 1, 1, 1, 1}),
          makeFlatVector<int64_t>({10, 10, 10, 10, 10}),
      }));
  buildVectors.push_back(makeRowVector(
      {"u0", "u1"},
      {
          makeFlatVector<int32_t>({2, 3, 4, 5, 6}),
          makeFlatVector<int64_t>({10, 10, 10, 10, 10}),
      }));

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);
  auto runQuery = [&](const std::string& query,
                      const std::string& filter,
                      core::JoinType joinType) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    std::vector<std::string> outputLayout = {"t0", "t1"};
    if (joinType == core::JoinType::kLeftSemiProject) {
      outputLayout.push_back("match");
    }
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors)
                    .hashJoin(
                        {"t0"},
                        {"u0"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors)
                            .planNode(),
                        filter,
                        outputLayout,
                        joinType,
                        false)
                    .planNode();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .config(core::QueryConfig::kPreferredOutputBatchRows, "5")
        .referenceQuery(query)
        .injectSpill(false)
        .run();
  };
  {
    SCOPED_TRACE("left semi filter join");
    std::string filter = "t1 = u1";
    runQuery(
        fmt::format(
            "SELECT t0, t1 FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0 AND {})",
            filter),
        filter,
        core::JoinType::kLeftSemiFilter);
  }

  {
    SCOPED_TRACE("left semi project join");
    std::string filter = "t1 <> u1";
    runQuery(
        fmt::format(
            "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE {}) FROM t", filter),
        filter,
        core::JoinType::kLeftSemiProject);
  }
}

TEST_F(HashJoinTest, nullAwareMultiKeyNotAllowed) {
  auto probe = makeRowVector(
      ROW({"t0", "t1", "t2"}, {INTEGER(), BIGINT(), VARCHAR()}), 10);
  auto build = makeRowVector(
      ROW({"u0", "u1", "u2"}, {INTEGER(), BIGINT(), VARCHAR()}), 10);

  // Null-aware left semi project join.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  VELOX_ASSERT_THROW(
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0", "t1"},
              {"u0", "u1"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "",
              {"t0", "t1", "match"},
              core::JoinType::kLeftSemiProject,
              true /* nullAware */),
      "Null-aware joins allow only one join key");

  // Null-aware right semi project join.
  VELOX_ASSERT_THROW(
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0", "t1"},
              {"u0", "u1"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "",
              {"u0", "u1", "match"},
              core::JoinType::kRightSemiProject,
              true /* nullAware */),
      "Null-aware joins allow only one join key");

  // Null-aware anti join.
  VELOX_ASSERT_THROW(
      PlanBuilder(planNodeIdGenerator)
          .values({probe})
          .hashJoin(
              {"t0", "t1"},
              {"u0", "u1"},
              PlanBuilder(planNodeIdGenerator).values({build}).planNode(),
              "",
              {"t0", "t1"},
              core::JoinType::kAnti,
              true /* nullAware */),
      "Null-aware joins allow only one join key");
}

TEST_F(HashJoinTest, semiProjectOverLazyVectors) {
  auto probeVectors = makeBatches(1, [&](auto /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
            makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
        });
  });

  auto buildVectors = makeBatches(3, [&](auto /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(
                1'000, [](auto row) { return -100 + (row / 5); }),
            makeFlatVector<int64_t>(
                1'000, [](auto row) { return -1000 + (row / 5) * 10; }),
        });
  });

  std::shared_ptr<TempFilePath> probeFile = TempFilePath::create();
  writeToFile(probeFile->getPath(), probeVectors);

  std::shared_ptr<TempFilePath> buildFile = TempFilePath::create();
  writeToFile(buildFile->getPath(), buildVectors);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(probeVectors[0]->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(asRowType(buildVectors[0]->type()))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      "",
                      {"t0", "t1", "match"},
                      core::JoinType::kLeftSemiProject)
                  .planNode();

  SplitInput splitInput = {
      {probeScanId,
       {exec::Split(makeHiveConnectorSplit(probeFile->getPath()))}},
      {buildScanId,
       {exec::Split(makeHiveConnectorSplit(buildFile->getPath()))}},
  };

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery("SELECT t0, t1, t0 IN (SELECT u0 FROM u) FROM t")
      .run();

  // With extra filter.
  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plan = PlanBuilder(planNodeIdGenerator)
             .tableScan(asRowType(probeVectors[0]->type()))
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator)
                     .tableScan(asRowType(buildVectors[0]->type()))
                     .capturePlanNodeId(buildScanId)
                     .planNode(),
                 "(t1 + u1) % 3 = 0",
                 {"t0", "t1", "match"},
                 core::JoinType::kLeftSemiProject)
             .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(plan)
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE (t1 + u1) % 3 = 0) FROM t")
      .run();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(flipJoinSides(plan))
      .inputSplits(splitInput)
      .checkSpillStats(false)
      .referenceQuery(
          "SELECT t0, t1, t0 IN (SELECT u0 FROM u WHERE (t1 + u1) % 3 = 0) FROM t")
      .run();
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    HashJoinTest,
    MultiThreadedHashJoinTest,
    testing::ValuesIn(MultiThreadedHashJoinTest::getTestParams()));

// TODO: try to parallelize the following test cases if possible.
TEST_F(HashJoinTest, memory) {
  // Measures memory allocation in a 1:n hash join followed by
  // projection and aggregation. We expect vectors to be mostly
  // reused, except for t_k0 + 1, which is a dictionary after the
  // join.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return std::dynamic_pointer_cast<RowVector>(
            BatchMaker::createBatch(probeType_, 1000, *pool_));
      });

  // auto buildType = makeRowType(keyTypes, "u_");
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return std::dynamic_pointer_cast<RowVector>(
            BatchMaker::createBatch(buildType_, 1000, *pool_));
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(probeVectors, true)
                        .hashJoin(
                            {"t_k1"},
                            {"u_k1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(buildVectors, true)
                                .planNode(),
                            "",
                            concat(probeType_->names(), buildType_->names()))
                        .project({"t_k1 % 1000 AS k1", "u_k1 % 1000 AS k2"})
                        .singleAggregation({}, {"sum(k1)", "sum(k2)"})
                        .planNode();
  params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
  auto [taskCursor, rows] = readCursor(params);
  EXPECT_GT(3'500, params.queryCtx->pool()->stats().numAllocs);
  EXPECT_GT(40'000'000, params.queryCtx->pool()->stats().cumulativeBytes);
}

TEST_F(HashJoinTest, lazyVectors) {
  // a dataset of multiple row groups with multiple columns. We create
  // different dictionary wrappings for different columns and load the
  // rows in scope at different times.
  auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
    return makeRowVector(
        {makeFlatVector<int32_t>(3'000, [](auto row) { return row; }),
         makeFlatVector<int64_t>(30'000, [](auto row) { return row % 23; }),
         makeFlatVector<int32_t>(30'000, [](auto row) { return row % 31; }),
         makeFlatVector<StringView>(30'000, [](auto row) {
           return StringView::makeInline(fmt::format("{}   string", row % 43));
         })});
  });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(4, [&](int32_t /*unused*/) {
        return makeRowVector(
            {makeFlatVector<int32_t>(1'000, [](auto row) { return row * 3; }),
             makeFlatVector<int64_t>(
                 10'000, [](auto row) { return row % 31; })});
      });

  std::vector<std::shared_ptr<TempFilePath>> tempFiles;

  for (const auto& probeVector : probeVectors) {
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->getPath(), probeVector);
  }
  createDuckDbTable("t", probeVectors);

  for (const auto& buildVector : buildVectors) {
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->getPath(), buildVector);
  }
  createDuckDbTable("u", buildVectors);

  auto makeInputSplits = [&](const core::PlanNodeId& probeScanId,
                             const core::PlanNodeId& buildScanId) {
    return [&] {
      std::vector<exec::Split> probeSplits;
      for (int i = 0; i < probeVectors.size(); ++i) {
        probeSplits.push_back(
            exec::Split(makeHiveConnectorSplit(tempFiles[i]->getPath())));
      }
      std::vector<exec::Split> buildSplits;
      for (int i = 0; i < buildVectors.size(); ++i) {
        buildSplits.push_back(exec::Split(makeHiveConnectorSplit(
            tempFiles[probeSplits.size() + i]->getPath())));
      }
      SplitInput splits;
      splits.emplace(probeScanId, probeSplits);
      splits.emplace(buildScanId, buildSplits);
      return splits;
    };
  };

  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(ROW({"c0"}, {INTEGER()}))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      "",
                      {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId, buildScanId))
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .run();
  }

  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      ROW({"c0", "c1", "c2", "c3"},
                          {INTEGER(), BIGINT(), INTEGER(), VARCHAR()}))
                  .capturePlanNodeId(probeScanId)
                  .filter("c2 < 29")
                  .hashJoin(
                      {"c0"},
                      {"bc0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                          .capturePlanNodeId(buildScanId)
                          .project({"c0 as bc0", "c1 as bc1"})
                          .planNode(),
                      "(c1 + bc1) % 33 < 27",
                      {"c1", "bc1", "c3"})
                  .project({"c1 + 1", "bc1", "length(c3)"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId, buildScanId))
        .referenceQuery(
            "SELECT t.c1 + 1, U.c1, length(t.c3) FROM t, u WHERE t.c0 = u.c0 and t.c2 < 29 and (t.c1 + u.c1) % 33 < 27")
        .run();
  }
}

TEST_F(HashJoinTest, lazyVectorNotLoadedInFilter) {
  // Ensure that if lazy vectors are temporarily wrapped during a filter's
  // execution and remain unloaded, the temporary wrap is promptly
  // discarded. This precaution prevents the generation of the probe's output
  // from wrapping an unloaded vector while the temporary wrap is
  // still alive.
  // This is done by generating a sufficiently small batch to allow the lazy
  // vector to remain unloaded, as it doesn't need to be split between batches.
  // Then we use a filter that skips the execution of the expression containing
  // the lazy vector, thereby avoiding its loading.

  testLazyVectorsWithFilter(
      core::JoinType::kInner,
      "c1 >= 0 OR c2 > 0",
      {"c1", "c2"},
      "SELECT t.c1, t.c2 FROM t, u WHERE t.c0 = u.c0");
}

TEST_F(HashJoinTest, lazyVectorPartiallyLoadedInFilterLeftJoin) {
  // Test the case where a filter loads a subset of the rows that will be output
  // from a column on the probe side.

  testLazyVectorsWithFilter(
      core::JoinType::kLeft,
      "c1 > 0 AND c2 > 0",
      {"c1", "c2"},
      "SELECT t.c1, t.c2 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (c1 > 0 AND c2 > 0)");
}

TEST_F(HashJoinTest, lazyVectorPartiallyLoadedInFilterFullJoin) {
  // Test the case where a filter loads a subset of the rows that will be output
  // from a column on the probe side.

  testLazyVectorsWithFilter(
      core::JoinType::kFull,
      "c1 > 0 AND c2 > 0",
      {"c1", "c2"},
      "SELECT t.c1, t.c2 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (c1 > 0 AND c2 > 0)");
}

TEST_F(HashJoinTest, lazyVectorPartiallyLoadedInFilterLeftSemiProject) {
  // Test the case where a filter loads a subset of the rows that will be output
  // from a column on the probe side.

  testLazyVectorsWithFilter(
      core::JoinType::kLeftSemiProject,
      "c1 > 0 AND c2 > 0",
      {"c1", "c2", "match"},
      "SELECT t.c1, t.c2, EXISTS (SELECT * FROM u WHERE t.c0 = u.c0 AND (t.c1 > 0 AND t.c2 > 0)) FROM t");
}

TEST_F(HashJoinTest, lazyVectorPartiallyLoadedInFilterAntiJoin) {
  // Test the case where a filter loads a subset of the rows that will be output
  // from a column on the probe side.

  testLazyVectorsWithFilter(
      core::JoinType::kAnti,
      "c1 > 0 AND c2 > 0",
      {"c1", "c2"},
      "SELECT t.c1, t.c2 FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE t.c0 = u.c0 AND (t.c1 > 0 AND t.c2 > 0))");
}

TEST_F(HashJoinTest, lazyVectorPartiallyLoadedInFilterInnerJoin) {
  // Test the case where a filter loads a subset of the rows that will be output
  // from a column on the probe side.

  testLazyVectorsWithFilter(
      core::JoinType::kInner,
      "not (c1 < 15 and c2 >= 0)",
      {"c1", "c2"},
      "SELECT t.c1, t.c2 FROM t, u WHERE t.c0 = u.c0 AND NOT (c1 < 15 AND c2 >= 0)");
}

TEST_F(HashJoinTest, lazyVectorPartiallyLoadedInFilterLeftSemiFilter) {
  // Test the case where a filter loads a subset of the rows that will be output
  // from a column on the probe side.

  testLazyVectorsWithFilter(
      core::JoinType::kLeftSemiFilter,
      "not (c1 < 15 and c2 >= 0)",
      {"c1", "c2"},
      "SELECT t.c1, t.c2 FROM t WHERE c0 IN (SELECT u.c0 FROM u WHERE t.c0 = u.c0 AND NOT (t.c1 < 15 AND t.c2 >= 0))");
}

TEST_F(HashJoinTest, dynamicFilters) {
  const int32_t numSplits = 10;
  const int32_t numRowsProbe = 333;
  const int32_t numRowsBuild = 100;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);

  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  for (int32_t i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numRowsProbe, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numRowsProbe, [](auto row) { return row; }),
    });
    probeVectors.push_back(rowVector);
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->getPath(), rowVector);
  }
  auto makeInputSplits = [&](const core::PlanNodeId& nodeId) {
    return [&] {
      std::vector<exec::Split> probeSplits;
      for (auto& file : tempFiles) {
        probeSplits.push_back(
            exec::Split(makeHiveConnectorSplit(file->getPath())));
      }
      SplitInput splits;
      splits.emplace(nodeId, probeSplits);
      return splits;
    };
  };

  // 100 key values in [35, 233] range.
  std::vector<RowVectorPtr> buildVectors;
  for (int i = 0; i < 5; ++i) {
    buildVectors.push_back(makeRowVector({
        makeFlatVector<int32_t>(
            numRowsBuild / 5,
            [i](auto row) { return 35 + 2 * (row + i * numRowsBuild / 5); }),
        makeFlatVector<int64_t>(numRowsBuild / 5, [](auto row) { return row; }),
    }));
  }
  std::vector<RowVectorPtr> keyOnlyBuildVectors;
  for (int i = 0; i < 5; ++i) {
    keyOnlyBuildVectors.push_back(
        makeRowVector({makeFlatVector<int32_t>(numRowsBuild / 5, [i](auto row) {
          return 35 + 2 * (row + i * numRowsBuild / 5);
        })}));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .values(buildVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();
  auto keyOnlyBuildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                              .values(keyOnlyBuildVectors)
                              .project({"c0 AS u_c0"})
                              .planNode();

  // Basic push-down.
  {
    SCOPED_TRACE("Inner join");
    core::PlanNodeId probeScanId;
    core::PlanNodeId joinId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "",
                      {"c0", "c1", "u_c1"},
                      core::JoinType::kInner)
                  .capturePlanNodeId(joinId)
                  .project({"c0", "c1 + 1", "c1 + u_c1"})
                  .planNode();
    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            auto planStats = toPlanStats(task->taskStats());
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_EQ(
                  planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                  std::unordered_set<core::PlanNodeId>({joinId}));
            }
          })
          .run();
    }

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType)
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kLeftSemiFilter)
             .capturePlanNodeId(joinId)
             .project({"c0", "c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            auto planStats = toPlanStats(task->taskStats());
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_EQ(
                  planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                  std::unordered_set<core::PlanNodeId>({joinId}));
            }
          })
          .run();
    }

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType)
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"u_c0", "u_c1"},
                 core::JoinType::kRightSemiFilter)
             .capturePlanNodeId(joinId)
             .project({"u_c0", "u_c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT u.c0, u.c1 + 1 FROM u WHERE u.c0 IN (SELECT c0 FROM t)")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            auto planStats = toPlanStats(task->taskStats());
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_EQ(
                  planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                  std::unordered_set<core::PlanNodeId>({joinId}));
            }
          })
          .run();
    }

    // Right join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType)
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1", "u_c1"},
                 core::JoinType::kRight)
             .capturePlanNodeId(joinId)
             .project({"c0", "c1 + 1", "c1 + u_c1"})
             .planNode();
    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            auto planStats = toPlanStats(task->taskStats());
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_EQ(
                  planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                  std::unordered_set<core::PlanNodeId>({joinId}));
            }
          })
          .run();
    }
  }

  // Basic push-down with column names projected out of the table scan
  // having different names than column names in the files.
  {
    SCOPED_TRACE("Inner join column rename");
    auto scanOutputType = ROW({"a", "b"}, {INTEGER(), BIGINT()});
    connector::ColumnHandleMap assignments;
    assignments["a"] = regularColumn("c0", INTEGER());
    assignments["b"] = regularColumn("c1", BIGINT());

    core::PlanNodeId probeScanId;
    core::PlanNodeId joinId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .startTableScan()
                  .outputType(scanOutputType)
                  .assignments(assignments)
                  .endTableScan()
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"a"}, {"u_c0"}, buildSide, "", {"a", "b", "u_c1"})
                  .capturePlanNodeId(joinId)
                  .project({"a", "b + 1", "b + u_c1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery(
            "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          auto planStats = toPlanStats(task->taskStats());
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_EQ(
                planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                std::unordered_set<core::PlanNodeId>({joinId}));
          }
        })
        .run();
  }

  // Push-down that requires merging filters.
  {
    SCOPED_TRACE("Merge filters");
    core::PlanNodeId probeScanId;
    core::PlanNodeId joinId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType, {"c0 < 500::INTEGER"})
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1", "u_c1"})
                  .capturePlanNodeId(joinId)
                  .project({"c1 + u_c1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery(
            "SELECT t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          auto planStats = toPlanStats(task->taskStats());
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_EQ(
                planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                std::unordered_set<core::PlanNodeId>({joinId}));
          }
        })
        .run();
  }

  // Push-down that turns join into a no-op.
  {
    SCOPED_TRACE("canReplaceWithDynamicFilter");
    core::PlanNodeId probeScanId;
    core::PlanNodeId joinId;
    auto op =
        PlanBuilder(planNodeIdGenerator, pool_.get())
            .tableScan(probeType)
            .capturePlanNodeId(probeScanId)
            .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c0", "c1"})
            .capturePlanNodeId(joinId)
            .project({"c0", "c1 + 1"})
            .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery("SELECT t.c0, t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          auto planStats = toPlanStats(task->taskStats());
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(
                getReplacedWithFilterRows(task, 1).sum,
                numRowsBuild * numSplits);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_EQ(
                planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                std::unordered_set<core::PlanNodeId>({joinId}));
          }
        })
        .run();
  }

  // Push-down that turns join into a no-op with output having a different
  // number of columns than the input.
  {
    SCOPED_TRACE("canReplaceWithDynamicFilter column rename");
    core::PlanNodeId probeScanId;
    core::PlanNodeId joinId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c0"})
                  .capturePlanNodeId(joinId)
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery("SELECT t.c0 FROM t JOIN u ON (t.c0 = u.c0)")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          auto planStats = toPlanStats(task->taskStats());
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(
                getReplacedWithFilterRows(task, 1).sum,
                numRowsBuild * numSplits);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_EQ(
                planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                std::unordered_set<core::PlanNodeId>({joinId}));
          }
        })
        .run();
  }

  // Push-down that requires merging filters and turns join into a no-op.
  {
    SCOPED_TRACE("canReplaceWithDynamicFilter merge filters");
    core::PlanNodeId probeScanId;
    core::PlanNodeId joinId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType, {"c0 < 500::INTEGER"})
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c1"})
                  .capturePlanNodeId(joinId)
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery(
            "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
          auto planStats = toPlanStats(task->taskStats());
          if (hasSpill) {
            // Dynamic filtering should be disabled with spilling triggered.
            ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_EQ(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
          } else {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
            ASSERT_EQ(
                planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                std::unordered_set<core::PlanNodeId>({joinId}));
          }
        })
        .run();
  }

  // Push-down with highly selective filter in the scan.
  {
    SCOPED_TRACE("Highly selective filter");
    // Inner join.
    core::PlanNodeId probeScanId;
    core::PlanNodeId joinId;
    auto op =
        PlanBuilder(planNodeIdGenerator, pool_.get())
            .tableScan(probeType, {"c0 < 200::INTEGER"})
            .capturePlanNodeId(probeScanId)
            .hashJoin(
                {"c0"}, {"u_c0"}, buildSide, "", {"c1"}, core::JoinType::kInner)
            .capturePlanNodeId(joinId)
            .project({"c1 + 1"})
            .planNode();

    {
      SCOPED_TRACE("Inner join");
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            auto planStats = toPlanStats(task->taskStats());
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_EQ(
                  planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                  std::unordered_set<core::PlanNodeId>({joinId}));
            }
          })
          .run();
    }

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType, {"c0 < 200::INTEGER"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c1"},
                 core::JoinType::kLeftSemiFilter)
             .capturePlanNodeId(joinId)
             .project({"c1 + 1"})
             .planNode();

    {
      SCOPED_TRACE("Left semi join");
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            auto planStats = toPlanStats(task->taskStats());
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_EQ(
                  planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                  std::unordered_set<core::PlanNodeId>({joinId}));
            }
          })
          .run();
    }

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType, {"c0 < 200::INTEGER"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"u_c1"},
                 core::JoinType::kRightSemiFilter)
             .capturePlanNodeId(joinId)
             .project({"u_c1 + 1"})
             .planNode();

    {
      SCOPED_TRACE("Right semi join");
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT u.c1 + 1 FROM u WHERE u.c0 IN (SELECT c0 FROM t) AND u.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            auto planStats = toPlanStats(task->taskStats());
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_EQ(
                  planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                  std::unordered_set<core::PlanNodeId>({joinId}));
            }
          })
          .run();
    }

    // Right join.
    op =
        PlanBuilder(planNodeIdGenerator, pool_.get())
            .tableScan(probeType, {"c0 < 200::INTEGER"})
            .capturePlanNodeId(probeScanId)
            .hashJoin(
                {"c0"}, {"u_c0"}, buildSide, "", {"c1"}, core::JoinType::kRight)
            .capturePlanNodeId(joinId)
            .project({"c1 + 1"})
            .planNode();

    {
      SCOPED_TRACE("Right join");
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c1 + 1 FROM (SELECT * FROM t WHERE t.c0 < 200) t RIGHT JOIN u ON t.c0 = u.c0")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            auto planStats = toPlanStats(task->taskStats());
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
              ASSERT_EQ(
                  planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
                  std::unordered_set<core::PlanNodeId>({joinId}));
            }
          })
          .run();
    }
  }

  // Disable filter push-down by using values in place of scan.
  {
    SCOPED_TRACE("Disabled in case of values node");
    core::PlanNodeId joinId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .values(probeVectors)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1"})
                  .capturePlanNodeId(joinId)
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          auto planStats = toPlanStats(task->taskStats());
          ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(numRowsProbe * numSplits, getInputPositions(task, 1));
        })
        .run();
  }

  // Disable filter push-down by using an expression as the join key on the
  // probe side.
  {
    SCOPED_TRACE("Disabled in case of join condition");
    core::PlanNodeId probeScanId;
    core::PlanNodeId joinId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .project({"cast(c0 + 1 as integer) AS t_key", "c1"})
                  .hashJoin({"t_key"}, {"u_c0"}, buildSide, "", {"c1"})
                  .capturePlanNodeId(joinId)
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .makeInputSplits(makeInputSplits(probeScanId))
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE (t.c0 + 1) = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          auto planStats = toPlanStats(task->taskStats());
          ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(numRowsProbe * numSplits, getInputPositions(task, 1));
          ASSERT_TRUE(planStats.at(probeScanId).dynamicFilterStats.empty());
        })
        .run();
  }
}

TEST_F(HashJoinTest, dynamicFiltersStatsWithChainedJoins) {
  const int32_t numSplits = 10;
  const int32_t numProbeRows = 333;
  const int32_t numBuildRows = 100;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);
  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  for (int32_t i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numProbeRows, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numProbeRows, [](auto row) { return row; }),
    });
    probeVectors.push_back(rowVector);
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->getPath(), rowVector);
  }
  auto makeInputSplits = [&](const core::PlanNodeId& nodeId) {
    return [&] {
      std::vector<exec::Split> probeSplits;
      for (auto& file : tempFiles) {
        probeSplits.push_back(
            exec::Split(makeHiveConnectorSplit(file->getPath())));
      }
      SplitInput splits;
      splits.emplace(nodeId, probeSplits);
      return splits;
    };
  };

  // 100 key values in [35, 233] range.
  std::vector<RowVectorPtr> buildVectors;
  for (int i = 0; i < 5; ++i) {
    buildVectors.push_back(makeRowVector({
        makeFlatVector<int32_t>(
            numBuildRows / 5,
            [i](auto row) { return 35 + 2 * (row + i * numBuildRows / 5); }),
        makeFlatVector<int64_t>(numBuildRows / 5, [](auto row) { return row; }),
    }));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide1 = PlanBuilder(planNodeIdGenerator, pool_.get())
                        .values(buildVectors)
                        .project({"c0 AS u_c0", "c1 AS u_c1"})
                        .planNode();
  auto buildSide2 = PlanBuilder(planNodeIdGenerator, pool_.get())
                        .values(buildVectors)
                        .project({"c0 AS u_c0", "c1 AS u_c1"})
                        .planNode();
  // Inner join pushdown.
  core::PlanNodeId probeScanId;
  core::PlanNodeId joinId1;
  core::PlanNodeId joinId2;
  auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                .tableScan(probeType)
                .capturePlanNodeId(probeScanId)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide1,
                    "",
                    {"c0", "c1"},
                    core::JoinType::kInner)
                .capturePlanNodeId(joinId1)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide2,
                    "",
                    {"c0", "c1", "u_c1"},
                    core::JoinType::kInner)
                .capturePlanNodeId(joinId2)
                .project({"c0", "c1 + 1", "c1 + u_c1"})
                .planNode();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(op))
      .makeInputSplits(makeInputSplits(probeScanId))
      .injectSpill(false)
      .referenceQuery(
          "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        auto planStats = toPlanStats(task->taskStats());
        ASSERT_EQ(
            planStats.at(probeScanId).dynamicFilterStats.producerNodeIds,
            std::unordered_set<core::PlanNodeId>({joinId1, joinId2}));
      })
      .run();
}

TEST_F(HashJoinTest, dynamicFiltersWithSkippedSplits) {
  const int32_t numSplits = 20;
  const int32_t numNonSkippedSplits = 10;
  const int32_t numRowsProbe = 333;
  const int32_t numRowsBuild = 100;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);

  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  // Each split has a column containing
  // the split number. This is used to filter out whole splits based
  // on metadata. We test how using metadata for dropping splits
  // interactts with dynamic filters. In specific, if the first split
  // is discarded based on metadata, the dynamic filters must not be
  // lost even if there is no actual reader for the split.
  for (int32_t i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numRowsProbe, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numRowsProbe, [](auto row) { return row; }),
        makeFlatVector<int64_t>(
            numRowsProbe, [&](auto /*row*/) { return i % 2 == 0 ? 0 : i; }),
    });
    probeVectors.push_back(rowVector);
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->getPath(), rowVector);
  }

  auto makeInputSplits = [&](const core::PlanNodeId& nodeId) {
    return [&] {
      std::vector<exec::Split> probeSplits;
      for (auto& file : tempFiles) {
        probeSplits.push_back(
            exec::Split(makeHiveConnectorSplit(file->getPath())));
      }
      // We add splits that have no rows.
      auto makeEmpty = [&]() {
        return exec::Split(
            HiveConnectorSplitBuilder(tempFiles.back()->getPath())
                .start(10000000)
                .length(1)
                .build());
      };
      std::vector<exec::Split> emptyFront = {makeEmpty(), makeEmpty()};
      std::vector<exec::Split> emptyMiddle = {makeEmpty(), makeEmpty()};
      probeSplits.insert(
          probeSplits.begin(), emptyFront.begin(), emptyFront.end());
      probeSplits.insert(
          probeSplits.begin() + 13, emptyMiddle.begin(), emptyMiddle.end());
      SplitInput splits;
      splits.emplace(nodeId, probeSplits);
      return splits;
    };
  };

  // 100 key values in [35, 233] range.
  std::vector<RowVectorPtr> buildVectors;
  for (int i = 0; i < 5; ++i) {
    buildVectors.push_back(makeRowVector({
        makeFlatVector<int32_t>(
            numRowsBuild / 5,
            [i](auto row) { return 35 + 2 * (row + i * numRowsBuild / 5); }),
        makeFlatVector<int64_t>(numRowsBuild / 5, [](auto row) { return row; }),
    }));
  }
  std::vector<RowVectorPtr> keyOnlyBuildVectors;
  for (int i = 0; i < 5; ++i) {
    keyOnlyBuildVectors.push_back(
        makeRowVector({makeFlatVector<int32_t>(numRowsBuild / 5, [i](auto row) {
          return 35 + 2 * (row + i * numRowsBuild / 5);
        })}));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto probeType = ROW({"c0", "c1", "c2"}, {INTEGER(), BIGINT(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .values(buildVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();
  auto keyOnlyBuildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                              .values(keyOnlyBuildVectors)
                              .project({"c0 AS u_c0"})
                              .planNode();

  // Basic push-down.
  {
    // Inner join.
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(probeType, {"c2 > 0"})
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "",
                      {"c0", "c1", "u_c1"},
                      core::JoinType::kInner)
                  .project({"c0", "c1 + 1", "c1 + u_c1"})
                  .planNode();
    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .numDrivers(1)
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0 AND t.c2 > 0")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_LT(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            }
          })
          .run();
    }

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType, {"c2 > 0"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kLeftSemiFilter)
             .project({"c0", "c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .numDrivers(1)
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c2 > 0")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
              ASSERT_EQ(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            }
          })
          .run();
    }

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tableScan(probeType, {"c2 > 0"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"u_c0", "u_c1"},
                 core::JoinType::kRightSemiFilter)
             .project({"u_c0", "u_c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .planNode(std::move(op))
          .numDrivers(1)
          .makeInputSplits(makeInputSplits(probeScanId))
          .referenceQuery(
              "SELECT u.c0, u.c1 + 1 FROM u WHERE u.c0 IN (SELECT c0 FROM t WHERE t.c2 > 0)")
          .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
            SCOPED_TRACE(fmt::format("hasSpill:{}", hasSpill));
            if (hasSpill) {
              // Dynamic filtering should be disabled with spilling triggered.
              ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_EQ(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            } else {
              ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
              ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
              ASSERT_EQ(getReplacedWithFilterRows(task, 1).sum, 0);
              ASSERT_LT(
                  getInputPositions(task, 1),
                  numRowsProbe * numNonSkippedSplits);
            }
          })
          .run();
    }
  }
}

TEST_F(HashJoinTest, dynamicFiltersAppliedToPreloadedSplits) {
  vector_size_t size = 1000;
  const int32_t numSplits = 5;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);

  // Prepare probe side table.
  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  std::vector<exec::Split> probeSplits;
  for (int32_t i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector(
        {"p0", "p1"},
        {
            makeFlatVector<int64_t>(
                size, [&](auto row) { return (row + 1) * (i + 1); }),
            makeFlatVector<int64_t>(size, [&](auto /*row*/) { return i; }),
        });
    probeVectors.push_back(rowVector);
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->getPath(), rowVector);
    auto split = HiveConnectorSplitBuilder(tempFiles.back()->getPath())
                     .partitionKey("p1", std::to_string(i))
                     .build();
    probeSplits.push_back(exec::Split(split));
  }

  auto outputType = ROW({"p0", "p1"}, {BIGINT(), BIGINT()});
  connector::ColumnHandleMap assignments = {
      {"p0", regularColumn("p0", BIGINT())},
      {"p1", partitionKey("p1", BIGINT())}};
  createDuckDbTable("p", probeVectors);

  // Prepare build side table.
  std::vector<RowVectorPtr> buildVectors{
      makeRowVector({"b0"}, {makeFlatVector<int64_t>({0, numSplits})})};
  createDuckDbTable("b", buildVectors);

  // Executing the join with p1=b0, we expect a dynamic filter for p1 to prune
  // the entire file/split. There are total of five splits, and all except the
  // first one are expected to be pruned. The result 'preloadedSplits' > 1
  // confirms the successful push of dynamic filters to the preloading data
  // source.
  core::PlanNodeId probeScanId;
  core::PlanNodeId joinNodeId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op =
      PlanBuilder(planNodeIdGenerator)
          .startTableScan()
          .outputType(outputType)
          .assignments(assignments)
          .endTableScan()
          .capturePlanNodeId(probeScanId)
          .hashJoin(
              {"p1"},
              {"b0"},
              PlanBuilder(planNodeIdGenerator).values(buildVectors).planNode(),
              "",
              {"p0"},
              core::JoinType::kInner)
          .capturePlanNodeId(joinNodeId)
          .project({"p0"})
          .planNode();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(op))
      .config(core::QueryConfig::kMaxSplitPreloadPerDriver, "3")
      .injectSpill(false)
      .inputSplits({{probeScanId, probeSplits}})
      .referenceQuery("select p.p0 from p, b where b.b0 = p.p1")
      .checkSpillStats(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool /*hasSpill*/) {
        auto planStats = toPlanStats(task->taskStats());
        auto getStatSum = [&](const core::PlanNodeId& id,
                              const std::string& name) {
          return planStats.at(id).customStats.at(name).sum;
        };
        ASSERT_EQ(1, getStatSum(joinNodeId, "dynamicFiltersProduced"));
        ASSERT_EQ(1, getStatSum(probeScanId, "dynamicFiltersAccepted"));
        ASSERT_EQ(4, getStatSum(probeScanId, "skippedSplits"));
        ASSERT_LT(1, getStatSum(probeScanId, "preloadedSplits"));
      })
      .run();
}

TEST_F(HashJoinTest, dynamicFiltersPushDownThroughAgg) {
  const int32_t numRowsProbe = 300;
  const int32_t numRowsBuild = 100;

  // Create probe data
  std::vector<RowVectorPtr> probeVectors{makeRowVector({
      makeFlatVector<int32_t>(numRowsProbe, [&](auto row) { return row - 10; }),
      makeFlatVector<int64_t>(numRowsProbe, folly::identity),
  })};
  std::shared_ptr<TempFilePath> probeFile = TempFilePath::create();
  writeToFile(probeFile->getPath(), probeVectors);

  // Create build data
  std::vector<RowVectorPtr> buildVectors{makeRowVector(
      {"u0"}, {makeFlatVector<int32_t>(numRowsBuild, [&](auto row) {
        return 35 + 2 * (row + numRowsBuild / 5);
      })})};

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto buildSide =
      PlanBuilder(planNodeIdGenerator).values(buildVectors).planNode();

  // Inner join.
  core::PlanNodeId scanNodeId;
  core::PlanNodeId joinNodeId;
  core::PlanNodeId aggNodeId;
  auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                .tableScan(probeType)
                .capturePlanNodeId(scanNodeId)
                .partialAggregation({"c0"}, {"sum(c1)"})
                .capturePlanNodeId(aggNodeId)
                .hashJoin(
                    {"c0"},
                    {"u0"},
                    buildSide,
                    "",
                    {"c0", "a0"},
                    core::JoinType::kInner)
                .capturePlanNodeId(joinNodeId)
                .planNode();

  SplitInput splitInput = {
      {scanNodeId, {Split(makeHiveConnectorSplit(probeFile->getPath()))}}};
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(op))
      .inputSplits(splitInput)
      .injectSpill(false)
      .checkSpillStats(false)
      .referenceQuery("SELECT c0, sum(c1) FROM t, u WHERE c0 = u0 group by c0")
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        auto planStats = toPlanStats(task->taskStats());
        auto dynamicFilterStats = planStats.at(scanNodeId).dynamicFilterStats;
        ASSERT_EQ(
            1, getFiltersProduced(task, getOperatorIndex(joinNodeId)).sum);
        ASSERT_EQ(
            1, getFiltersAccepted(task, getOperatorIndex(scanNodeId)).sum);
        ASSERT_LT(
            getInputPositions(task, getOperatorIndex(aggNodeId)), numRowsProbe);
        ASSERT_EQ(
            dynamicFilterStats.producerNodeIds,
            std::unordered_set({joinNodeId}));
      })
      .run();
}

TEST_F(HashJoinTest, noDynamicFiltersPushDownThroughRightJoin) {
  std::vector<RowVectorPtr> innerBuild = {makeRowVector(
      {"a"},
      {
          makeFlatVector<int64_t>(5, [](auto i) { return 2 * i; }),
      })};
  std::vector<RowVectorPtr> rightBuild = {makeRowVector(
      {"b"},
      {
          makeFlatVector<int64_t>(5, [](auto i) { return 1 + 2 * i; }),
      })};
  std::vector<RowVectorPtr> rightProbe = {makeRowVector(
      {"aa", "bb"},
      {
          makeFlatVector<int64_t>(10, folly::identity),
          makeFlatVector<int64_t>(10, folly::identity),
      })};
  auto file = TempFilePath::create();
  writeToFile(file->getPath(), rightProbe);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanNodeId;
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(asRowType(rightProbe[0]->type()))
          .capturePlanNodeId(scanNodeId)
          .hashJoin(
              {"bb"},
              {"b"},
              PlanBuilder(planNodeIdGenerator).values(rightBuild).planNode(),
              "",
              {"aa", "b"},
              core::JoinType::kRight)
          .hashJoin(
              {"aa"},
              {"a"},
              PlanBuilder(planNodeIdGenerator).values(innerBuild).planNode(),
              "",
              {"aa"})
          .planNode();
  AssertQueryBuilder(plan)
      .split(scanNodeId, Split(makeHiveConnectorSplit(file->getPath())))
      .assertResults(
          BaseVector::create<RowVector>(innerBuild[0]->type(), 0, pool_.get()));
}

// Verify the size of the join output vectors when projecting build-side
// variable-width column.
TEST_F(HashJoinTest, memoryUsage) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return makeRowVector(
            {makeFlatVector<int32_t>(1'000, [](auto row) { return row % 5; })});
      });
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"u_c0", "u_c1"},
            {makeFlatVector<int32_t>({0, 1, 2}),
             makeFlatVector<std::string>({
                 std::string(40, 'a'),
                 std::string(50, 'b'),
                 std::string(30, 'c'),
             })});
      });
  core::PlanNodeId joinNodeId;

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .planNode(),
                      "",
                      {"c0", "u_c1"})
                  .capturePlanNodeId(joinNodeId)
                  .singleAggregation({}, {"count(1)"})
                  .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(plan))
      .referenceQuery("SELECT 30000")
      .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
        if (hasSpill) {
          return;
        }
        auto planStats = toPlanStats(task->taskStats());
        auto outputBytes = planStats.at(joinNodeId).outputBytes;
        ASSERT_LT(outputBytes, ((40 + 50 + 30) / 3 + 8) * 1000 * 10 * 5);
        // Verify number of memory allocations. Should not be too high if
        // hash join is able to re-use output vectors that contain
        // build-side data.
        ASSERT_GT(40, task->pool()->stats().numAllocs);
      })
      .run();
}

/// Test an edge case in producing small output batches where the logic to
/// calculate the set of probe-side rows to load lazy vectors for was
/// triggering a crash.
TEST_F(HashJoinTest, smallOutputBatchSize) {
  // Setup probe data with 50 non-null matching keys followed by 50 null
  // keys: 1, 2, 1, 2,...null, null.
  auto probeVectors = makeRowVector({
      makeFlatVector<int32_t>(
          100,
          [](auto row) { return 1 + row % 2; },
          [](auto row) { return row > 50; }),
      makeFlatVector<int32_t>(100, [](auto row) { return row * 10; }),
  });

  // Setup build side to match non-null probe side keys.
  auto buildVectors = makeRowVector(
      {"u_c0", "u_c1"},
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<int32_t>({100, 200}),
      });

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  // Plan hash inner join with a filter.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({probeVectors})
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .planNode(),
                      "c1 < u_c1",
                      {"c0", "u_c1"})
                  .planNode();

  // Use small output batch size to trigger logic for calculating set of
  // probe-side rows to load lazy vectors for.
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(plan))
      .config(core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
      .referenceQuery("SELECT c0, u_c1 FROM t, u WHERE c0 = u_c0 AND c1 < u_c1")
      .injectSpill(false)
      .run();
}

TEST_F(HashJoinTest, spillFileSize) {
  const std::vector<uint64_t> maxSpillFileSizes({0, 1, 1'000'000'000});
  for (const auto spillFileSize : maxSpillFileSizes) {
    SCOPED_TRACE(fmt::format("spillFileSize: {}", spillFileSize));
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .keyTypes({BIGINT()})
        .probeVectors(100, 3)
        .buildVectors(100, 3)
        .referenceQuery(
            "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
        .config(core::QueryConfig::kSpillStartPartitionBit, "48")
        .config(core::QueryConfig::kSpillNumPartitionBits, "3")
        .config(
            core::QueryConfig::kMaxSpillFileSize, std::to_string(spillFileSize))
        .checkSpillStats(false)
        .maxSpillLevel(0)
        .verifier([&](const std::shared_ptr<Task>& task, bool hasSpill) {
          if (!hasSpill) {
            return;
          }
          const auto statsPair = taskSpilledStats(*task);
          const int32_t numPartitions = statsPair.first.spilledPartitions;
          ASSERT_EQ(statsPair.second.spilledPartitions, numPartitions);
          const auto fileSizes = numTaskSpillFiles(*task);
          if (spillFileSize != 1) {
            ASSERT_EQ(fileSizes.first, numPartitions);
          } else {
            ASSERT_GT(fileSizes.first, numPartitions);
          }
          verifyTaskSpilledRuntimeStats(*task, true);
        })
        .run();
  }
}

TEST_F(HashJoinTest, spillPartitionBitsOverlap) {
  auto builder =
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .keyTypes({BIGINT(), BIGINT()})
          .probeVectors(2'000, 3)
          .buildVectors(2'000, 3)
          .referenceQuery(
              "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM t, u WHERE t_k0 = u_k0 and t_k1 = u_k1")
          .config(core::QueryConfig::kSpillStartPartitionBit, "8")
          .config(core::QueryConfig::kSpillNumPartitionBits, "1")
          .checkSpillStats(false)
          .maxSpillLevel(0);
  VELOX_ASSERT_THROW(builder.run(), "vs. 8");
}

// The test is to verify if the hash build reservation has been released on
// task error.
DEBUG_ONLY_TEST_F(HashJoinTest, buildReservationReleaseCheck) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(1, [&](int32_t /*unused*/) {
        return std::dynamic_pointer_cast<RowVector>(
            BatchMaker::createBatch(probeType_, 1000, *pool_));
      });
  std::vector<RowVectorPtr> buildVectors = makeBatches(10, [&](int32_t index) {
    return std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(buildType_, 5000 * (1 + index), *pool_));
  });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(probeVectors, true)
                        .hashJoin(
                            {"t_k1"},
                            {"u_k1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(buildVectors, true)
                                .planNode(),
                            "",
                            concat(probeType_->names(), buildType_->names()))
                        .planNode();
  params.queryCtx = core::QueryCtx::create(driverExecutor_.get());
  // NOTE: the spilling setup is to trigger memory reservation code path which
  // only gets executed when spilling is enabled. We don't care about if
  // spilling is really triggered in test or not.
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  params.spillDirectory = spillDirectory->getPath();
  params.queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSpillEnabled, "true"},
       {core::QueryConfig::kMaxSpillLevel, "0"}});
  params.maxDrivers = 1;

  auto cursor = TaskCursor::create(params);
  auto* task = cursor->task().get();

  // Set up a testvalue to trigger task abort when hash build tries to reserve
  // memory.
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPool*)>(
          [&](memory::MemoryPool* /*unused*/) { task->requestAbort(); }));
  auto runTask = [&]() {
    while (cursor->moveNext()) {
    }
  };
  VELOX_ASSERT_THROW(runTask(), "");
  ASSERT_TRUE(waitForTaskAborted(task, 5'000'000));
}

TEST_F(HashJoinTest, dynamicFilterOnPartitionKey) {
  vector_size_t size = 10;
  auto filePaths = makeFilePaths(1);
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>(size, [&](auto row) { return row; })});
  createDuckDbTable("u", {rowVector});
  writeToFile(filePaths[0]->getPath(), rowVector);
  std::vector<RowVectorPtr> buildVectors{
      makeRowVector({"c0"}, {makeFlatVector<int64_t>({0, 1, 2})})};
  createDuckDbTable("t", buildVectors);
  auto split = facebook::velox::exec::test::HiveConnectorSplitBuilder(
                   filePaths[0]->getPath())
                   .partitionKey("k", "0")
                   .build();
  auto outputType = ROW({"n1_0", "n1_1"}, {BIGINT(), BIGINT()});
  connector::ColumnHandleMap assignments = {
      {"n1_0", regularColumn("c0", BIGINT())},
      {"n1_1", partitionKey("k", BIGINT())}};

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op =
      PlanBuilder(planNodeIdGenerator)
          .startTableScan()
          .outputType(outputType)
          .assignments(assignments)
          .endTableScan()
          .capturePlanNodeId(probeScanId)
          .hashJoin(
              {"n1_1"},
              {"c0"},
              PlanBuilder(planNodeIdGenerator).values(buildVectors).planNode(),
              "",
              {"c0"},
              core::JoinType::kInner)
          .project({"c0"})
          .planNode();
  SplitInput splits = {{probeScanId, {exec::Split(split)}}};

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(op))
      .inputSplits(splits)
      .referenceQuery("select t.c0 from t, u where t.c0 = 0")
      .checkSpillStats(false)
      .run();
}

TEST_F(HashJoinTest, probeMemoryLimitOnBuildProjection) {
  const uint64_t numBuildRows = 20;
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return makeRowVector({makeFlatVector<int32_t>(
            1'000, [](auto row) { return row % 25; })});
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(1, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"u_c0", "u_c1", "u_c2", "u_c3", "u_c4"},
            {makeFlatVector<int32_t>(
                 numBuildRows, [](auto row) { return row; }),
             makeFlatVector<std::string>(
                 numBuildRows,
                 [](auto /* row */) { return std::string(4096, 'a'); }),
             makeFlatVector<std::string>(
                 numBuildRows,
                 [](auto /* row */) { return std::string(4096, 'a'); }),
             makeFlatVector<std::string>(
                 numBuildRows,
                 [](auto row) {
                   // Row that has too large of size variation.
                   if (row == 0) {
                     return std::string(4096, 'a');
                   } else {
                     return std::string(1, 'a');
                   }
                 }),
             makeFlatVector<std::string>(numBuildRows, [](auto row) {
               // Row that has tolerable size variation.
               if (row == 0) {
                 return std::string(4096, 'a');
               } else {
                 return std::string(256, 'a');
               }
             })});
      });

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  struct TestParam {
    std::vector<int32_t> varSizeColumns;
    int32_t numExpectedBatches;
    std::string referenceQuery;
    std::string debugString() const {
      std::stringstream ss;
      ss << "varSizeColumns [";
      for (const auto& columnIndex : varSizeColumns) {
        ss << columnIndex << ", ";
      }
      ss << "] ";
      ss << "numExpectedBatches " << numExpectedBatches << ", referenceQuery '"
         << referenceQuery << "'";
      return ss.str();
    }
  };

  std::vector<TestParam> testParams{
      {{}, 10, "SELECT t.c0 FROM t JOIN u ON t.c0 = u.u_c0"},
      {{1}, 4000, "SELECT t.c0, u.u_c1 FROM t JOIN u ON t.c0 = u.u_c0"},
      {{1, 2},
       8000,
       "SELECT t.c0, u.u_c1, u.u_c2 FROM t JOIN u ON t.c0 = u.u_c0"},
      {{3}, 210, "SELECT t.c0, u.u_c3 FROM t JOIN u ON t.c0 = u.u_c0"},
      {{4}, 2670, "SELECT t.c0, u.u_c4 FROM t JOIN u ON t.c0 = u.u_c0"}};

  for (const auto& testParam : testParams) {
    SCOPED_TRACE(testParam.debugString());
    core::PlanNodeId joinNodeId;
    std::vector<std::string> outputLayout;
    outputLayout.push_back("c0");
    for (int32_t i = 0; i < testParam.varSizeColumns.size(); i++) {
      outputLayout.push_back(fmt::format("u_c{}", testParam.varSizeColumns[i]));
    }
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors)
                    .hashJoin(
                        {"c0"},
                        {"u_c0"},
                        PlanBuilder(planNodeIdGenerator)
                            .values({buildVectors})
                            .planNode(),
                        "",
                        outputLayout)
                    .capturePlanNodeId(joinNodeId)
                    .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(plan))
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "8192")
        .injectSpill(false)
        .referenceQuery(testParam.referenceQuery)
        .verifier([&](const std::shared_ptr<Task>& task, bool /* unused */) {
          auto planStats = toPlanStats(task->taskStats());
          auto outputBatches = planStats.at(joinNodeId).outputVectors;
          ASSERT_EQ(outputBatches, testParam.numExpectedBatches);
        })
        .run();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringInputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    // 0: trigger reclaim with some input processed.
    // 1: trigger reclaim after all the inputs processed.
    int triggerCondition;
    bool spillEnabled;
    bool expectedReclaimable;

    std::string debugString() const {
      return fmt::format(
          "triggerCondition {}, spillEnabled {}, expectedReclaimable {}",
          triggerCondition,
          spillEnabled,
          expectedReclaimable);
    }
  } testSettings[] = {
      {0, true, true}, {0, true, true}, {0, false, false}, {0, false, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryPool = memory::memoryManager()->addRootPool(
        "", kMaxBytes, memory::MemoryReclaimer::create());

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, false)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, false)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    std::atomic<int> numInputs{0};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
            return;
          }
          op = testOp;
          ++numInputs;
          if (testData.triggerCondition == 0) {
            if (numInputs != 2) {
              return;
            }
          }
          if (testData.triggerCondition == 1) {
            if (numInputs != numBuildVectors) {
              return;
            }
          }
          ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, testData.expectedReclaimable);
          if (testData.expectedReclaimable) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWait.notify();
          driverWait.wait(driverWaitKey);
        })));

    std::thread taskThread([&]() {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .planNode(plan)
          .queryPool(std::move(queryPool))
          .injectSpill(false)
          .spillDirectory(testData.spillEnabled ? tempDirectory->getPath() : "")
          .referenceQuery(
              "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
          .config(core::QueryConfig::kSpillStartPartitionBit, "29")
          .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
            const auto statsPair = taskSpilledStats(*task);
            if (testData.expectedReclaimable) {
              ASSERT_GT(statsPair.first.spilledBytes, 0);
              ASSERT_EQ(statsPair.first.spilledPartitions, 8);
              ASSERT_GT(statsPair.second.spilledBytes, 0);
              ASSERT_EQ(statsPair.second.spilledPartitions, 8);
              verifyTaskSpilledRuntimeStats(*task, true);
            } else {
              ASSERT_EQ(statsPair.first.spilledBytes, 0);
              ASSERT_EQ(statsPair.first.spilledPartitions, 0);
              ASSERT_EQ(statsPair.second.spilledBytes, 0);
              ASSERT_EQ(statsPair.second.spilledPartitions, 0);
              verifyTaskSpilledRuntimeStats(*task, false);
            }
          })
          .run();
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->operatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWait.notify();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
    ASSERT_EQ(reclaimable, testData.expectedReclaimable);
    if (testData.expectedReclaimable) {
      ASSERT_GT(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }

    if (testData.expectedReclaimable) {
      {
        memory::ScopedMemoryArbitrationContext ctx(op->pool());
        op->pool()->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
            0,
            reclaimerStats_);
      }
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
      ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
      reclaimerStats_.reset();
      ASSERT_EQ(op->pool()->usedBytes(), 0);
    } else {
      VELOX_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
              reclaimerStats_),
          "");
    }

    Task::resume(task);
    task.reset();

    taskThread.join();
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringReserve) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  const int32_t numBuildVectors = 3;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    const size_t size = i == 0 ? 1 : 1'000;
    VectorFuzzer fuzzer({.vectorSize = size}, pool());
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }

  const int32_t numProbeVectors = 3;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    VectorFuzzer fuzzer({.vectorSize = 1'000}, pool());
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryPool = memory::memoryManager()->addRootPool(
      "", kMaxBytes, memory::MemoryReclaimer::create());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  folly::EventCount driverWait;
  std::atomic_bool driverWaitFlag{true};
  folly::EventCount testWait;
  std::atomic_bool testWaitFlag{true};

  Operator* op{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "HashBuild") {
          return;
        }
        op = testOp;
      })));

  std::atomic<bool> injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            ASSERT_TRUE(op != nullptr);
            if (!isHashBuildMemoryPool(*pool)) {
              return;
            }
            ASSERT_TRUE(op->canReclaim());
            if (op->pool()->usedBytes() == 0) {
              // We skip trigger memory reclaim when the hash table is empty on
              // memory reservation.
              return;
            }
            if (!injectOnce.exchange(false)) {
              return;
            }
            uint64_t reclaimableBytes{0};
            const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
            ASSERT_TRUE(reclaimable);
            ASSERT_GT(reclaimableBytes, 0);
            auto* driver = op->operatorCtx()->driver();
            TestSuspendedSection suspendedSection(driver);
            testWaitFlag = false;
            testWait.notifyAll();
            driverWait.await([&]() { return !driverWaitFlag.load(); });
          })));

  std::thread taskThread([&]() {
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .planNode(plan)
        .queryPool(std::move(queryPool))
        .injectSpill(false)
        .spillDirectory(tempDirectory->getPath())
        .referenceQuery(
            "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
        .config(core::QueryConfig::kSpillStartPartitionBit, "29")
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_GT(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 8);
          ASSERT_GT(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 8);
          verifyTaskSpilledRuntimeStats(*task, true);
        })
        .run();
  });

  testWait.await([&]() { return !testWaitFlag.load(); });
  ASSERT_TRUE(op != nullptr);
  auto task = op->operatorCtx()->task();
  task->requestPause().wait();

  uint64_t reclaimableBytes{0};
  const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
  ASSERT_TRUE(op->canReclaim());
  ASSERT_TRUE(reclaimable);
  ASSERT_GT(reclaimableBytes, 0);

  {
    memory::ScopedMemoryArbitrationContext ctx(op->pool());
    uint64_t reclaimedBytes = task->pool()->reclaim(
        folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
        0,
        reclaimerStats_);
    ASSERT_GT(reclaimedBytes, 0);
  }
  ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
  ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
  ASSERT_EQ(op->pool()->usedBytes(), 0);

  driverWaitFlag = false;
  driverWait.notifyAll();
  Task::resume(task);
  task.reset();

  taskThread.join();
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringAllocation) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryPool = memory::memoryManager()->addRootPool("", kMaxBytes);

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, false)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, false)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
            return;
          }
          op = testOp;
        })));

    std::atomic<bool> injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              ASSERT_TRUE(op != nullptr);
              const std::string re(".*HashBuild");
              if (!RE2::FullMatch(pool->name(), re)) {
                return;
              }
              if (!injectOnce.exchange(false)) {
                return;
              }
              ASSERT_EQ(op->canReclaim(), enableSpilling);
              uint64_t reclaimableBytes{0};
              const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
              ASSERT_EQ(reclaimable, enableSpilling);
              if (enableSpilling) {
                ASSERT_GE(reclaimableBytes, 0);
              } else {
                ASSERT_EQ(reclaimableBytes, 0);
              }
              auto* driver = op->operatorCtx()->driver();
              TestSuspendedSection suspendedSection(driver);
              testWait.notify();
              driverWait.wait(driverWaitKey);
            })));

    std::thread taskThread([&]() {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .planNode(plan)
          .queryPool(std::move(queryPool))
          .injectSpill(false)
          .spillDirectory(enableSpilling ? tempDirectory->getPath() : "")
          .referenceQuery(
              "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
          .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
            const auto statsPair = taskSpilledStats(*task);
            ASSERT_EQ(statsPair.first.spilledBytes, 0);
            ASSERT_EQ(statsPair.first.spilledPartitions, 0);
            ASSERT_EQ(statsPair.second.spilledBytes, 0);
            ASSERT_EQ(statsPair.second.spilledPartitions, 0);
            verifyTaskSpilledRuntimeStats(*task, false);
          })
          .run();
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->operatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);
    if (enableSpilling) {
      ASSERT_GE(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }
    VELOX_ASSERT_THROW(
        op->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
            reclaimerStats_),
        "");

    driverWait.notify();
    Task::resume(task);
    task.reset();

    taskThread.join();
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{0});
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringOutputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryPool = memory::memoryManager()->addRootPool(
        "", kMaxBytes, memory::MemoryReclaimer::create());

    core::PlanNodeId probeScanId;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, false)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, false)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    std::atomic_bool driverWaitFlag{true};
    folly::EventCount driverWait;
    std::atomic_bool testWaitFlag{true};
    folly::EventCount testWait;

    std::atomic<bool> injectOnce{true};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "HashBuild") {
            return;
          }
          op = testOp;
          if (!injectOnce.exchange(false)) {
            return;
          }
          ASSERT_EQ(op->canReclaim(), enableSpilling);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, enableSpilling);
          if (enableSpilling) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWaitFlag = false;
          testWait.notifyAll();
          driverWait.await([&]() { return !driverWaitFlag.load(); });
        })));

    std::thread taskThread([&]() {
      HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
          .numDrivers(numDrivers_)
          .planNode(plan)
          .queryPool(std::move(queryPool))
          .injectSpill(false)
          .spillDirectory(enableSpilling ? tempDirectory->getPath() : "")
          .referenceQuery(
              "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
          .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
            const auto statsPair = taskSpilledStats(*task);
            ASSERT_EQ(statsPair.first.spilledBytes, 0);
            ASSERT_EQ(statsPair.first.spilledPartitions, 0);
            ASSERT_EQ(statsPair.second.spilledBytes, 0);
            ASSERT_EQ(statsPair.second.spilledPartitions, 0);
            verifyTaskSpilledRuntimeStats(*task, false);
          })
          .run();
    });

    testWait.await([&]() { return !testWaitFlag.load(); });
    ASSERT_TRUE(op != nullptr);
    auto task = op->operatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWaitFlag = false;
    driverWait.notifyAll();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);

    if (enableSpilling) {
      ASSERT_GT(reclaimableBytes, 0);
      const auto usedMemoryBytes = op->pool()->usedBytes();
      {
        memory::ScopedMemoryArbitrationContext ctx(op->pool());
        op->pool()->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
            0,
            reclaimerStats_);
      }
      ASSERT_GE(reclaimerStats_.reclaimedBytes, 0);
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
      // No reclaim as the operator has started output processing.
      ASSERT_EQ(usedMemoryBytes, op->pool()->usedBytes());
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
      VELOX_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
              reclaimerStats_),
          "");
    }

    Task::resume(task);
    task.reset();

    taskThread.join();
  }
  ASSERT_EQ(reclaimerStats_.numNonReclaimableAttempts, 1);
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringWaitForProbe) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryPool = memory::memoryManager()->addRootPool(
      "", kMaxBytes, memory::MemoryReclaimer::create());

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  std::atomic_bool driverWaitFlag{true};
  folly::EventCount driverWait;
  std::atomic_bool testWaitFlag{true};
  folly::EventCount testWait;

  Operator* op{nullptr};
  std::atomic<bool> injectSpillOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "HashBuild") {
          return;
        }
        op = testOp;
        if (!injectSpillOnce.exchange(false)) {
          return;
        }
        auto* driver = op->operatorCtx()->driver();
        auto task = driver->task();
        memory::ScopedMemoryArbitrationContext ctx(op->pool());
        Operator::ReclaimableSectionGuard guard(testOp);
        testingRunArbitration(testOp->pool());
      })));

  std::atomic<bool> injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::noMoreInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "HashProbe") {
          return;
        }
        if (!injectOnce.exchange(false)) {
          return;
        }
        ASSERT_TRUE(op != nullptr);
        ASSERT_TRUE(op->canReclaim());
        uint64_t reclaimableBytes{0};
        const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
        ASSERT_TRUE(reclaimable);
        ASSERT_GT(reclaimableBytes, 0);
        testWaitFlag = false;
        testWait.notifyAll();
        auto* driver = testOp->operatorCtx()->driver();
        auto task = driver->task();
        TestSuspendedSection suspendedSection(driver);
        driverWait.await([&]() { return !driverWaitFlag.load(); });
      })));

  std::thread taskThread([&]() {
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .planNode(plan)
        .queryPool(std::move(queryPool))
        .injectSpill(false)
        .spillDirectory(tempDirectory->getPath())
        .referenceQuery(
            "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
        .config(core::QueryConfig::kSpillStartPartitionBit, "29")
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_GT(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 8);
          ASSERT_GT(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 8);
        })
        .run();
  });

  testWait.await([&]() { return !testWaitFlag.load(); });
  ASSERT_TRUE(op != nullptr);
  auto task = op->operatorCtx()->task();
  auto taskPauseWait = task->requestPause();
  taskPauseWait.wait();

  uint64_t reclaimableBytes{0};
  const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
  ASSERT_TRUE(op->canReclaim());
  ASSERT_TRUE(reclaimable);
  ASSERT_GT(reclaimableBytes, 0);

  const auto usedMemoryBytes = op->pool()->usedBytes();
  reclaimerStats_.reset();
  {
    memory::ScopedMemoryArbitrationContext ctx(op->pool());
    op->pool()->reclaim(
        folly::Random::oneIn(2) ? 0 : folly::Random::rand32(),
        0,
        reclaimerStats_);
  }
  ASSERT_GE(reclaimerStats_.reclaimedBytes, 0);
  ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
  //  No reclaim as the build operator is not in building table state.
  ASSERT_EQ(usedMemoryBytes, op->pool()->usedBytes());

  driverWaitFlag = false;
  driverWait.notifyAll();
  Task::resume(task);
  task.reset();

  taskThread.join();
  ASSERT_EQ(reclaimerStats_.numNonReclaimableAttempts, 1);
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashBuildAbortDuringOutputProcessing) {
  const auto buildVectors = makeVectors(buildType_, 10, 128);
  const auto probeVectors = makeVectors(probeType_, 5, 128);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    std::atomic<bool> injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          if (!injectOnce.exchange(false)) {
            return;
          }
          ASSERT_GT(op->pool()->usedBytes(), 0);
          auto* driver = op->operatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testData.abortFromRootMemoryPool ? abortPool(op->pool()->root())
                                           : abortPool(op->pool());
          // We can't directly reclaim memory from this hash build operator as
          // its driver thread is running and in suspension state.
          ASSERT_GT(op->pool()->root()->usedBytes(), 0);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          ASSERT_TRUE(op->pool()->aborted());
          ASSERT_TRUE(op->pool()->root()->aborted());
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    VELOX_ASSERT_THROW(
        HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
            .numDrivers(numDrivers_)
            .planNode(plan)
            .injectSpill(false)
            .referenceQuery(
                "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
            .run(),
        "Manual MemoryPool Abortion");
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashBuildAbortDuringInputProcessing) {
  const auto buildVectors = makeVectors(buildType_, 10, 128);
  const auto probeVectors = makeVectors(probeType_, 5, 128);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    std::atomic<int> numInputs{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashBuild") {
            return;
          }
          if (++numInputs != 2) {
            return;
          }
          ASSERT_GT(op->pool()->usedBytes(), 0);
          auto* driver = op->operatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testData.abortFromRootMemoryPool ? abortPool(op->pool()->root())
                                           : abortPool(op->pool());
          // We can't directly reclaim memory from this hash build operator as
          // its driver thread is running and in suspension state.
          ASSERT_GT(op->pool()->root()->usedBytes(), 0);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          ASSERT_TRUE(op->pool()->aborted());
          ASSERT_TRUE(op->pool()->root()->aborted());
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    VELOX_ASSERT_THROW(
        HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
            .numDrivers(numDrivers_)
            .planNode(plan)
            .injectSpill(false)
            .referenceQuery(
                "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
            .run(),
        "Manual MemoryPool Abortion");

    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashBuildAbortDuringAllocation) {
  const auto buildVectors = makeVectors(buildType_, 10, 128);
  const auto probeVectors = makeVectors(probeType_, 5, 128);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    std::atomic_bool injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              if (!isHashBuildMemoryPool(*pool)) {
                return;
              }
              if (!injectOnce.exchange(false)) {
                return;
              }

              const auto* driverCtx = driverThreadContext()->driverCtx();
              ASSERT_EQ(
                  driverCtx->task->enterSuspended(driverCtx->driver->state()),
                  StopReason::kNone);
              testData.abortFromRootMemoryPool ? abortPool(pool->root())
                                               : abortPool(pool);
              // We can't directly reclaim memory from this hash build operator
              // as its driver thread is running and in suspegnsion state.
              ASSERT_GE(pool->root()->usedBytes(), 0);
              ASSERT_EQ(
                  driverCtx->task->leaveSuspended(driverCtx->driver->state()),
                  StopReason::kAlreadyTerminated);
              ASSERT_TRUE(pool->aborted());
              ASSERT_TRUE(pool->root()->aborted());
              VELOX_MEM_POOL_ABORTED("Memory pool aborted");
            })));

    VELOX_ASSERT_THROW(
        HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
            .numDrivers(numDrivers_)
            .planNode(plan)
            .injectSpill(false)
            .referenceQuery(
                "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
            .run(),
        "Manual MemoryPool Abortion");

    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashProbeAbortDuringInputProcessing) {
  const auto buildVectors = makeVectors(buildType_, 10, 128);
  const auto probeVectors = makeVectors(probeType_, 5, 128);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        "",
                        concat(probeType_->names(), buildType_->names()))
                    .planNode();

    std::atomic<int> numInputs{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "HashProbe") {
            return;
          }
          if (++numInputs != 2) {
            return;
          }
          auto* driver = op->operatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testData.abortFromRootMemoryPool ? abortPool(op->pool()->root())
                                           : abortPool(op->pool());
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          ASSERT_TRUE(op->pool()->aborted());
          ASSERT_TRUE(op->pool()->root()->aborted());
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    VELOX_ASSERT_THROW(
        HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
            .numDrivers(numDrivers_)
            .planNode(plan)
            .injectSpill(false)
            .referenceQuery(
                "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
            .run(),
        "Manual MemoryPool Abortion");
    waitForAllTasksToBeDeleted();
  }
}

TEST_F(HashJoinTest, leftJoinWithMissAtEndOfBatch) {
  // Tests some cases where the row at the end of an output batch fails the
  // filter.
  auto probeVectors = std::vector<RowVectorPtr>{makeRowVector(
      {"t_k1", "t_k2"},
      {makeFlatVector<int32_t>(20, [](auto row) { return 1 + row % 2; }),
       makeFlatVector<int32_t>(20, [](auto row) { return row; })})};
  auto buildVectors = std::vector<RowVectorPtr>{
      makeRowVector({"u_k1"}, {makeFlatVector<int32_t>({1, 2})})};
  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {buildVectors});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto test = [&](const std::string& filter) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        filter,
                        {"t_k1", "u_k1"},
                        core::JoinType::kLeft)
                    .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .injectSpill(false)
        .checkSpillStats(false)
        .maxSpillLevel(0)
        .numDrivers(1)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
        .referenceQuery(fmt::format(
            "SELECT t_k1, u_k1 from t left join u on t_k1 = u_k1 and {}",
            filter))
        .run();
  };

  // Alternate rows pass this filter and last row of a batch fails.
  test("t_k1=1");

  // All rows fail this filter.
  test("t_k1=5");

  // All rows in the second batch pass this filter.
  test("t_k2 > 9");
}

TEST_F(HashJoinTest, leftJoinWithMissAtEndOfBatchMultipleBuildMatches) {
  // Tests some cases where the row at the end of an output batch fails the
  // filter and there are multiple matches with the build side..
  auto probeVectors = std::vector<RowVectorPtr>{makeRowVector(
      {"t_k1", "t_k2"},
      {makeFlatVector<int32_t>(10, [](auto row) { return 1 + row % 2; }),
       makeFlatVector<int32_t>(10, [](auto row) { return row; })})};
  auto buildVectors = std::vector<RowVectorPtr>{
      makeRowVector({"u_k1"}, {makeFlatVector<int32_t>({1, 2, 1, 2})})};
  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {buildVectors});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto test = [&](const std::string& filter) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeVectors, true)
                    .hashJoin(
                        {"t_k1"},
                        {"u_k1"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildVectors, true)
                            .planNode(),
                        filter,
                        {"t_k1", "u_k1"},
                        core::JoinType::kLeft)
                    .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(plan)
        .injectSpill(false)
        .checkSpillStats(false)
        .maxSpillLevel(0)
        .numDrivers(1)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
        .referenceQuery(fmt::format(
            "SELECT t_k1, u_k1 from t left join u on t_k1 = u_k1 and {}",
            filter))
        .run();
  };

  // In this case the rows with t_k2 = 4 appear at the end of the first batch,
  // meaning the last rows in that output batch are misses, and don't get added.
  // The rows with t_k2 = 8 appear in the second batch so only one row is
  // written, meaning there is space in the second output batch for the miss
  // with tk_2 = 4 to get written.
  test("t_k2 != 4 and t_k2 != 8");
}

TEST_F(HashJoinTest, leftJoinPreserveProbeOrder) {
  const std::vector<RowVectorPtr> probeVectors = {
      makeRowVector(
          {"k1", "v1"},
          {
              makeConstant<int64_t>(0, 2),
              makeFlatVector<int64_t>({1, 0}),
          }),
  };
  const std::vector<RowVectorPtr> buildVectors = {
      makeRowVector(
          {"k2", "v2"},
          {
              makeConstant<int64_t>(0, 2),
              makeConstant<int64_t>(0, 2),
          }),
  };
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(probeVectors)
          .hashJoin(
              {"k1"},
              {"k2"},
              PlanBuilder(planNodeIdGenerator).values(buildVectors).planNode(),
              "v1 % 2 = v2 % 2",
              {"v1"},
              core::JoinType::kLeft)
          .planNode();
  auto result = AssertQueryBuilder(plan)
                    .config(core::QueryConfig::kPreferredOutputBatchRows, "1")
                    .serialExecution(true)
                    .copyResults(pool_.get());
  ASSERT_EQ(result->size(), 3);
  auto* v1 =
      result->childAt(0)->loadedVector()->asUnchecked<SimpleVector<int64_t>>();
  ASSERT_FALSE(v1->mayHaveNulls());
  ASSERT_EQ(v1->valueAt(0), 1);
  ASSERT_EQ(v1->valueAt(1), 0);
  ASSERT_EQ(v1->valueAt(2), 0);
}

DEBUG_ONLY_TEST_F(HashJoinTest, minSpillableMemoryReservation) {
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzInputRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzInputRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  for (int32_t minSpillableReservationPct : {5, 50, 100}) {
    SCOPED_TRACE(fmt::format(
        "minSpillableReservationPct: {}", minSpillableReservationPct));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::HashBuild::addInput",
        std::function<void(exec::HashBuild*)>(([&](exec::HashBuild* hashBuild) {
          memory::MemoryPool* pool = hashBuild->pool();
          const auto availableReservationBytes = pool->availableReservation();
          const auto currentUsedBytes = pool->usedBytes();
          // Verifies we always have min reservation after ensuring the input.
          ASSERT_GE(
              availableReservationBytes,
              currentUsedBytes * minSpillableReservationPct / 100);
        })));

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers_)
        .planNode(plan)
        .injectSpill(false)
        .spillDirectory(tempDirectory->getPath())
        .referenceQuery(
            "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
        .run();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, exceededMaxSpillLevel) {
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 10;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  const int exceededMaxSpillLevelCount =
      common::globalSpillStats().spillMaxLevelExceededCount;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::reclaim",
      std::function<void(exec::Operator*)>(([&](exec::Operator* op) {
        HashBuild* hashBuild = static_cast<HashBuild*>(op);
        ASSERT_FALSE(hashBuild->testingExceededMaxSpillLevelLimit());
      })));
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashProbe::reclaim",
      std::function<void(exec::HashBuild*)>(([&](exec::Operator* op) {
        HashProbe* hashProbe = static_cast<HashProbe*>(op);
        ASSERT_FALSE(hashProbe->testingExceededMaxSpillLevelLimit());
      })));
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(exec::HashBuild*)>(([&](exec::HashBuild* hashBuild) {
        Operator::ReclaimableSectionGuard guard(hashBuild);
        testingRunArbitration(hashBuild->pool());
      })));
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .planNode(plan)
      // Always trigger spilling.
      .injectSpill(false)
      .maxSpillLevel(0)
      .spillDirectory(tempDirectory->getPath())
      .referenceQuery(
          "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
      .config(core::QueryConfig::kSpillStartPartitionBit, "29")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        auto opStats = toOperatorStats(task->taskStats());
        ASSERT_EQ(
            opStats.at("HashProbe")
                .runtimeStats[Operator::kExceededMaxSpillLevel]
                .sum,
            8);
        ASSERT_EQ(
            opStats.at("HashProbe")
                .runtimeStats[Operator::kExceededMaxSpillLevel]
                .count,
            1);
        ASSERT_EQ(
            opStats.at("HashBuild")
                .runtimeStats[Operator::kExceededMaxSpillLevel]
                .sum,
            8);
        ASSERT_EQ(
            opStats.at("HashBuild")
                .runtimeStats[Operator::kExceededMaxSpillLevel]
                .count,
            1);
      })
      .run();
  ASSERT_EQ(
      common::globalSpillStats().spillMaxLevelExceededCount,
      exceededMaxSpillLevelCount + 16);
}

TEST_F(HashJoinTest, maxSpillBytes) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto probeVectors = createVectors(rowType, 1024, 10 << 20);
  const auto buildVectors = createVectors(rowType, 1024, 10 << 20);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, true)
                  .project({"c0", "c1", "c2"})
                  .hashJoin(
                      {"c0"},
                      {"u1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, true)
                          .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                          .planNode(),
                      "",
                      {"c0", "c1", "c2"},
                      core::JoinType::kInner)
                  .planNode();

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());

  struct {
    int32_t maxSpilledBytes;
    bool expectedExceedLimit;
    std::string debugString() const {
      return fmt::format("maxSpilledBytes {}", maxSpilledBytes);
    }
  } testSettings[] = {{1 << 30, false}, {16 << 20, true}, {0, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    try {
      TestScopedSpillInjection scopedSpillInjection(100);
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->getPath())
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kJoinSpillEnabled, true)
          .config(core::QueryConfig::kMaxSpillBytes, testData.maxSpilledBytes)
          .copyResults(pool_.get());
      ASSERT_FALSE(testData.expectedExceedLimit);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_TRUE(testData.expectedExceedLimit);
      ASSERT_NE(
          e.message().find(
              "Query exceeded per-query local spill limit of 16.00MB"),
          std::string::npos);
      ASSERT_EQ(
          e.errorCode(), facebook::velox::error_code::kSpillLimitExceeded);
    }
  }
}

TEST_F(HashJoinTest, onlyHashBuildMaxSpillBytes) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto probeVectors = createVectors(rowType, 32, 128);
  const auto buildVectors = createVectors(rowType, 1024, 10 << 20);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, true)
                  .hashJoin(
                      {"c0"},
                      {"u1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, true)
                          .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                          .planNode(),
                      "",
                      {"c0", "c1", "c2"},
                      core::JoinType::kInner)
                  .planNode();

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());

  struct {
    int32_t maxSpilledBytes;
    bool expectedExceedLimit;
    std::string debugString() const {
      return fmt::format("maxSpilledBytes {}", maxSpilledBytes);
    }
  } testSettings[] = {{1 << 30, false}, {16 << 20, true}, {0, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    try {
      TestScopedSpillInjection scopedSpillInjection(100);
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->getPath())
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kJoinSpillEnabled, true)
          .config(core::QueryConfig::kMaxSpillBytes, testData.maxSpilledBytes)
          .copyResults(pool_.get());
      ASSERT_FALSE(testData.expectedExceedLimit);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_TRUE(testData.expectedExceedLimit);
      ASSERT_NE(
          e.message().find(
              "Query exceeded per-query local spill limit of 16.00MB"),
          std::string::npos);
      ASSERT_EQ(
          e.errorCode(), facebook::velox::error_code::kSpillLimitExceeded);
    }
  }
}

TEST_F(HashJoinTest, reclaimFromJoinBuilderWithMultiDrivers) {
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  const auto vectors = createVectors(rowType, 64 << 20, fuzzerOpts_);
  const int numDrivers = 4;

  memory::MemoryManager::Options options;
  options.allocatorCapacity = 8L << 30;
  auto memoryManagerWithoutArbitrator =
      std::make_unique<memory::MemoryManager>(options);
  const auto expectedResult =
      runHashJoinTask(
          vectors,
          newQueryCtx(
              memoryManagerWithoutArbitrator.get(), executor_.get(), 8L << 30),
          false,
          numDrivers,
          pool(),
          false)
          .data;

  auto memoryManagerWithArbitrator = createMemoryManager();
  const auto& arbitrator = memoryManagerWithArbitrator->arbitrator();
  // Create a query ctx with a small capacity to trigger spilling.
  auto result = runHashJoinTask(
      vectors,
      newQueryCtx(
          memoryManagerWithArbitrator.get(), executor_.get(), 128 << 20),
      false,
      numDrivers,
      pool(),
      true,
      expectedResult);
  auto taskStats = exec::toPlanStats(result.task->taskStats());
  auto& planStats = taskStats.at(result.planNodeId);
  ASSERT_GT(planStats.spilledBytes, 0);
  result.task.reset();

  // This test uses on-demand created memory manager instead of the global
  // one. We need to make sure any used memory got cleaned up before exiting
  // the scope
  waitForAllTasksToBeDeleted();
  ASSERT_GT(arbitrator->stats().numRequests, 0);
  ASSERT_GT(arbitrator->stats().reclaimedUsedBytes, 0);
}

DEBUG_ONLY_TEST_F(
    HashJoinTest,
    failedToReclaimFromHashJoinBuildersInNonReclaimableSection) {
  auto rowType = ROW({
      {"c0", INTEGER()},
      {"c1", INTEGER()},
      {"c2", VARCHAR()},
  });
  const auto vectors = createVectors(rowType, 64 << 20, fuzzerOpts_);
  const int numDrivers = 1;
  std::shared_ptr<core::QueryCtx> queryCtx =
      newQueryCtx(memory::memoryManager(), executor_.get(), 512 << 20);
  const auto expectedResult =
      runHashJoinTask(vectors, queryCtx, false, numDrivers, pool(), false).data;

  std::atomic_bool nonReclaimableSectionWaitFlag{true};
  std::atomic_bool reclaimerInitializationWaitFlag{true};
  folly::EventCount nonReclaimableSectionWait;
  std::atomic_bool memoryArbitrationWaitFlag{true};
  folly::EventCount memoryArbitrationWait;

  std::atomic<uint32_t> numInitializedDrivers{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal",
      std::function<void(exec::Driver*)>([&](exec::Driver* driver) {
        numInitializedDrivers++;
        // We need to make sure reclaimers on both build and probe side are set
        // (in Operator::initialize) to avoid race conditions, producing
        // consistent test results.
        if (numInitializedDrivers.load() == 2) {
          reclaimerInitializationWaitFlag = false;
          nonReclaimableSectionWait.notifyAll();
        }
      }));

  std::atomic<bool> injectNonReclaimableSectionOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            if (!isHashBuildMemoryPool(*pool)) {
              return;
            }
            if (!injectNonReclaimableSectionOnce.exchange(false)) {
              return;
            }

            // Signal the test control that one of the hash build operator has
            // entered into non-reclaimable section.
            nonReclaimableSectionWaitFlag = false;
            nonReclaimableSectionWait.notifyAll();

            // Suspend the driver to simulate the arbitration.
            pool->reclaimer()->enterArbitration();
            // Wait for the memory arbitration to complete.
            memoryArbitrationWait.await(
                [&]() { return !memoryArbitrationWaitFlag.load(); });
            pool->reclaimer()->leaveArbitration();
          })));

  std::thread joinThread([&]() {
    const auto result = runHashJoinTask(
        vectors, queryCtx, false, numDrivers, pool(), true, expectedResult);
    auto taskStats = exec::toPlanStats(result.task->taskStats());
    auto& planStats = taskStats.at(result.planNodeId);
    ASSERT_EQ(planStats.spilledBytes, 0);
  });

  // Wait for the hash build operators to enter into non-reclaimable section.
  nonReclaimableSectionWait.await([&]() {
    return (
        !nonReclaimableSectionWaitFlag.load() &&
        !reclaimerInitializationWaitFlag.load());
  });

  // We expect capacity grow fails as we can't reclaim from hash join operators.
  memory::testingRunArbitration();

  // Notify the hash build operator that memory arbitration has been done.
  memoryArbitrationWaitFlag = false;
  memoryArbitrationWait.notifyAll();

  joinThread.join();

  // This test uses on-demand created memory manager instead of the global
  // one. We need to make sure any used memory got cleaned up before exiting
  // the scope
  waitForAllTasksToBeDeleted();
  ASSERT_EQ(
      memory::memoryManager()->arbitrator()->stats().numNonReclaimableAttempts,
      2);
}

DEBUG_ONLY_TEST_F(HashJoinTest, reclaimDuringTableBuild) {
  VectorFuzzer fuzzer({.vectorSize = 1000}, pool());
  const int32_t numBuildVectors = 5;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }
  const int32_t numProbeVectors = 5;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  core::PlanNodeId probeScanId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, false)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, false)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  std::atomic_bool injectSpillOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!injectSpillOnce.exchange(false)) {
          return;
        }
        Operator::ReclaimableSectionGuard guard(op);
        testingRunArbitration(op->pool());
      }));

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(4)
      .planNode(plan)
      .injectSpill(false)
      .maxSpillLevel(0)
      .spillDirectory(tempDirectory->getPath())
      .referenceQuery(
          "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
      .config(core::QueryConfig::kSpillStartPartitionBit, "29")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        auto opStats = toOperatorStats(task->taskStats());
        ASSERT_GT(
            opStats.at("HashBuild").runtimeStats[Operator::kSpillWrites].sum,
            0);
      })
      .run();
}

DEBUG_ONLY_TEST_F(HashJoinTest, exceptionDuringFinishJoinBuild) {
  // This test is to make sure there is no memory leak when exceptions are
  // thrown while parallelly preparing join table.
  auto memoryManager = memory::memoryManager();
  const auto& arbitrator = memoryManager->arbitrator();
  const uint64_t numDrivers = 2;
  const auto expectedFreeCapacityBytes = arbitrator->stats().freeCapacityBytes;

  const uint64_t numBuildSideRows = 500;
  auto buildKeyVector = makeFlatVector<int64_t>(
      numBuildSideRows,
      [](vector_size_t row) { return folly::Random::rand64(); });
  auto buildSideVector =
      makeRowVector({"b0", "b1"}, {buildKeyVector, buildKeyVector});
  std::vector<RowVectorPtr> buildSideVectors;
  for (int i = 0; i < numDrivers; ++i) {
    buildSideVectors.push_back(buildSideVector);
  }
  createDuckDbTable("build", buildSideVectors);

  const uint64_t numProbeSideRows = 10;
  auto probeKeyVector = makeFlatVector<int64_t>(
      numProbeSideRows,
      [&](vector_size_t row) { return buildKeyVector->valueAt(row); });
  auto probeSideVector =
      makeRowVector({"p0", "p1"}, {probeKeyVector, probeKeyVector});
  std::vector<RowVectorPtr> probeSideVectors;
  for (int i = 0; i < numDrivers; ++i) {
    probeSideVectors.push_back(probeSideVector);
  }
  createDuckDbTable("probe", probeSideVectors);

  ASSERT_EQ(arbitrator->stats().freeCapacityBytes, expectedFreeCapacityBytes);

  // We set the task to fail right before we reserve memory for other operators.
  // We rely on the driver suspension before parallel join build to throw
  // exceptions (suspension on an already terminated task throws).
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::ensureTableFits",
      std::function<void(HashBuild*)>([&](HashBuild* buildOp) {
        try {
          VELOX_FAIL("Simulated failure");
        } catch (VeloxException&) {
          buildOp->operatorCtx()->task()->setError(std::current_exception());
        }
      }));

  std::vector<RowVectorPtr> probeInput = {probeSideVector};
  std::vector<RowVectorPtr> buildInput = {buildSideVector};
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();

  ASSERT_EQ(arbitrator->stats().freeCapacityBytes, expectedFreeCapacityBytes);
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->getPath())
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kJoinSpillEnabled, true)
          .queryCtx(
              newQueryCtx(memoryManager, executor_.get(), kMemoryCapacity))
          .maxDrivers(numDrivers)
          .plan(PlanBuilder(planNodeIdGenerator)
                    .values(probeInput, true)
                    .hashJoin(
                        {"p0"},
                        {"b0"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildInput, true)
                            .planNode(),
                        "",
                        {"p0", "p1", "b0", "b1"},
                        core::JoinType::kInner)
                    .planNode())
          .assertResults(
              "SELECT probe.p0, probe.p1, build.b0, build.b1 FROM probe "
              "INNER JOIN build ON probe.p0 = build.b0"),
      "Simulated failure");
  // This test uses on-demand created memory manager instead of the global
  // one. We need to make sure any used memory got cleaned up before exiting
  // the scope
  waitForAllTasksToBeDeleted();
  ASSERT_EQ(arbitrator->stats().freeCapacityBytes, expectedFreeCapacityBytes);
}

DEBUG_ONLY_TEST_F(HashJoinTest, arbitrationTriggeredDuringParallelJoinBuild) {
  std::unique_ptr<memory::MemoryManager> memoryManager = createMemoryManager();
  const uint64_t numDrivers = 2;

  // Large build side key product to bump hash mode to kHash instead of kArray
  // to trigger parallel join build.
  const uint64_t numBuildSideRows = 500;
  auto buildKeyVector = makeFlatVector<int64_t>(
      numBuildSideRows,
      [](vector_size_t row) { return folly::Random::rand64(); });
  auto buildSideVector = makeRowVector(
      {"b0", "b1", "b2"}, {buildKeyVector, buildKeyVector, buildKeyVector});
  std::vector<RowVectorPtr> buildSideVectors;
  for (int i = 0; i < numDrivers; ++i) {
    buildSideVectors.push_back(buildSideVector);
  }
  createDuckDbTable("build", buildSideVectors);

  const uint64_t numProbeSideRows = 10;
  auto probeKeyVector = makeFlatVector<int64_t>(
      numProbeSideRows,
      [&](vector_size_t row) { return buildKeyVector->valueAt(row); });
  auto probeSideVector = makeRowVector(
      {"p0", "p1", "p2"}, {probeKeyVector, probeKeyVector, probeKeyVector});
  std::vector<RowVectorPtr> probeSideVectors;
  for (int i = 0; i < numDrivers; ++i) {
    probeSideVectors.push_back(probeSideVector);
  }
  createDuckDbTable("probe", probeSideVectors);

  std::shared_ptr<core::QueryCtx> joinQueryCtx =
      newQueryCtx(memoryManager.get(), executor_.get(), kMemoryCapacity);

  const int64_t allocSize = 512LL << 20;
  std::atomic<bool> parallelBuildTriggered{false};
  std::atomic<memory::MemoryPool*> joinBuildPool{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashTable::parallelJoinBuild",
      std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
        parallelBuildTriggered = true;
        // Pick the last running driver threads' pool for later memory
        // allocation. This pick is rather arbitrary, as it is un-important
        // which pool is going to be allocated from later in a parallel join's
        // off-driver thread.
        joinBuildPool = pool;
      }));

  std::atomic_bool offThreadAllocationTriggered{false};
  folly::EventCount asyncMoveWait;
  std::atomic<bool> asyncMoveWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::AsyncSource::prepare",
      std::function<void(void*)>([&](void* /* unused */) {
        if (!offThreadAllocationTriggered.exchange(true)) {
          SCOPE_EXIT {
            asyncMoveWaitFlag = false;
            asyncMoveWait.notifyAll();
          };
          // Executed by the first thread hitting the test value location. This
          // allocation will trigger arbitration and fail.
          VELOX_ASSERT_THROW(
              joinBuildPool.load()->allocate(allocSize),
              "Exceeded memory pool cap");
        }
      }));

  // Wait for allocation (hence arbitration) on the prepare thread to finish
  // before calling AsyncSource::move(). This is to ensure no other AsyncSource
  // (hence arbitration) is running on the driver thread (on-thread) before the
  // ongoing arbitration finishes. Without ensuring this, the on-thread
  // arbitration (triggered by calling AsyncSource::move() first) has
  // thread-local driver context by default, defying the purpose of this test.
  SCOPED_TESTVALUE_SET(
      "facebook::velox::AsyncSource::move",
      std::function<void(void*)>([&](void* /* unused */) {
        asyncMoveWait.await([&]() { return !asyncMoveWaitFlag.load(); });
      }));

  std::vector<RowVectorPtr> probeInput = {probeSideVector};
  std::vector<RowVectorPtr> buildInput = {buildSideVector};
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  AssertQueryBuilder(duckDbQueryRunner_)
      .spillDirectory(spillDirectory->getPath())
      .config(core::QueryConfig::kSpillEnabled, true)
      .config(core::QueryConfig::kJoinSpillEnabled, true)
      // Set very low table size threshold to trigger parallel build.
      .config(core::QueryConfig::kMinTableRowsForParallelJoinBuild, 0)
      // Set multiple hash build drivers to trigger parallel build.
      .maxDrivers(numDrivers)
      .queryCtx(joinQueryCtx)
      .plan(PlanBuilder(planNodeIdGenerator)
                .values(probeInput, true)
                .hashJoin(
                    {"p0", "p1", "p2"},
                    {"b0", "b1", "b2"},
                    PlanBuilder(planNodeIdGenerator)
                        .values(buildInput, true)
                        .planNode(),
                    "",
                    {"p0", "p1", "b0", "b1"},
                    core::JoinType::kInner)
                .planNode())
      .assertResults(
          "SELECT probe.p0, probe.p1, build.b0, build.b1 FROM probe "
          "INNER JOIN build ON probe.p0 = build.b0 AND probe.p1 = build.b1 AND "
          "probe.p2 = build.b2");
  ASSERT_TRUE(parallelBuildTriggered);

  // This test uses on-demand created memory manager instead of the global
  // one. We need to make sure any used memory got cleaned up before exiting
  // the scope
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(HashJoinTest, arbitrationTriggeredByEnsureJoinTableFit) {
  // Use manual spill injection other than spill injection framework. This is
  // because spill injection framework does not allow fine grain spill within a
  // single operator (We do not want to spill during addInput() but only during
  // finishHashBuild()).
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::ensureTableFits",
      std::function<void(Operator*)>(([&](Operator* op) {
        Operator::ReclaimableSectionGuard guard(op);
        memory::testingRunArbitration(op->pool());
      })));
  auto tempDirectory = exec::test::TempDirectoryPath::create();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(numDrivers_)
      .injectSpill(false)
      .spillDirectory(tempDirectory->getPath())
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        const auto statsPair = taskSpilledStats(*task);
        ASSERT_GT(statsPair.first.spilledBytes, 0);
      })
      .run();
}

DEBUG_ONLY_TEST_F(HashJoinTest, joinBuildSpillError) {
  const int kMemoryCapacity = 32 << 20;
  // Set a small memory capacity to trigger spill.
  std::unique_ptr<memory::MemoryManager> memoryManager =
      createMemoryManager(kMemoryCapacity, 0);
  const auto& arbitrator = memoryManager->arbitrator();
  auto rowType = ROW(
      {{"c0", INTEGER()},
       {"c1", INTEGER()},
       {"c2", VARCHAR()},
       {"c3", VARCHAR()}});

  std::vector<RowVectorPtr> vectors = createVectors(16, rowType, fuzzerOpts_);
  createDuckDbTable(vectors);

  std::shared_ptr<core::QueryCtx> joinQueryCtx =
      newQueryCtx(memoryManager.get(), executor_.get(), kMemoryCapacity);

  const int numDrivers = 4;
  std::atomic<int> numAppends{0};
  const std::string injectedErrorMsg("injected spillError");
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::SpillState::appendToPartition",
      std::function<void(exec::SpillState*)>([&](exec::SpillState* state) {
        if (++numAppends != numDrivers) {
          return;
        }
        VELOX_FAIL(injectedErrorMsg);
      }));

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(vectors)
                  .project({"c0 AS t0", "c1 AS t1", "c2 AS t2"})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(vectors)
                          .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
                          .planNode(),
                      "",
                      {"t1"},
                      core::JoinType::kAnti)
                  .planNode();
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan)
          .queryCtx(joinQueryCtx)
          .spillDirectory(spillDirectory->getPath())
          .config(core::QueryConfig::kSpillEnabled, true)
          .copyResults(pool()),
      injectedErrorMsg);

  waitForAllTasksToBeDeleted();
  ASSERT_EQ(arbitrator->stats().numFailures, 1);

  // Wait again here as this test uses on-demand created memory manager instead
  // of the global one. We need to make sure any used memory got cleaned up
  // before exiting the scope
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(HashJoinTest, probeSpillOnWaitForPeers) {
  // This test creates a scenario when tester probe thread finishes processing
  // input, entering kWaitForPeers state, and the other thread is still
  // processing, spill is triggered properly performed.

  folly::EventCount startWait;
  folly::Synchronized<std::string> testerOpName;
  std::atomic_bool injectedSpillOnce{false};

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        testerOpName.withWLock([&](std::string& opName) {
          if (opName.empty()) {
            opName = op->pool()->name();
          }
        });
        if (op->pool()->name() == *testerOpName.rlock()) {
          // Do not block tester thread.
          return;
        }
        startWait.await([&]() { return injectedSpillOnce.load(); });
      }));

  // tester probe operator is guaranteed to be in kWaitForPeers state the next
  // isBlocked() is called after noMoreInput() is called.
  std::atomic_bool noMoreInputCalled{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::noMoreInput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        noMoreInputCalled = true;
      }));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::isBlocked",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        if (injectedSpillOnce || !noMoreInputCalled) {
          return;
        }
        injectedSpillOnce = true;
        EXPECT_EQ(
            dynamic_cast<HashProbe*>(op)->testingState(),
            ProbeOperatorState::kWaitForPeers);
        testingRunArbitration(op->pool());
      }));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Task::requestPauseLocked",
      std::function<void(Task*)>([&](Task* task) { startWait.notifyAll(); }));

  const uint64_t numDrivers{2};
  std::shared_ptr<core::QueryCtx> joinQueryCtx =
      newQueryCtx(memory::memoryManager(), executor_.get(), kMemoryCapacity);
  auto rowType = ROW({{"c0", INTEGER()}, {"c1", INTEGER()}});
  fuzzerOpts_.vectorSize = 20;
  std::vector<RowVectorPtr> vectors = createVectors(6, rowType, fuzzerOpts_);
  std::vector<RowVectorPtr> totalVectors;
  for (auto i = 0; i < numDrivers; ++i) {
    totalVectors.insert(totalVectors.end(), vectors.begin(), vectors.end());
  }
  createDuckDbTable(totalVectors);
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(vectors, true)
                  .project({"c0 AS t0", "c1 AS t1"})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(vectors, true)
                          .project({"c0 AS u0", "c1 AS u1"})
                          .planNode(),
                      "",
                      {"t1"},
                      core::JoinType::kInner)
                  .planNode();

  {
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .plan(plan)
            .queryCtx(joinQueryCtx)
            .spillDirectory(spillDirectory->getPath())
            .config(core::QueryConfig::kSpillEnabled, true)
            .maxDrivers(numDrivers)
            .assertResults("SELECT a.c1 from tmp a join tmp b on a.c0 = b.c0");

    auto opStats = toOperatorStats(task->taskStats());
    ASSERT_GT(opStats.at("HashProbe").spilledBytes, 0);
    ASSERT_EQ(opStats.at("HashBuild").spilledBytes, 0);

    const auto* arbitrator = memory::memoryManager()->arbitrator();
    ASSERT_GT(arbitrator->stats().reclaimedUsedBytes, 0);
  }
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(HashJoinTest, taskWaitTimeout) {
  const int queryMemoryCapacity = 128 << 20;
  // Creates a large number of vectors based on the query capacity to trigger
  // memory arbitration.
  fuzzerOpts_.vectorSize = 10'000;
  auto rowType = ROW(
      {{"c0", INTEGER()},
       {"c1", INTEGER()},
       {"c2", VARCHAR()},
       {"c3", VARCHAR()}});
  const auto vectors =
      createVectors(rowType, queryMemoryCapacity / 2, fuzzerOpts_);
  const int numDrivers = 4;
  const auto expectedResult =
      runHashJoinTask(vectors, nullptr, false, numDrivers, pool(), false).data;

  for (uint64_t timeoutMs : {1'000, 30'000}) {
    SCOPED_TRACE(fmt::format("timeout {}", succinctMillis(timeoutMs)));
    auto memoryManager = createMemoryManager(512 << 20, 0, timeoutMs);
    auto queryCtx =
        newQueryCtx(memoryManager.get(), executor_.get(), queryMemoryCapacity);

    // Set test injection to block one hash build operator to inject delay when
    // memory reclaim waits for task to pause.
    folly::EventCount buildBlockWait;
    std::atomic<bool> buildBlockWaitFlag{true};
    std::atomic<bool> blockOneBuild{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
        std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
          const std::string re(".*HashBuild");
          if (!RE2::FullMatch(pool->name(), re)) {
            return;
          }
          if (!blockOneBuild.exchange(false)) {
            return;
          }
          buildBlockWait.await([&]() { return !buildBlockWaitFlag.load(); });
        }));

    folly::EventCount taskPauseWait;
    std::atomic<bool> taskPauseWaitFlag{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Task::requestPauseLocked",
        std::function<void(Task*)>(([&](Task* /*unused*/) {
          taskPauseWaitFlag = true;
          taskPauseWait.notifyAll();
        })));

    std::thread queryThread([&]() {
      // We expect failure on short time out.
      if (timeoutMs == 1'000) {
        VELOX_ASSERT_THROW(
            runHashJoinTask(
                vectors,
                queryCtx,
                false,
                numDrivers,
                pool(),
                true,
                expectedResult),
            "Memory reclaim failed to wait");
      } else {
        // We expect succeed on large time out or no timeout.
        const auto result = runHashJoinTask(
            vectors, queryCtx, false, numDrivers, pool(), true, expectedResult);
        auto taskStats = exec::toPlanStats(result.task->taskStats());
        auto& planStats = taskStats.at(result.planNodeId);
        ASSERT_GT(planStats.spilledBytes, 0);
      }
    });

    // Wait for task pause to reach, and then delay for a while before unblock
    // the blocked hash build operator.
    taskPauseWait.await([&]() { return taskPauseWaitFlag.load(); });
    // Wait for two seconds and expect the short reclaim wait timeout.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // Unblock the blocked build operator to let memory reclaim proceed.
    buildBlockWaitFlag = false;
    buildBlockWait.notifyAll();

    queryThread.join();

    // This test uses on-demand created memory manager instead of the global
    // one. We need to make sure any used memory got cleaned up before exiting
    // the scope
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashProbeSpill) {
  struct {
    bool triggerBuildSpill;
    // Triggers after no more input or not.
    bool afterNoMoreInput;
    // The index of get output call to trigger probe side spilling.
    int probeOutputIndex;

    std::string debugString() const {
      return fmt::format(
          "triggerBuildSpill: {}, afterNoMoreInput: {}, probeOutputIndex: {}",
          triggerBuildSpill,
          afterNoMoreInput,
          probeOutputIndex);
    }
  } testSettings[] = {
      {false, false, 0},
      {false, false, 1},
      {false, false, 10},
      {false, true, 0},
      {true, false, 0},
      {true, false, 1},
      {true, false, 10},
      {true, true, 0}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::atomic_bool injectBuildSpillOnce{true};
    std::atomic_int buildInputCount{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>([&](Operator* op) {
          if (!testData.triggerBuildSpill) {
            return;
          }
          if (!isHashBuildMemoryPool(*op->pool())) {
            return;
          }
          if (buildInputCount++ != 1) {
            return;
          }
          if (!injectBuildSpillOnce.exchange(false)) {
            return;
          }
          testingRunArbitration(op->pool());
        }));

    std::atomic_bool injectProbeSpillOnce{true};
    std::atomic_int probeOutputCount{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::getOutput",
        std::function<void(Operator*)>([&](Operator* op) {
          if (!isHashProbeMemoryPool(*op->pool())) {
            return;
          }
          if (testData.afterNoMoreInput) {
            if (!op->testingNoMoreInput()) {
              return;
            }
          } else {
            if (probeOutputCount++ != testData.probeOutputIndex) {
              return;
            }
          }
          if (!injectProbeSpillOnce.exchange(false)) {
            return;
          }
          testingRunArbitration(op->pool());
        }));

    fuzzerOpts_.vectorSize = 128;
    auto probeVectors = createVectors(10, probeType_, fuzzerOpts_);
    auto buildVectors = createVectors(20, buildType_, fuzzerOpts_);
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(1)
        .spillDirectory(spillDirectory->getPath())
        .probeKeys({"t_k1"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_k1"})
        .buildVectors(std::move(buildVectors))
        .config(core::QueryConfig::kJoinSpillEnabled, "true")
        .joinType(core::JoinType::kRight)
        .joinOutputLayout({"t_k1", "t_k2", "u_k1", "t_v1"})
        .referenceQuery(
            "SELECT t.t_k1, t.t_k2, u.u_k1, t.t_v1 FROM t RIGHT JOIN u ON t.t_k1 = u.u_k1")
        .injectSpill(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          auto opStats = toOperatorStats(task->taskStats());
          ASSERT_GT(opStats.at("HashProbe").spilledBytes, 0);
          if (testData.triggerBuildSpill) {
            ASSERT_GT(opStats.at("HashBuild").spilledBytes, 0);
          } else {
            ASSERT_EQ(opStats.at("HashBuild").spilledBytes, 0);
          }

          const auto* arbitrator = memory::memoryManager()->arbitrator();
          ASSERT_GT(arbitrator->stats().numRequests, 0);
          ASSERT_GT(arbitrator->stats().reclaimedUsedBytes, 0);
        })
        .run();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashProbeSpillInMiddeOfLastOutputProcessing) {
  std::atomic_int outputCountAfterNoMoreInout{0};
  std::atomic_bool injectOnce{true};
  ::facebook::velox::common::testutil::ScopedTestValue abc(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        if (!op->testingNoMoreInput()) {
          return;
        }
        if (outputCountAfterNoMoreInout++ != 1) {
          return;
        }
        if (!injectOnce.exchange(false)) {
          return;
        }
        testingRunArbitration(op->pool());
      }));

  fuzzerOpts_.vectorSize = 128;
  auto probeVectors = createVectors(10, probeType_, fuzzerOpts_);
  auto buildVectors = createVectors(20, buildType_, fuzzerOpts_);

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .spillDirectory(spillDirectory->getPath())
      .probeKeys({"t_k1"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_k1"})
      .buildVectors(std::move(buildVectors))
      .config(core::QueryConfig::kJoinSpillEnabled, "true")
      .config(core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
      .joinType(core::JoinType::kRight)
      .joinOutputLayout({"t_k1", "t_k2", "u_k1", "t_v1"})
      .referenceQuery(
          "SELECT t.t_k1, t.t_k2, u.u_k1, t.t_v1 FROM t RIGHT JOIN u ON t.t_k1 = u.u_k1")
      .injectSpill(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        auto opStats = toOperatorStats(task->taskStats());
        ASSERT_GT(opStats.at("HashProbe").spilledBytes, 0);
        // Verifies that we only spill the output which is single partitioned
        // but not the hash table.
        ASSERT_EQ(opStats.at("HashProbe").spilledPartitions, 1);
      })
      .run();
}

// Inject probe-side spilling in the middle of output processing. If
// 'recursiveSpill' is true, we trigger probe-spilling when probe the hash table
// built from spilled data.
DEBUG_ONLY_TEST_F(HashJoinTest, hashProbeSpillInMiddeOfOutputProcessing) {
  for (bool recursiveSpill : {false, true}) {
    std::atomic_int buildInputCount{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>([&](Operator* op) {
          if (!isHashBuildMemoryPool(*op->pool())) {
            return;
          }
          if (!recursiveSpill) {
            return;
          }
          // Trigger spill after the build side has processed some rows.
          if (buildInputCount++ != 1) {
            return;
          }
          testingRunArbitration(op->pool());
        }));

    std::atomic_bool injectProbeSpillOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::getOutput",
        std::function<void(Operator*)>([&](Operator* op) {
          if (!isHashProbeMemoryPool(*op->pool())) {
            return;
          }

          if (op->testingHasInput()) {
            return;
          }
          if (recursiveSpill) {
            if (static_cast<HashProbe*>(op)->testingHasInputSpiller()) {
              return;
            }
          }
          if (!injectProbeSpillOnce.exchange(false)) {
            return;
          }
          testingRunArbitration(op->pool());
        }));

    fuzzerOpts_.vectorSize = 128;
    auto probeVectors = createVectors(10, probeType_, fuzzerOpts_);
    auto buildVectors = createVectors(20, buildType_, fuzzerOpts_);

    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(1)
        .spillDirectory(spillDirectory->getPath())
        .probeKeys({"t_k1"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_k1"})
        .buildVectors(std::move(buildVectors))
        .config(core::QueryConfig::kJoinSpillEnabled, "true")
        .config(
            core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
        .joinType(core::JoinType::kRight)
        .joinOutputLayout({"t_k1", "t_k2", "u_k1", "t_v1"})
        .referenceQuery(
            "SELECT t.t_k1, t.t_k2, u.u_k1, t.t_v1 FROM t RIGHT JOIN u ON t.t_k1 = u.u_k1")
        .injectSpill(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          auto opStats = toOperatorStats(task->taskStats());
          ASSERT_GT(opStats.at("HashProbe").spilledBytes, 0);
          ASSERT_GT(opStats.at("HashProbe").spilledPartitions, 1);
        })
        .run();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashProbeSpillWhenOneOfProbeFinish) {
  const int numDrivers{3};

  std::atomic_bool probeWaitFlag{true};
  folly::EventCount probeWait;
  std::atomic_int numBlockedProbeOps{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        if (++numBlockedProbeOps <= numDrivers - 1) {
          probeWait.await([&]() { return !probeWaitFlag.load(); });
          return;
        }
      }));

  std::atomic_bool notifyOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::noMoreInput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (!isHashProbeMemoryPool(*op->pool())) {
          return;
        }
        if (!notifyOnce.exchange(false)) {
          return;
        }
        probeWaitFlag = false;
        probeWait.notifyAll();
      }));

  std::thread queryThread([&]() {
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers, true, true)
        .spillDirectory(spillDirectory->getPath())
        .keyTypes({BIGINT()})
        .probeVectors(32, 5)
        .buildVectors(32, 5)
        .config(core::QueryConfig::kJoinSpillEnabled, "true")
        .referenceQuery(
            "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
        .injectSpill(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          auto opStats = toOperatorStats(task->taskStats());
          ASSERT_EQ(opStats.at("HashBuild").spilledBytes, 0);
          ASSERT_GT(opStats.at("HashProbe").spilledBytes, 0);
        })
        .run();
  });
  // Wait until one of the hash probe operator has finished.
  probeWait.await([&]() { return !probeWaitFlag.load(); });
  memory::testingRunArbitration();
  queryThread.join();
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashProbeSpillExceedLimit) {
  // If 'buildTriggerSpill' is true, then spilling is triggered by hash build.
  for (const bool buildTriggerSpill : {false, true}) {
    SCOPED_TRACE(fmt::format("buildTriggerSpill {}", buildTriggerSpill));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
        std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
          if (buildTriggerSpill && !isHashBuildMemoryPool(*pool)) {
            return;
          }
          if (!buildTriggerSpill && !isHashProbeMemoryPool(*pool)) {
            return;
          }
          testingRunArbitration(pool);
        }));

    fuzzerOpts_.vectorSize = 128;
    auto probeVectors = createVectors(32, probeType_, fuzzerOpts_);
    auto buildVectors = createVectors(64, buildType_, fuzzerOpts_);
    for (int i = 0; i < probeVectors.size(); ++i) {
      const auto probeKeyChannel = probeType_->getChildIdx("t_k1");
      const auto buildKeyChannle = buildType_->getChildIdx("u_k1");
      probeVectors[i]->childAt(probeKeyChannel) =
          buildVectors[i]->childAt(buildKeyChannle);
    }

    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(1)
        .spillDirectory(spillDirectory->getPath())
        .probeKeys({"t_k1"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u_k1"})
        .buildVectors(std::move(buildVectors))
        .config(core::QueryConfig::kMaxSpillLevel, "1")
        .config(core::QueryConfig::kSpillNumPartitionBits, "1")
        .config(core::QueryConfig::kJoinSpillEnabled, "true")
        // Set small write buffer size to have small vectors to read from
        // spilled data.
        .config(core::QueryConfig::kSpillWriteBufferSize, "1")
        .config(
            core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
        .joinType(core::JoinType::kRight)
        .joinOutputLayout({"t_k1", "t_k2", "u_k1", "t_v1"})
        .referenceQuery(
            "SELECT t.t_k1, t.t_k2, u.u_k1, t.t_v1 FROM t RIGHT JOIN u ON t.t_k1 = u.u_k1")
        .injectSpill(false)
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          auto opStats = toOperatorStats(task->taskStats());
          if (buildTriggerSpill) {
            ASSERT_GT(opStats.at("HashProbe").spilledBytes, 0);
            ASSERT_GT(opStats.at("HashBuild").spilledBytes, 0);
          } else {
            ASSERT_GT(opStats.at("HashProbe").spilledBytes, 0);
            ASSERT_EQ(opStats.at("HashBuild").spilledBytes, 0);
          }
          ASSERT_GT(
              opStats.at("HashProbe")
                  .runtimeStats[Operator::kExceededMaxSpillLevel]
                  .sum,
              0);
          ASSERT_GT(
              opStats.at("HashBuild")
                  .runtimeStats[Operator::kExceededMaxSpillLevel]
                  .sum,
              0);
        })
        .run();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashProbeSpillUnderNonReclaimableSection) {
  std::atomic_bool injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
      std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
        if (!isHashProbeMemoryPool(*pool)) {
          return;
        }
        if (!injectOnce.exchange(false)) {
          return;
        }
        auto* arbitrator = memory::memoryManager()->arbitrator();
        const auto numNonReclaimableAttempts =
            arbitrator->stats().numNonReclaimableAttempts;
        testingRunArbitration(pool);
        // Verifies that we run into non-reclaimable section when reclaim from
        // hash probe.
        ASSERT_EQ(
            arbitrator->stats().numNonReclaimableAttempts,
            numNonReclaimableAttempts + 1);
      }));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .spillDirectory(spillDirectory->getPath())
      .keyTypes({BIGINT()})
      .probeVectors(32, 5)
      .buildVectors(32, 5)
      .config(core::QueryConfig::kJoinSpillEnabled, "true")
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .injectSpill(false)
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        auto opStats = toOperatorStats(task->taskStats());
        ASSERT_EQ(opStats.at("HashProbe").spilledBytes, 0);
        ASSERT_EQ(opStats.at("HashBuild").spilledBytes, 0);
      })
      .run();
}

// This test case is to cover the case that hash probe trigger spill for right
// semi join types and the pending input needs to be processed in multiple
// steps.
DEBUG_ONLY_TEST_F(HashJoinTest, spillOutputWithRightSemiJoins) {
  for (const auto joinType :
       {core::JoinType::kRightSemiFilter, core::JoinType::kRightSemiProject}) {
    std::atomic_bool injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::getOutput",
        std::function<void(Operator*)>([&](Operator* op) {
          if (op->operatorCtx()->operatorType() != "HashProbe") {
            return;
          }
          if (!op->testingHasInput()) {
            return;
          }
          if (!injectOnce.exchange(false)) {
            return;
          }
          testingRunArbitration(op->pool());
        }));

    std::string duckDbSqlReference;
    std::vector<std::string> joinOutputLayout;
    bool nullAware{false};
    if (joinType == core::JoinType::kRightSemiProject) {
      duckDbSqlReference = "SELECT u_k2, u_k1 IN (SELECT t_k1 FROM t) FROM u";
      joinOutputLayout = {"u_k2", "match"};
      // Null aware is only supported for semi projection join type.
      nullAware = true;
    } else {
      duckDbSqlReference =
          "SELECT u_k2 FROM u WHERE u_k1 IN (SELECT t_k1 FROM t)";
      joinOutputLayout = {"u_k2"};
    }

    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(1)
        .spillDirectory(spillDirectory->getPath())
        .probeType(probeType_)
        .probeVectors(128, 3)
        .probeKeys({"t_k1"})
        .buildType(buildType_)
        .buildVectors(128, 4)
        .buildKeys({"u_k1"})
        .joinType(joinType)
        // Set a small number of output rows to process the input in multiple
        // steps.
        .config(
            core::QueryConfig::kPreferredOutputBatchRows, std::to_string(10))
        .injectSpill(false)
        .joinOutputLayout(std::move(joinOutputLayout))
        .nullAware(nullAware)
        .referenceQuery(duckDbSqlReference)
        .run();
  }
}

DEBUG_ONLY_TEST_F(HashJoinTest, spillCheckOnLeftSemiFilterWithDynamicFilters) {
  const int32_t numSplits = 10;
  const int32_t numRowsProbe = 333;
  const int32_t numRowsBuild = 100;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);

  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  for (int32_t i = 0; i < numSplits; ++i) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numRowsProbe, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numRowsProbe, [](auto row) { return row; }),
    });
    probeVectors.push_back(rowVector);
    tempFiles.push_back(TempFilePath::create());
    writeToFile(tempFiles.back()->getPath(), rowVector);
  }
  auto makeInputSplits = [&](const core::PlanNodeId& nodeId) {
    return [&] {
      std::vector<exec::Split> probeSplits;
      for (auto& file : tempFiles) {
        probeSplits.push_back(
            exec::Split(makeHiveConnectorSplit(file->getPath())));
      }
      SplitInput splits;
      splits.emplace(nodeId, probeSplits);
      return splits;
    };
  };

  // 100 key values in [35, 233] range.
  std::vector<RowVectorPtr> buildVectors;
  for (int i = 0; i < 5; ++i) {
    buildVectors.push_back(makeRowVector({
        makeFlatVector<int32_t>(
            numRowsBuild / 5,
            [i](auto row) { return 35 + 2 * (row + i * numRowsBuild / 5); }),
        makeFlatVector<int64_t>(numRowsBuild / 5, [](auto row) { return row; }),
    }));
  }
  std::vector<RowVectorPtr> keyOnlyBuildVectors;
  for (int i = 0; i < 5; ++i) {
    keyOnlyBuildVectors.push_back(
        makeRowVector({makeFlatVector<int32_t>(numRowsBuild / 5, [i](auto row) {
          return 35 + 2 * (row + i * numRowsBuild / 5);
        })}));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .values(buildVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();

  // Left semi join.
  core::PlanNodeId probeScanId;
  core::PlanNodeId joinNodeId;
  const auto op = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(probeType)
                      .capturePlanNodeId(probeScanId)
                      .hashJoin(
                          {"c0"},
                          {"u_c0"},
                          buildSide,
                          "",
                          {"c0", "c1"},
                          core::JoinType::kLeftSemiFilter)
                      .capturePlanNodeId(joinNodeId)
                      .project({"c0", "c1 + 1"})
                      .planNode();

  std::atomic_bool injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (op->operatorCtx()->operatorType() != "HashProbe") {
          return;
        }
        if (!op->testingHasInput()) {
          return;
        }
        if (!injectOnce.exchange(false)) {
          return;
        }
        testingRunArbitration(op->pool());
      }));

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .planNode(std::move(op))
      .makeInputSplits(makeInputSplits(probeScanId))
      .spillDirectory(spillDirectory->getPath())
      .injectSpill(false)
      .referenceQuery(
          "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)")
      .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
        // Verify spill hasn't triggered.
        auto taskStats = exec::toPlanStats(task->taskStats());
        auto& planStats = taskStats.at(joinNodeId);
        ASSERT_GT(planStats.spilledBytes, 0);
      })
      .run();
}

// This test is to verify there is no memory reservation made before hash probe
// start processing. This can cause unnecessary spill and query OOM under some
// real workload with many stages as each hash probe might reserve non-trivial
// amount of memory.
DEBUG_ONLY_TEST_F(
    HashJoinTest,
    hashProbeMemoryReservationCheckBeforeProbeStartWithSpillEnabled) {
  fuzzerOpts_.vectorSize = 128;
  auto probeVectors = createVectors(10, probeType_, fuzzerOpts_);
  auto buildVectors = createVectors(20, buildType_, fuzzerOpts_);

  std::atomic_bool checkOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "HashProbe") {
          return;
        }
        if (!checkOnce.exchange(false)) {
          return;
        }
        ASSERT_EQ(op->pool()->usedBytes(), 0);
        ASSERT_EQ(op->pool()->reservedBytes(), 0);
      })));

  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .spillDirectory(spillDirectory->getPath())
      .probeKeys({"t_k1"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_k1"})
      .buildVectors(std::move(buildVectors))
      .config(core::QueryConfig::kJoinSpillEnabled, "true")
      .joinType(core::JoinType::kInner)
      .joinOutputLayout({"t_k1", "t_k2", "u_k1", "t_v1"})
      .referenceQuery(
          "SELECT t.t_k1, t.t_k2, u.u_k1, t.t_v1 FROM t JOIN u ON t.t_k1 = u.u_k1")
      .injectSpill(true)
      .verifier([&](const std::shared_ptr<Task>& task, bool injectSpill) {
        if (!injectSpill) {
          return;
        }
        auto opStats = toOperatorStats(task->taskStats());
        ASSERT_GT(opStats.at("HashProbe").spilledBytes, 0);
        ASSERT_GE(opStats.at("HashProbe").spilledPartitions, 1);
      })
      .run();
}

TEST_F(HashJoinTest, nanKeys) {
  // Verify the NaN values with different binary representations are considered
  // equal.
  static const double kNan = std::numeric_limits<double>::quiet_NaN();
  static const double kSNaN = std::numeric_limits<double>::signaling_NaN();
  auto probeInput = makeRowVector(
      {makeFlatVector<double>({kNan, kSNaN}), makeFlatVector<int64_t>({1, 2})});
  auto buildInput = makeRowVector({makeFlatVector<double>({kNan, 1})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({probeInput})
                  .project({"c0 AS t0", "c1 AS t1"})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildInput})
                          .project({"c0 AS u0"})
                          .planNode(),
                      "",
                      {"t0", "u0", "t1"},
                      core::JoinType::kLeft)
                  .planNode();
  auto queryCtx = core::QueryCtx::create(executor_.get());
  auto result =
      AssertQueryBuilder(plan).queryCtx(queryCtx).copyResults(pool_.get());
  auto expected = makeRowVector(
      {makeFlatVector<double>({kNan, kNan}),
       makeFlatVector<double>({kNan, kNan}),
       makeFlatVector<int64_t>({1, 2})});
  facebook::velox::test::assertEqualVectors(expected, result);
}

DEBUG_ONLY_TEST_F(HashJoinTest, spillOnBlockedProbe) {
  auto blockedOperatorFactoryUniquePtr =
      std::make_unique<BlockedOperatorFactory>();
  auto blockedOperatorFactory = blockedOperatorFactoryUniquePtr.get();
  Operator::registerOperator(std::move(blockedOperatorFactoryUniquePtr));

  std::vector<ContinuePromise> unblockPromises;
  std::atomic_bool shouldBlock{true};
  blockedOperatorFactory->setBlockedCb([&](ContinueFuture* future) {
    if (!shouldBlock) {
      return BlockingReason::kNotBlocked;
    }
    auto [p, f] = makeVeloxContinuePromiseContract("Blocked Operator");
    *future = std::move(f);
    unblockPromises.push_back(std::move(p));
    return BlockingReason::kWaitForConsumer;
  });

  folly::EventCount arbitrationWait;
  std::atomic<bool> arbitrationWaitFlag{true};
  ::facebook::velox::common::testutil::ScopedTestValue _scopedTestValue15(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(exec::HashBuild*)>([&](Operator* /* unused */) {
        arbitrationWaitFlag = false;
        arbitrationWait.notifyAll();
      }));
  std::thread arbitrationThread([&]() {
    arbitrationWait.await([&]() { return !arbitrationWaitFlag.load(); });
    memory::memoryManager()->shrinkPools();
    shouldBlock = false;
    for (auto& unblockPromise : unblockPromises) {
      unblockPromise.setValue();
    }
  });

  auto rowType = ROW({{"c0", INTEGER()}, {"c1", INTEGER()}});
  std::vector<RowVectorPtr> vectors = createVectors(1, rowType, fuzzerOpts_);
  createDuckDbTable(vectors);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(vectors)
                  .project({"c0 AS t0", "c1 AS t1"})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(vectors)
                          .project({"c0 AS u0", "c1 AS u1"})
                          .planNode(),
                      "",
                      {"t1"},
                      core::JoinType::kInner)
                  .addNode([&](std::string id, core::PlanNodePtr input) {
                    return std::make_shared<BlockedNode>(id, input);
                  })
                  .planNode();

  {
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .plan(plan)
            .queryCtx(newQueryCtx(
                memory::memoryManager(), executor_.get(), kMemoryCapacity))
            .spillDirectory(spillDirectory->getPath())
            .config(core::QueryConfig::kSpillEnabled, true)
            .maxDrivers(1)
            .assertResults("SELECT a.c1 from tmp a join tmp b on a.c0 = b.c0");
    auto joinSpillStats = taskSpilledStats(*task);
    auto buildSpillStats = joinSpillStats.first;
    ASSERT_GT(buildSpillStats.spilledBytes, 0);
  }
  arbitrationThread.join();
  waitForAllTasksToBeDeleted(30'000'000);
}

DEBUG_ONLY_TEST_F(HashJoinTest, buildReclaimedMemoryReport) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  const int32_t numBuildVectors = 3;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    VectorFuzzer fuzzer({.vectorSize = 200}, pool());
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }

  const int32_t numProbeVectors = 3;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    VectorFuzzer fuzzer({.vectorSize = 200}, pool());
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  const int numDrivers{2};
  // duckdb need double probe and build inputs as we run two drivers for hash
  // join.
  std::vector<RowVectorPtr> totalProbeVectors = probeVectors;
  totalProbeVectors.insert(
      totalProbeVectors.end(), probeVectors.begin(), probeVectors.end());
  std::vector<RowVectorPtr> totalBuildVectors = buildVectors;
  totalBuildVectors.insert(
      totalBuildVectors.end(), buildVectors.begin(), buildVectors.end());

  createDuckDbTable("t", totalProbeVectors);
  createDuckDbTable("u", totalBuildVectors);

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryPool = memory::memoryManager()->addRootPool(
      "", kMaxBytes, memory::MemoryReclaimer::create());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, true)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, true)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  folly::EventCount driverWait;
  std::atomic_bool driverWaitFlag{true};
  folly::EventCount taskWait;
  std::atomic_bool taskWaitFlag{true};

  Operator* op{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashBuild::finishHashBuild",
      std::function<void(Operator*)>(([&](Operator* testOp) { op = testOp; })));

  std::atomic_bool injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            if (op == nullptr || op->pool() != pool) {
              return;
            }
            ASSERT_TRUE(isHashBuildMemoryPool(*pool));
            ASSERT_TRUE(op->canReclaim());
            ASSERT_GT(op->pool()->usedBytes(), 0);
            ASSERT_GT(
                op->pool()->parent()->reservedBytes(),
                op->pool()->reservedBytes());
            if (!injectOnce.exchange(false)) {
              return;
            }
            uint64_t reclaimableBytes{0};
            const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
            ASSERT_TRUE(reclaimable);
            ASSERT_GT(reclaimableBytes, 0);
            auto* driver = op->operatorCtx()->driver();
            TestSuspendedSection suspendedSection(driver);
            taskWaitFlag = false;
            taskWait.notifyAll();
            driverWait.await([&]() { return !driverWaitFlag.load(); });
          })));

  std::thread taskThread([&]() {
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(numDrivers)
        .planNode(plan)
        .queryPool(std::move(queryPool))
        .injectSpill(false)
        .spillDirectory(tempDirectory->getPath())
        .referenceQuery(
            "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
        .config(core::QueryConfig::kSpillStartPartitionBit, "29")
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          ASSERT_GT(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 16);
          ASSERT_GT(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 16);
          verifyTaskSpilledRuntimeStats(*task, true);
        })
        .run();
  });

  taskWait.await([&]() { return !taskWaitFlag.load(); });
  ASSERT_TRUE(op != nullptr);
  auto task = op->operatorCtx()->task();
  auto* nodePool = op->pool()->parent();
  const auto nodeMemoryUsage = nodePool->reservedBytes();
  {
    memory::ScopedMemoryArbitrationContext ctx(op->pool());
    const uint64_t reclaimedBytes = task->pool()->reclaim(
        task->pool()->capacity(), 1'000'000, reclaimerStats_);
    ASSERT_GT(reclaimedBytes, 0);
    ASSERT_EQ(nodeMemoryUsage - nodePool->reservedBytes(), reclaimedBytes);
  }
  // Verify all the memory has been freed.
  ASSERT_EQ(nodePool->reservedBytes(), 0);

  driverWaitFlag = false;
  driverWait.notifyAll();
  task.reset();

  taskThread.join();
}

DEBUG_ONLY_TEST_F(HashJoinTest, probeReclaimedMemoryReport) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  const int32_t numBuildVectors = 3;
  std::vector<RowVectorPtr> buildVectors;
  for (int32_t i = 0; i < numBuildVectors; ++i) {
    VectorFuzzer fuzzer({.vectorSize = 200}, pool());
    buildVectors.push_back(fuzzer.fuzzRow(buildType_));
  }

  const int32_t numProbeVectors = 3;
  std::vector<RowVectorPtr> probeVectors;
  for (int32_t i = 0; i < numProbeVectors; ++i) {
    VectorFuzzer fuzzer({.vectorSize = 200}, pool());
    probeVectors.push_back(fuzzer.fuzzRow(probeType_));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryPool = memory::memoryManager()->addRootPool(
      "", kMaxBytes, memory::MemoryReclaimer::create());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, true)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, true)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .planNode();

  folly::EventCount driverWait;
  std::atomic_bool driverWaitFlag{true};
  folly::EventCount taskWait;
  std::atomic_bool taskWaitFlag{true};

  Operator* op{nullptr};
  std::atomic_int probeInputCount{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "HashProbe") {
          return;
        }
        op = testOp;

        ASSERT_TRUE(op->canReclaim());
        if (probeInputCount++ != 1) {
          return;
        }
        auto* driver = op->operatorCtx()->driver();
        TestSuspendedSection suspendedSection(driver);
        taskWaitFlag = false;
        taskWait.notifyAll();
        driverWait.await([&]() { return !driverWaitFlag.load(); });
      })));

  std::thread taskThread([&]() {
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .numDrivers(1)
        .planNode(plan)
        .queryPool(std::move(queryPool))
        .injectSpill(false)
        .spillDirectory(tempDirectory->getPath())
        .referenceQuery(
            "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
        .config(core::QueryConfig::kSpillStartPartitionBit, "29")
        .verifier([&](const std::shared_ptr<Task>& task, bool /*unused*/) {
          const auto statsPair = taskSpilledStats(*task);
          // The spill triggered at the probe side.
          ASSERT_EQ(statsPair.first.spilledBytes, 0);
          ASSERT_EQ(statsPair.first.spilledPartitions, 0);
          ASSERT_GT(statsPair.second.spilledBytes, 0);
          ASSERT_EQ(statsPair.second.spilledPartitions, 16);
        })
        .run();
  });

  taskWait.await([&]() { return !taskWaitFlag.load(); });
  ASSERT_TRUE(op != nullptr);
  auto task = op->operatorCtx()->task();
  auto* nodePool = op->pool()->parent();
  const auto nodeMemoryUsage = nodePool->reservedBytes();
  {
    memory::ScopedMemoryArbitrationContext ctx(op->pool());
    const uint64_t reclaimedBytes = task->pool()->reclaim(
        task->pool()->capacity(), 1'000'000, reclaimerStats_);
    ASSERT_GT(reclaimedBytes, 0);
    ASSERT_EQ(nodeMemoryUsage - nodePool->reservedBytes(), reclaimedBytes);
  }
  // Verify all the memory has been freed, except for the ones for hash lookup.
  ASSERT_EQ(nodePool->reservedBytes(), 1048576);

  driverWaitFlag = false;
  driverWait.notifyAll();
  task.reset();

  taskThread.join();
}

DEBUG_ONLY_TEST_F(HashJoinTest, hashTableCleanupAfterProbeFinish) {
  auto buildVectors = makeVectors(buildType_, 5, 100);
  auto probeVectors = makeVectors(probeType_, 5, 100);

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  HashProbe* probeOp{nullptr};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (probeOp == nullptr && op->operatorType() == "HashProbe") {
          probeOp = dynamic_cast<HashProbe*>(op);
        }
      }));

  bool tableEmpty{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::noMoreInput",
      std::function<void(Operator*)>([&](Operator* op) {
        if (op->operatorType() == "FilterProject") {
          tableEmpty = (probeOp->testingTable()->numDistinct() == 0);
        }
      }));

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors, true)
                  .hashJoin(
                      {"t_k1"},
                      {"u_k1"},
                      PlanBuilder(planNodeIdGenerator)
                          .values(buildVectors, true)
                          .planNode(),
                      "",
                      concat(probeType_->names(), buildType_->names()))
                  .project({"t_k1", "t_k2", "t_v1", "u_k1", "u_k2", "u_v1"})
                  .planNode();

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .planNode(plan)
      .injectSpill(false)
      .spillDirectory(tempDirectory->getPath())
      .referenceQuery(
          "SELECT t_k1, t_k2, t_v1, u_k1, u_k2, u_v1 FROM t, u WHERE t.t_k1 = u.u_k1")
      .config(core::QueryConfig::kSpillStartPartitionBit, "29")
      .run();
  ASSERT_TRUE(tableEmpty);
}

} // namespace
} // namespace facebook::velox::exec
