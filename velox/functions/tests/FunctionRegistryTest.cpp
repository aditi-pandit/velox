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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/expression/FunctionSignature.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/IPPrefixRegistration.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"
#include "velox/functions/tests/RegistryTestUtil.h"
#include "velox/type/Type.h"

#define VELOX_EXPECT_EQ_TYPES(actual, expected)                                \
  if (expected != nullptr) {                                                   \
    ASSERT_TRUE(actual != nullptr)                                             \
        << "Expected: " << expected->toString() << ", got null";               \
    EXPECT_EQ(*actual, *expected) << "Expected: " << expected->toString()      \
                                  << ", got " << actual->toString();           \
  } else {                                                                     \
    EXPECT_EQ(actual, nullptr) << "Expected null, got " << actual->toString(); \
  }

namespace facebook::velox {
namespace {

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_func_one,
    VectorFuncOne::signatures(),
    std::make_unique<VectorFuncOne>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_func_two,
    VectorFuncTwo::signatures(),
    std::make_unique<VectorFuncTwo>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_func_three,
    VectorFuncThree::signatures(),
    std::make_unique<VectorFuncThree>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_vector_func_four,
    VectorFuncFour::signatures(),
    exec::VectorFunctionMetadataBuilder().deterministic(false).build(),
    std::make_unique<VectorFuncFour>());

inline void registerTestFunctions() {
  // If no alias is specified, ensure it will fallback to the struct name.
  registerFunction<FuncOne, Varchar, Varchar>({"func_one", "Func_One_Alias"});

  // func_two has two signatures.
  registerFunction<FuncTwo, int64_t, int64_t, int32_t>({"func_two"});
  registerFunction<FuncTwo, int64_t, int64_t, int16_t>({"func_two"});

  // func_three has two aliases.
  registerFunction<FuncThree, Array<int64_t>, Array<int64_t>>(
      {"func_three_alias1", "func_three_alias2"});

  // We swap func_four and func_five while registering.
  registerFunction<FuncFour, Varchar, Varchar>({"func_five"});
  registerFunction<FuncFive, int64_t, int64_t>({"func_four"});

  registerFunction<VariadicFunc, Varchar, Variadic<Varchar>>(
      {"variadic_func", "Variadic_Func_Alias"});

  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_one, "vector_func_one");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_one, "Vector_Func_One_Alias");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_two, "vector_func_two");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_three, "vector_func_three");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_four, "vector_func_four");
}

inline void registerTestVectorFunctionOne(const std::string& functionName) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_one, functionName);
}

class FunctionRegistryTest : public testing::Test {
 public:
  FunctionRegistryTest() {
    registerTestFunctions();
    exec::registerFunctionCallToSpecialForms();
  }

  void testResolveVectorFunction(
      const std::string& functionName,
      const std::vector<TypePtr>& types,
      const TypePtr& expected) {
    auto type = velox::resolveFunction(functionName, types);
    VELOX_EXPECT_EQ_TYPES(type, expected);

    type = velox::resolveVectorFunction(functionName, types);
    VELOX_EXPECT_EQ_TYPES(type, expected);

    std::vector<TypePtr> coercions;
    type = resolveFunctionWithCoercions(functionName, types, coercions);
    VELOX_EXPECT_EQ_TYPES(type, expected);

    if (expected != nullptr) {
      EXPECT_EQ(types.size(), coercions.size());
      for (const auto& coercion : coercions) {
        EXPECT_EQ(coercion, nullptr);
      }
    }
  }

  void testCoercions(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& expectedReturnType,
      const std::vector<TypePtr>& expectedCoercions) {
    auto type = resolveFunction(name, argTypes);
    ASSERT_TRUE(type == nullptr);

    std::vector<TypePtr> coercions;
    type = resolveFunctionWithCoercions(name, argTypes, coercions);

    VELOX_EXPECT_EQ_TYPES(type, expectedReturnType);

    EXPECT_EQ(coercions.size(), argTypes.size());
    EXPECT_EQ(coercions.size(), expectedCoercions.size());

    for (auto i = 0; i < coercions.size(); ++i) {
      if (expectedCoercions[i] == nullptr) {
        EXPECT_EQ(coercions[i], nullptr);
      } else {
        ASSERT_NE(coercions[i], nullptr);
        EXPECT_EQ(*coercions[i], *expectedCoercions[i])
            << "Expected: " << expectedCoercions[i]->toString()
            << ", but got: " << coercions[i]->toString();
      }
    }
  }

  void testNoCoercions(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& expectedReturnType) {
    auto type = resolveFunction(name, argTypes);
    VELOX_EXPECT_EQ_TYPES(type, expectedReturnType);

    std::vector<TypePtr> coercions;
    type = resolveFunctionWithCoercions(name, argTypes, coercions);

    VELOX_EXPECT_EQ_TYPES(type, expectedReturnType);

    EXPECT_EQ(coercions.size(), argTypes.size());
    for (const auto& coercion : coercions) {
      EXPECT_EQ(coercion, nullptr);
    }
  }

  void testCannotResolve(
      const std::string& name,
      const std::vector<TypePtr>& argTypes) {
    auto type = resolveFunction(name, argTypes);
    ASSERT_TRUE(type == nullptr);

    std::vector<TypePtr> coercions;
    type = resolveFunctionWithCoercions(name, argTypes, coercions);
    ASSERT_TRUE(type == nullptr);
  }

  exec::FunctionSignaturePtr makeSignature(
      const std::string& returnType,
      const std::vector<std::string>& argTypes) {
    exec::FunctionSignatureBuilder builder;
    builder.returnType(returnType);

    for (const auto& argType : argTypes) {
      builder.argumentType(argType);
    }

    return builder.build();
  }
};

TEST_F(FunctionRegistryTest, removeFunction) {
  const std::string functionName = "func_to_remove";
  auto checkFunctionExists = [&](const std::string& name,
                                 bool vectorFuncSignatures,
                                 bool simpleFuncSignatures) {
    EXPECT_EQ(
        getFunctionSignatures(name).size(),
        vectorFuncSignatures + simpleFuncSignatures);
    EXPECT_EQ(getVectorFunctionSignatures().count(name), vectorFuncSignatures);
    EXPECT_EQ(
        exec::simpleFunctions().getFunctionSignatures(name).size(),
        simpleFuncSignatures);
  };

  checkFunctionExists(functionName, 0, 0);

  // Only vector function registered
  registerTestVectorFunctionOne(functionName);
  checkFunctionExists(functionName, 1, 0);
  removeFunction(functionName);
  checkFunctionExists(functionName, 0, 0);

  // Only simple function registered
  registerFunction<FuncOne, Varchar, Varchar>(
      std::vector<std::string>{functionName});
  checkFunctionExists(functionName, 0, 1);
  removeFunction(functionName);
  checkFunctionExists(functionName, 0, 0);

  // Both vector and simple function registered
  registerTestVectorFunctionOne(functionName);
  registerFunction<FuncOne, Varchar, Varchar>(
      std::vector<std::string>{functionName});
  checkFunctionExists(functionName, 1, 1);
  removeFunction(functionName);
  checkFunctionExists(functionName, 0, 0);
}

TEST_F(FunctionRegistryTest, getFunctionSignaturesByName) {
  {
    auto signatures = getFunctionSignatures("func_one");
    ASSERT_EQ(signatures.size(), 1);
    ASSERT_EQ(
        signatures.at(0)->toString(),
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varchar")
            .build()
            ->toString());
  }

  {
    auto signatures = getFunctionSignatures("vector_func_one");
    ASSERT_EQ(signatures.size(), 1);
    ASSERT_EQ(
        signatures.at(0)->toString(),
        exec::FunctionSignatureBuilder()
            .returnType("bigint")
            .argumentType("varchar")
            .build()
            ->toString());
  }

  ASSERT_TRUE(getFunctionSignatures("non-existent-function").empty());
}

TEST_F(FunctionRegistryTest, getFunctionSignatures) {
  auto functionSignatures = getFunctionSignatures();
  ASSERT_EQ(functionSignatures.size(), 14);

  ASSERT_EQ(functionSignatures.count("func_one"), 1);
  ASSERT_EQ(functionSignatures.count("func_two"), 1);
  ASSERT_EQ(functionSignatures.count("func_three"), 0);
  ASSERT_EQ(functionSignatures.count("func_three_alias1"), 1);
  ASSERT_EQ(functionSignatures.count("func_three_alias2"), 1);
  ASSERT_EQ(functionSignatures.count("func_four"), 1);
  ASSERT_EQ(functionSignatures.count("func_five"), 1);
  ASSERT_EQ(functionSignatures.count("variadic_func"), 1);
  ASSERT_EQ(functionSignatures.count("vector_func_one"), 1);
  ASSERT_EQ(functionSignatures.count("vector_func_two"), 1);
  ASSERT_EQ(functionSignatures.count("vector_func_three"), 1);
  ASSERT_EQ(functionSignatures.count("vector_func_four"), 1);

  ASSERT_EQ(functionSignatures["func_one"].size(), 1);
  ASSERT_EQ(functionSignatures["func_two"].size(), 2);
  ASSERT_EQ(functionSignatures["func_three"].size(), 0);
  ASSERT_EQ(functionSignatures["func_three_alias1"].size(), 1);
  ASSERT_EQ(functionSignatures["func_three_alias2"].size(), 1);
  ASSERT_EQ(functionSignatures["vector_func_one"].size(), 1);
  ASSERT_EQ(functionSignatures["vector_func_two"].size(), 1);
  ASSERT_EQ(functionSignatures["vector_func_three"].size(), 1);
  ASSERT_EQ(functionSignatures["vector_func_four"].size(), 1);

  ASSERT_EQ(
      functionSignatures["func_one"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .build()
          ->toString());

  std::vector<std::string> funcTwoSignatures;
  std::transform(
      functionSignatures["func_two"].begin(),
      functionSignatures["func_two"].end(),
      std::back_inserter(funcTwoSignatures),
      [](auto& signature) { return signature->toString(); });
  ASSERT_THAT(
      funcTwoSignatures,
      testing::UnorderedElementsAre(
          exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("bigint")
              .argumentType("integer")
              .build()
              ->toString(),
          exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("bigint")
              .argumentType("smallint")
              .build()
              ->toString()));

  ASSERT_EQ(
      functionSignatures["func_three_alias1"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("array(bigint)")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["func_three_alias2"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("array(bigint)")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["func_four"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["func_five"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["variadic_func"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .variableArity("varchar")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["vector_func_one"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("varchar")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["vector_func_two"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("array(varchar)")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["vector_func_three"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("opaque")
          .argumentType("any")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["vector_func_four"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .knownTypeVariable("K")
          .typeVariable("V")
          .returnType("array(K)")
          .argumentType("map(K,V)")
          .build()
          ->toString());
}

TEST_F(FunctionRegistryTest, getVectorFunctionSignatures) {
  auto functionSignatures = getVectorFunctionSignatures();
  ASSERT_EQ(functionSignatures.size(), 5);

  std::set<std::string> functionNames;
  std::transform(
      functionSignatures.begin(),
      functionSignatures.end(),
      std::inserter(functionNames, functionNames.end()),
      [](auto& signature) { return signature.first; });

  ASSERT_THAT(
      functionNames,
      ::testing::UnorderedElementsAre(
          "vector_func_one",
          "vector_func_one_alias",
          "vector_func_two",
          "vector_func_three",
          "vector_func_four"));
}

TEST_F(FunctionRegistryTest, hasSimpleFunctionSignature) {
  auto result = resolveFunction("func_one", {VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());
}

TEST_F(FunctionRegistryTest, hasSimpleFunctionSignatureWrongArgType) {
  auto result = resolveFunction("func_one", {INTEGER()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasSimpleFunctionSignatureWrongFunctionName) {
  auto result = resolveFunction("method_one", {VARCHAR()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasVariadicFunctionSignature) {
  auto result = resolveFunction("variadic_func", {});
  ASSERT_EQ(*result, *VARCHAR());

  result = resolveFunction("variadic_func", {VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());

  result = resolveFunction("variadic_func", {VARCHAR(), VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());

  result = resolveFunction("variadic_func", {INTEGER()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature) {
  testResolveVectorFunction("vector_func_one", {VARCHAR()}, BIGINT());
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature2) {
  testResolveVectorFunction(
      "vector_func_two", {ARRAY(VARCHAR())}, ARRAY(BIGINT()));
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature3) {
  testResolveVectorFunction("vector_func_three", {REAL()}, OPAQUE<void>());
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature4) {
  testResolveVectorFunction(
      "vector_func_four", {MAP(BIGINT(), VARCHAR())}, ARRAY(BIGINT()));
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignatureWrongArgType) {
  testResolveVectorFunction("vector_func_one", {INTEGER()}, nullptr);
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignatureWrongFunctionName) {
  testResolveVectorFunction("vector_method_one", {VARCHAR()}, nullptr);
}

TEST_F(FunctionRegistryTest, registerFunctionTwice) {
  // For better or worse, there are code paths that depend on the ability to
  // register the same functions repeatedly and have those repeated calls
  // ignored.
  registerFunction<FuncOne, Varchar, Varchar>({"func_one"});
  registerFunction<FuncOne, Varchar, Varchar>({"func_one"});

  auto& simpleFunctions = exec::simpleFunctions();
  auto signatures = simpleFunctions.getFunctionSignatures("func_one");
  // The function should only be registered once, despite the multiple calls to
  // registerFunction.
  ASSERT_EQ(signatures.size(), 1);
}

TEST_F(FunctionRegistryTest, functionNameInMixedCase) {
  auto result = resolveFunction("funC_onE", {VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());
  result = resolveFunction("funC_onE_aliaS", {VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());

  testResolveVectorFunction("vectoR_funC_onE_aliaS", {VARCHAR()}, BIGINT());
  testResolveVectorFunction("vectoR_funC_onE", {VARCHAR()}, BIGINT());

  result = resolveFunction("variadiC_funC_aliaS", {VARCHAR(), VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());
  result = resolveFunction("variadiC_funC", {});
  ASSERT_EQ(*result, *VARCHAR());
}

TEST_F(FunctionRegistryTest, isDeterministic) {
  functions::prestosql::registerAllScalarFunctions();
  ASSERT_TRUE(isDeterministic("plus").value());
  ASSERT_TRUE(isDeterministic("in").value());

  ASSERT_FALSE(isDeterministic("rand").value());
  ASSERT_FALSE(isDeterministic("uuid").value());
  ASSERT_FALSE(isDeterministic("shuffle").value());

  // Not found functions.
  ASSERT_FALSE(isDeterministic("cast").has_value());
  ASSERT_FALSE(isDeterministic("not_found_function").has_value());
}

TEST_F(FunctionRegistryTest, companionFunction) {
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  const auto functions = {"array_frequency", "bitwise_left_shift", "ceil"};
  // Aggregate companion functions with suffix '_extract' are registered as
  // vector functions.
  const auto companionFunctions = {
      "array_agg_extract", "arbitrary_extract", "bitwise_and_agg_extract"};

  for (const auto& function : functions) {
    ASSERT_FALSE(exec::simpleFunctions()
                     .getFunctionSignaturesAndMetadata(function)
                     .front()
                     .first.companionFunction);
  }
  for (const auto& function : companionFunctions) {
    ASSERT_TRUE(exec::getVectorFunctionMetadata(function)->companionFunction);
  }
}

template <typename T>
struct TestFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<Varchar>& out,
      const arg_type<Varchar>&,
      const arg_type<Varchar>&) {
    out.copy_from("1"_sv);
  }

  void call(int32_t& out, const arg_type<Variadic<Varchar>>&) {
    out = 2;
  }

  void
  call(float& out, const arg_type<Generic<T1>>&, const arg_type<Generic<T1>>&) {
    out = 3;
  }

  void call(int64_t& out, const arg_type<Variadic<Any>>&) {
    out = 4;
  }

  void
  call(double& out, const arg_type<Varchar>&, const arg_type<Variadic<Any>>&) {
    out = 5;
  }
};

TEST_F(FunctionRegistryTest, resolveFunctionsBasedOnPriority) {
  std::string func = "func_with_priority";

  registerFunction<TestFunction, double, Varchar, Variadic<Any>>({func});
  registerFunction<TestFunction, Varchar, Varchar, Varchar>({func});
  registerFunction<TestFunction, int64_t, Variadic<Any>>({func});
  registerFunction<TestFunction, int32_t, Variadic<Varchar>>({func});
  registerFunction<TestFunction, float, Generic<T1>, Generic<T1>>({func});

  auto result1 = resolveFunction(func, {VARCHAR(), VARCHAR()});
  ASSERT_EQ(*result1, *VARCHAR());

  auto result2 = resolveFunction(func, {VARCHAR(), VARCHAR(), VARCHAR()});
  ASSERT_EQ(*result2, *INTEGER());

  auto result3 = resolveFunction(func, {VARCHAR(), INTEGER()});
  ASSERT_EQ(*result3, *DOUBLE());

  auto result4 = resolveFunction(func, {INTEGER(), VARCHAR()});
  ASSERT_EQ(*result4, *BIGINT());

  auto result5 = resolveFunction(func, {INTEGER(), INTEGER()});
  ASSERT_EQ(*result5, *REAL());
}

class DummyVectorFunction : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx& /* context */,
      velox::VectorPtr& /* result */) const override {}
};

template <typename TExec>
struct DummySimpleFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  void call(T&, const T&, const T&) {}
};

TEST_F(FunctionRegistryTest, resolveFunctionWithCoercions) {
  removeFunction("foo");

  {
    SCOPE_EXIT {
      removeFunction("foo");
    };

    registerFunction<DummySimpleFunction, int32_t, int32_t, int32_t>({"foo"});
    registerFunction<DummySimpleFunction, int64_t, int64_t, int64_t>({"foo"});
    registerFunction<DummySimpleFunction, float, float, float>({"foo"});
    registerFunction<DummySimpleFunction, double, double, double>({"foo"});

    testCoercions(
        "foo", {TINYINT(), TINYINT()}, INTEGER(), {INTEGER(), INTEGER()});

    testCoercions(
        "foo", {TINYINT(), SMALLINT()}, INTEGER(), {INTEGER(), INTEGER()});
    testCoercions(
        "foo", {SMALLINT(), TINYINT()}, INTEGER(), {INTEGER(), INTEGER()});

    testCoercions("foo", {TINYINT(), REAL()}, REAL(), {REAL(), nullptr});
    testCoercions("foo", {REAL(), TINYINT()}, REAL(), {nullptr, REAL()});

    testNoCoercions("foo", {INTEGER(), INTEGER()}, INTEGER());
    testNoCoercions("foo", {REAL(), REAL()}, REAL());
    testNoCoercions("foo", {DOUBLE(), DOUBLE()}, DOUBLE());

    testCannotResolve("foo", {TINYINT(), VARCHAR()});
  }

  {
    SCOPE_EXIT {
      removeFunction("foo");
    };

    exec::registerVectorFunction(
        "foo",
        {
            makeSignature("integer", {"integer", "integer"}),
            makeSignature("bigint", {"bigint", "bigint"}),
            makeSignature("real", {"real", "real"}),
        },
        std::make_unique<DummyVectorFunction>());

    testCoercions(
        "foo", {TINYINT(), TINYINT()}, INTEGER(), {INTEGER(), INTEGER()});

    testCoercions(
        "foo", {TINYINT(), SMALLINT()}, INTEGER(), {INTEGER(), INTEGER()});
    testCoercions(
        "foo", {SMALLINT(), TINYINT()}, INTEGER(), {INTEGER(), INTEGER()});

    testCoercions("foo", {TINYINT(), REAL()}, REAL(), {REAL(), nullptr});
    testCoercions("foo", {REAL(), TINYINT()}, REAL(), {nullptr, REAL()});

    testNoCoercions("foo", {INTEGER(), INTEGER()}, INTEGER());
    testNoCoercions("foo", {REAL(), REAL()}, REAL());

    testCannotResolve("foo", {TINYINT(), VARCHAR()});
  }

  // Coercions with complex types are not supported yet.
  {
    SCOPE_EXIT {
      removeFunction("foo");
    };

    exec::registerVectorFunction(
        "foo",
        {
            makeSignature("integer", {"array(integer)", "integer"}),
            makeSignature("bigint", {"array(bigint)", "bigint"}),
            makeSignature("real", {"array(real)", "real"}),
        },
        std::make_unique<DummyVectorFunction>());

    testCannotResolve("foo", {ARRAY(TINYINT()), SMALLINT()});
  }

  // Coercions with variable number of arguments are not supported yet.
  {
    SCOPE_EXIT {
      removeFunction("foo");
    };

    exec::registerVectorFunction(
        "foo",
        {velox::exec::FunctionSignatureBuilder()
             .returnType("bigint")
             .argumentType("bigint")
             .argumentType("bigint")
             .variableArity()
             .build(),
         velox::exec::FunctionSignatureBuilder()
             .returnType("double")
             .argumentType("double")
             .argumentType("double")
             .variableArity()
             .build()},
        std::make_unique<DummyVectorFunction>());

    testCannotResolve("foo", {TINYINT(), SMALLINT(), INTEGER()});
  }

  // Coercions with generic types are not supported yet.
  {
    SCOPE_EXIT {
      removeFunction("foo");
    };

    exec::registerVectorFunction(
        "foo",
        {velox::exec::FunctionSignatureBuilder()
             .typeVariable("T")
             .returnType("T")
             .argumentType("T")
             .argumentType("T")
             .build()},
        std::make_unique<DummyVectorFunction>());

    testCannotResolve("foo", {TINYINT(), REAL()});
  }
}

TEST_F(FunctionRegistryTest, resolveSpecialForms) {
  auto andResult =
      resolveFunctionOrCallableSpecialForm("and", {BOOLEAN(), BOOLEAN()});
  ASSERT_EQ(*andResult, *BOOLEAN());

  auto coalesceResult =
      resolveFunctionOrCallableSpecialForm("coalesce", {VARCHAR(), VARCHAR()});
  ASSERT_EQ(*coalesceResult, *VARCHAR());

  auto ifResult = resolveFunctionOrCallableSpecialForm(
      "if", {BOOLEAN(), INTEGER(), INTEGER()});
  ASSERT_EQ(*ifResult, *INTEGER());

  auto orResult =
      resolveFunctionOrCallableSpecialForm("or", {BOOLEAN(), BOOLEAN()});
  ASSERT_EQ(*orResult, *BOOLEAN());

  auto switchResult = resolveFunctionOrCallableSpecialForm(
      "switch", {BOOLEAN(), DOUBLE(), BOOLEAN(), DOUBLE(), DOUBLE()});
  ASSERT_EQ(*switchResult, *DOUBLE());

  auto tryResult = resolveFunctionOrCallableSpecialForm("try", {REAL()});
  ASSERT_EQ(*tryResult, *REAL());
}

TEST_F(FunctionRegistryTest, resolveRowConstructor) {
  auto result = resolveFunctionOrCallableSpecialForm(
      "row_constructor", {INTEGER(), BOOLEAN(), DOUBLE()});
  ASSERT_EQ(
      *result, *ROW({"c1", "c2", "c3"}, {INTEGER(), BOOLEAN(), DOUBLE()}));
}

TEST_F(FunctionRegistryTest, resolveFunctionNotSpecialForm) {
  auto result = resolveFunctionOrCallableSpecialForm("func_one", {VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());
}

TEST_F(FunctionRegistryTest, resolveCast) {
  ASSERT_THROW(
      resolveFunctionOrCallableSpecialForm("cast", {VARCHAR()}),
      velox::VeloxRuntimeError);
}

TEST_F(FunctionRegistryTest, resolveWithMetadata) {
  auto result = resolveFunctionWithMetadata("func_one", {VARCHAR()});
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(*result->first, *VARCHAR());
  EXPECT_TRUE(result->second.defaultNullBehavior);
  EXPECT_FALSE(result->second.deterministic);
  EXPECT_FALSE(result->second.supportsFlattening);

  result = resolveFunctionWithMetadata("func_two", {BIGINT(), INTEGER()});
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(*result->first, *BIGINT());
  EXPECT_FALSE(result->second.defaultNullBehavior);
  EXPECT_TRUE(result->second.deterministic);
  EXPECT_FALSE(result->second.supportsFlattening);

  result = resolveFunctionWithMetadata(
      "vector_func_four", {MAP(INTEGER(), VARCHAR())});
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(*result->first, *ARRAY(INTEGER()));
  EXPECT_TRUE(result->second.defaultNullBehavior);
  EXPECT_FALSE(result->second.deterministic);
  EXPECT_FALSE(result->second.supportsFlattening);

  result = resolveFunctionWithMetadata("non-existent-function", {VARCHAR()});
  EXPECT_FALSE(result.has_value());
}

class FunctionRegistryOverwriteTest : public functions::test::FunctionBaseTest {
 public:
  FunctionRegistryOverwriteTest() {
    registerTestFunctions();
  }
};

TEST_F(FunctionRegistryOverwriteTest, overwrite) {
  ASSERT_TRUE((registerFunction<FuncFive, int64_t, int64_t>({"foo"})));
  ASSERT_FALSE(
      (registerFunction<FuncSix, int64_t, int64_t>({"foo"}, {}, false)));
  ASSERT_TRUE((evaluateOnce<int64_t, int64_t>("foo(c0)", 0) == 5));
  ASSERT_TRUE((registerFunction<FuncSix, int64_t, int64_t>({"foo"})));
  ASSERT_TRUE((evaluateOnce<int64_t, int64_t>("foo(c0)", 0) == 6));

  auto& simpleFunctions = exec::simpleFunctions();
  auto signatures = simpleFunctions.getFunctionSignatures("foo");
  ASSERT_EQ(signatures.size(), 1);
}

TEST_F(FunctionRegistryTest, ipPrefixRegistration) {
  registerIPPrefixType();
  registerFunction<IPPrefixFunc, IPPrefix, IPPrefix>({"ipprefix_func"});

  auto& simpleFunctions = exec::simpleFunctions();
  auto signatures = simpleFunctions.getFunctionSignatures("ipprefix_func");
  ASSERT_EQ(signatures.size(), 1);

  auto result = resolveFunctionWithMetadata("ipprefix_func", {IPPREFIX()});
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(*result->first, *IPPREFIX());
  EXPECT_TRUE(result->second.defaultNullBehavior);
  EXPECT_TRUE(result->second.deterministic);
  EXPECT_FALSE(result->second.supportsFlattening);
}

} // namespace
} // namespace facebook::velox

#undef VELOX_EXPECT_EQ_TYPES
