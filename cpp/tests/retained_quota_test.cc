#include <gtest/gtest.h>

#include <arrow/api.h>

#include <atomic>
#include <barrier>
#include <limits>
#include <thread>

#include "api.hpp"

namespace warpsql::detail {
namespace {

TEST(RetainedQuota, EnforcesBothLimitsAtomicallyAndReleasesAcrossPrincipals) {
  RetainedQuota quota(10, 15);
  EXPECT_TRUE(quota.Change("alice", 10));
  EXPECT_FALSE(quota.Change("bob", 6)); // global ceiling
  EXPECT_TRUE(quota.Change("bob", 5)); // failed reservation changed nothing
  EXPECT_FALSE(quota.Change("alice", 1));
  EXPECT_TRUE(quota.Change("alice", -10));
  EXPECT_FALSE(quota.Change("bob", 6)); // per-principal ceiling
  EXPECT_TRUE(quota.Change("charlie", 10));
  EXPECT_FALSE(quota.Change("dave", 1));
  EXPECT_TRUE(quota.Change("charlie", -10));
  EXPECT_TRUE(quota.Change("dave", 10));
}

TEST(RetainedQuota, HandlesIntegerBoundariesWithoutOverflow) {
  constexpr auto maximum = std::numeric_limits<int64_t>::max();
  RetainedQuota quota(maximum, maximum);
  EXPECT_TRUE(quota.Change("a", maximum - 5));
  EXPECT_FALSE(quota.Change("b", 6));
  EXPECT_TRUE(quota.Change("b", 5));
  EXPECT_TRUE(quota.Change("a", std::numeric_limits<int64_t>::min()));
  EXPECT_TRUE(quota.Change("b", -5));
  EXPECT_TRUE(quota.Change("c", maximum));
  EXPECT_FALSE(quota.Change("d", 1));
}

TEST(RetainedQuota, ConcurrentReservationsCannotOverrunTheGlobalCeiling) {
  RetainedQuota quota(64, 8);
  std::barrier start(33);
  std::atomic<int> accepted = 0;
  std::vector<std::jthread> threads;
  for (int i = 0; i < 32; ++i) threads.emplace_back([&, i] {
    start.arrive_and_wait();
    if (quota.Change(std::to_string(i), 1)) ++accepted;
  });
  start.arrive_and_wait();
  threads.clear(); // joins every reservation
  EXPECT_EQ(accepted.load(), 8);
  EXPECT_FALSE(quota.Change("after", 1));
  for (int i = 0; i < 32; ++i) quota.Change(std::to_string(i), -1);
  EXPECT_TRUE(quota.Change("after", 8));
}

std::shared_ptr<arrow::RecordBatch> NestedBatch() {
  arrow::StringBuilder strings;
  EXPECT_TRUE(strings.Append(std::string(1000, 'x')).ok());
  auto values = strings.Finish().ValueOrDie();
  auto nested = arrow::StructArray::Make({values}, {"child"}).ValueOrDie();
  return arrow::RecordBatch::Make(arrow::schema({arrow::field("v", nested->type())}), 1, {nested});
}

TEST(RetainedQuota, NestedChildBuffersCountAgainstTheQuota) {
  RetainedQuota quota(512, 512);
  Statement::Config config;
  config.retain = true;
  config.reserve_bytes = [&](int64_t bytes) { return quota.Change("a", bytes); };
  auto batch = NestedBatch();
  Statement statement("nested", config);
  ASSERT_TRUE(statement.BeginRunning());
  statement.SetRows(batch->schema());
  EXPECT_FALSE(statement.AddBatch(batch));
  EXPECT_EQ(statement.state(), StatementState::kFailed);
  EXPECT_TRUE(quota.Change("other", 512)); // failure left no reservation
}

TEST(RetainedQuota, DictionaryValuesCountAgainstTheQuota) {
  arrow::StringBuilder strings;
  ASSERT_TRUE(strings.Append(std::string(1000, 'x')).ok());
  auto values = strings.Finish().ValueOrDie();
  arrow::Int8Builder indices;
  ASSERT_TRUE(indices.Append(0).ok());
  auto array = arrow::DictionaryArray::FromArrays(indices.Finish().ValueOrDie(), values).ValueOrDie();
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("v", array->type())}), 1, {array});
  RetainedQuota quota(512, 512);
  Statement::Config config;
  config.retain = true;
  config.reserve_bytes = [&](int64_t bytes) { return quota.Change("a", bytes); };
  Statement statement("dictionary", config);
  ASSERT_TRUE(statement.BeginRunning());
  statement.SetRows(batch->schema());
  EXPECT_FALSE(statement.AddBatch(batch));
  EXPECT_TRUE(quota.Change("other", 512));
}

TEST(RetainedQuota, ExpiryRetainsTheReservationUntilTheLastActiveReadEnds) {
  RetainedQuota quota(2048, 2048);
  Statement::Config config;
  config.retain = true;
  config.retention = std::chrono::seconds(1);
  config.reserve_bytes = [&](int64_t bytes) { return quota.Change("a", bytes); };
  auto batch = NestedBatch();
  auto statement = std::make_shared<Statement>("expiry", config);
  ASSERT_TRUE(statement->BeginRunning());
  statement->SetRows(batch->schema());
  ASSERT_TRUE(statement->AddBatch(batch));
  statement->Finish();
  statement->ClaimRead(0, 0);
  EXPECT_TRUE(statement->ExpireIfDue(*statement->finished_at() + std::chrono::seconds(2)));
  EXPECT_FALSE(quota.Change("other", 1500));
  statement->EndRead(0);
  EXPECT_TRUE(quota.Change("other", 2048));
}

TEST(RetainedQuota, CancellationAndDestructionReturnReservations) {
  RetainedQuota quota(2048, 2048);
  Statement::Config config;
  config.retain = true;
  config.reserve_bytes = [&](int64_t bytes) { return quota.Change("a", bytes); };
  auto batch = NestedBatch();
  for (bool cancel : {false, true}) {
    auto statement = std::make_shared<Statement>("released", config);
    ASSERT_TRUE(statement->BeginRunning());
    statement->SetRows(batch->schema());
    ASSERT_TRUE(statement->AddBatch(batch));
    if (cancel) {
      statement->RequestCancel(CancelReason::kClient);
      statement->Finish();
    }
    statement.reset();
    EXPECT_TRUE(quota.Change("other", 2048));
    quota.Change("other", -2048);
  }
}

}  // namespace
}  // namespace warpsql::detail
