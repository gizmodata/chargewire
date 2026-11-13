// A retained result's expiry against reads in progress (spec §6.5, §6.6). A Windows CI run
// polled a result every 0.5 s while its 2 s retention ran out; the sweep expired it while a
// poll was mid-stream, the stream's next Read() found the batches gone, and the server cut
// the chunked body ("incomplete chunked read") for a statement that had succeeded. A read
// that has started runs to the end; expiry applies to reads that start afterwards, and the
// batches are freed when the last read that holds them ends.
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>

#include <arrow/api.h>
#include <boost/json.hpp>

#include "statements.hpp"
#include "chargewire/errors.hpp"

namespace {

using chargewire::Error;
using chargewire::ErrorCode;
using chargewire::detail::CancelReason;
using chargewire::detail::ReadStep;
using chargewire::detail::Statement;
using chargewire::detail::WallClock;

constexpr int kBatches = 3;

class RetainedResult : public ::testing::Test {
 protected:
  void SetUp() override {
    Statement::Config config;
    config.retain = true;
    config.retention = std::chrono::seconds(2);
    config.reserve_bytes = [this](int64_t bytes) {
      reserved_ += bytes;
      return true;
    };
    statement_ = std::make_shared<Statement>("q_r", config);
    ASSERT_TRUE(statement_->BeginRunning());
    auto schema = arrow::schema({arrow::field("v", arrow::int64())});
    statement_->SetRows(schema);
    for (int i = 0; i < kBatches; ++i) {
      arrow::Int64Builder builder;
      ASSERT_TRUE(builder.AppendValues({i, i + 1, i + 2}).ok());
      std::shared_ptr<arrow::Array> values;
      ASSERT_TRUE(builder.Finish(&values).ok());
      ASSERT_TRUE(statement_->AddBatch(arrow::RecordBatch::Make(schema, 3, {values})));
    }
    statement_->Finish();
    ASSERT_TRUE(statement_->finished_at().has_value());
    ASSERT_GT(reserved_, 0);
  }

  // A moment past the result's expiry.
  WallClock::time_point AfterExpiry() const { return *statement_->finished_at() + std::chrono::seconds(3); }

  void ExpectExpired(std::size_t partition, int64_t from_batch) {
    try {
      statement_->ClaimRead(partition, from_batch);
      FAIL() << "a read that started after the expiry was allowed";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::kResultExpired);
    }
  }

  std::shared_ptr<Statement> statement_;
  int64_t reserved_ = 0;
};

// The CI failure, driven step by step: the sweep expires the result between two batches of
// a read. The read gets the rest of its batches and the end; a read that starts afterwards
// gets result_expired; the memory goes when the read ends.
TEST_F(RetainedResult, AReadInProgressWhenItExpiresRunsToTheEnd) {
  statement_->ClaimRead(0, 0);
  auto [first, batch] = statement_->Read(0, 0);
  ASSERT_EQ(first, ReadStep::kBatch);
  EXPECT_TRUE(statement_->ExpireIfDue(AfterExpiry()));
  ExpectExpired(0, 0);  // even while the earlier read is still going
  for (int64_t index = 1; index < kBatches; ++index) {
    auto [step, next] = statement_->Read(0, index);
    ASSERT_EQ(step, ReadStep::kBatch) << "batch " << index << " of a read that started before the expiry";
    EXPECT_EQ(next->num_rows(), 3);
  }
  EXPECT_EQ(statement_->Read(0, kBatches).first, ReadStep::kEnd);
  EXPECT_TRUE(statement_->HoldsRetainedResult());  // the read still holds the batches
  EXPECT_GT(reserved_, 0);
  EXPECT_FALSE(statement_->Forgettable(AfterExpiry() + std::chrono::hours(1), std::chrono::seconds(0)));
  statement_->EndRead(0);
  EXPECT_FALSE(statement_->HoldsRetainedResult());
  EXPECT_EQ(reserved_, 0) << "the expired result's bytes were not released when its last read ended";
  ExpectExpired(0, 0);
  EXPECT_TRUE(statement_->Forgettable(AfterExpiry() + std::chrono::hours(1), std::chrono::seconds(0)));
}

// With nobody reading, the expiry frees the result at once.
TEST_F(RetainedResult, AnUnreadResultIsFreedWhenItExpires) {
  EXPECT_FALSE(statement_->ExpireIfDue(*statement_->finished_at()));  // not yet
  statement_->ClaimRead(0, 1);
  statement_->EndRead(0);
  EXPECT_TRUE(statement_->ExpireIfDue(AfterExpiry()));
  EXPECT_FALSE(statement_->ExpireIfDue(AfterExpiry()));  // once
  EXPECT_FALSE(statement_->HoldsRetainedResult());
  EXPECT_EQ(reserved_, 0);
  ExpectExpired(0, 0);
  ExpectExpired(0, 2);
}

// Freeing the result on the client's request (DELETE of the statement, the session's end)
// does cut a read in progress: it ends incomplete, as §6.6 has a stream end that can't
// complete, never with a batch the result no longer has.
TEST_F(RetainedResult, AReleasedResultEndsAReadInProgress) {
  statement_->ClaimRead(0, 0);
  ASSERT_EQ(statement_->Read(0, 0).first, ReadStep::kBatch);
  statement_->Release();
  EXPECT_EQ(statement_->Read(0, 1).first, ReadStep::kFailed);
  EXPECT_EQ(reserved_, 0);
  statement_->EndRead(0);
  ExpectExpired(0, 0);
}

// Two reads of one partition in progress: the batches stay until both have ended (a
// partition's readers are counted, not flagged, or the first to end would free the batches
// under the other, which would then wait forever for batches that won't come).
TEST_F(RetainedResult, TheBatchesStayUntilTheLastReadEnds) {
  statement_->ClaimRead(0, 0);
  statement_->ClaimRead(0, 1);  // a retained partition may be read again, from any batch
  EXPECT_EQ(statement_->Describe().active_readers, 2);
  EXPECT_TRUE(statement_->ExpireIfDue(AfterExpiry()));
  statement_->EndRead(0);  // the first read ends
  EXPECT_GT(reserved_, 0);
  for (int64_t index = 1; index < kBatches; ++index) {
    ASSERT_EQ(statement_->Read(0, index).first, ReadStep::kBatch) << "batch " << index << " of the second read";
  }
  EXPECT_EQ(statement_->Read(0, kBatches).first, ReadStep::kEnd);
  statement_->EndRead(0);
  EXPECT_EQ(reserved_, 0);
  EXPECT_EQ(statement_->Describe().active_readers, 0);
}

// A statement without rows gets its result and its end in one step (spec §6.2): there is
// no moment at which its document says "running" with an update result. (Issue
// gizmodata/hardcharger#42: the two were set one after the other, and a streamed submit
// answered in between.)
TEST(UpdateResult, ComesWithTheEndNeverBefore) {
  Statement statement("q_u", Statement::Config{});
  ASSERT_TRUE(statement.BeginRunning());
  EXPECT_FALSE(statement.HasOutcome());
  auto running = statement.Document(std::nullopt);
  EXPECT_EQ(running.at("state"), boost::json::value("running"));
  EXPECT_TRUE(running.at("result").is_null());

  statement.FinishUpdate(3);
  EXPECT_TRUE(statement.HasOutcome());
  EXPECT_TRUE(statement.IsTerminal());
  EXPECT_FALSE(statement.HasRows());
  auto done = statement.Document(std::nullopt);
  EXPECT_EQ(done.at("state"), boost::json::value("succeeded"));
  EXPECT_EQ(done.at("result"), (boost::json::object{{"kind", "update"}, {"rows_affected", 3}}));
  EXPECT_TRUE(done.contains("finished_at"));
  EXPECT_FALSE(done.contains("progress"));
}

TEST(UpdateResult, WithoutACountWhenTheEngineHasNone) {
  Statement statement("q_u", Statement::Config{});
  ASSERT_TRUE(statement.BeginRunning());
  statement.FinishUpdate(std::nullopt);
  EXPECT_EQ(statement.Document(std::nullopt).at("result"), (boost::json::object{{"kind", "update"}}));
}

// A cancellation requested while the engine worked, which the engine finished anyway: the
// statement's change is made, so it succeeded (spec §6.8). Ending it "cancelled" told the
// client the change had not happened.
TEST(UpdateResult, ACancellationThatCameTooLateChangesNothing) {
  for (const CancelReason reason : {CancelReason::kClient, CancelReason::kTimeout, CancelReason::kSessionClosed}) {
    Statement statement("q_u", Statement::Config{});
    ASSERT_TRUE(statement.BeginRunning());
    EXPECT_TRUE(statement.RequestCancel(reason));  // running: the caller interrupts the engine
    statement.FinishUpdate(1);                     // the engine had done the work
    auto doc = statement.Document(std::nullopt);
    EXPECT_EQ(doc.at("state"), boost::json::value("succeeded"));
    EXPECT_EQ(doc.at("result"), (boost::json::object{{"kind", "update"}, {"rows_affected", 1}}));
    EXPECT_FALSE(doc.contains("error"));
    EXPECT_FALSE(doc.contains("cancel_reason"));
    EXPECT_FALSE(statement.error().has_value());
    EXPECT_FALSE(statement.RequestCancel(CancelReason::kClient));  // finished: a no-op
  }
}

// A statement cancelled while queued never starts, and nothing ends it twice.
TEST(UpdateResult, DoesNotReviveAStatementThatHasEnded) {
  Statement statement("q_u", Statement::Config{});
  EXPECT_FALSE(statement.RequestCancel(CancelReason::kClient));  // queued: cancelled at once
  EXPECT_FALSE(statement.BeginRunning());
  statement.FinishUpdate(1);
  auto doc = statement.Document(std::nullopt);
  EXPECT_EQ(doc.at("state"), boost::json::value("cancelled"));
  EXPECT_TRUE(doc.at("result").is_null());
}

}  // namespace
