// Copyright 2016 Ishbir Singh. All rights reserved.

#define DOCTEST_CONFIG_IMPLEMENT
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <cppnats.hpp>
#include "doctest.h"

TEST_CASE("Test if connection throws an exception on invalid options") {
  auto opts =
      cppnats::Options("nats://localhost:2321")
          .name("dummyclient")
          .servers({"nats://123.123.123.123:2121", "nats://24.13.56.12:2312",
                    "nats://124.45.78.55:4432"})
          .timeout(std::chrono::milliseconds(10));

  REQUIRE_THROWS(new cppnats::Connection(opts));
}

TEST_CASE("Test if connection succeeds") {
  auto opts =
      cppnats::Options(NATS_DEFAULT_URL).timeout(std::chrono::milliseconds(10));

  // Connect to the NATS server.
  cppnats::Connection conn(opts);

  std::string subject = "test-channel";
  // Needs to be like this because Message DOES NOT own data.
  std::string data = "some test data to be sent over the wire";
  cppnats::Message msg(subject, data);

  SUBCASE("Test if synchronous subscriptions work") {
    // Create a subscription first so that the message sent isn't lost.
    auto sub = cppnats::Subscription::Sync(conn, subject);

    // Send the message.
    auto status = conn.publish_message(msg);
    REQUIRE(status == cppnats::Status::OK);

    // Synchronously receive the message.
    auto msg_recvd = sub.next_msg(std::chrono::milliseconds(100));

    // Check that we got it.
    REQUIRE(static_cast<bool>(msg_recvd));
    CHECK(msg_recvd->subject == msg.subject);
    CHECK(msg_recvd->data_length == msg.data_length);
    CHECK(std::string(msg_recvd->data) == data);
  }

  SUBCASE("Test if asynchronous subscriptions work") {
    std::mutex m;
    std::condition_variable cv;

    auto sub = cppnats::Subscription::Async(
        conn, subject,
        [&m, &cv, &msg, &data](const cppnats::Message& msg_recvd) {
          std::lock_guard<std::mutex> lk(m);

          CHECK(msg_recvd.subject == msg.subject);
          CHECK(msg_recvd.data_length == msg.data_length);
          CHECK(std::string(msg_recvd.data) == data);

          cv.notify_all();
        });
    sub.auto_unsubscribe(1);

    // Send the message.
    auto status = conn.publish_message(msg);
    REQUIRE(status == cppnats::Status::OK);

    // Wait for the message for 100ms.
    std::unique_lock<std::mutex> lk(m);
    if (cv.wait_for(lk, std::chrono::milliseconds(100)) ==
        std::cv_status::timeout) {
      REQUIRE(false);
    }
  }
}

int main(int argc, char** argv) {
  doctest::Context context(argc, argv);  // initialize
  int res = context.run();               // run

  cppnats::ShutdownLibrary();  // Clean up memory

  return res;
}
