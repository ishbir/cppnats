// Copyright 2016 Ishbir Singh. All rights reserved.

#ifndef CPPNATS_HPP
#define CPPNATS_HPP

#include <algorithm>
#include <cassert>
#include <chrono>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <nats.h>

namespace cppnats {

/** \brief Status represents the result of a NATS operation.
 *
 * These have been taken verbatim from the CNATS API and encapsulated in an
 * enum class.
 */
enum class Status {
  OK,
  Err,
  ProtocolError,
  IOError,
  LineTooLong,
  ConnectionClosed,
  NoServer,
  StaleConnection,
  SecureConnectionWanted,
  SecureConnectionRequired,
  ConnectionDisconnected,
  ConnectionAuthFailed,
  NotPermitted,
  NotFound,
  AddressMissing,
  InvalidSubject,
  InvalidArg,
  InvalidSubscription,
  InvalidTimeout,
  IllegalState,
  SlowConsumer,
  MaxPayload,
  MaxDeliveredMessages,
  InsufficientBuffer,
  NoMemory,
  SystemError,
  Timeout,
  FailedToInitialize,
  NotInitialized,
  SSLError
};

// Forward declarations.
class Connection;
class Subscription;

/// Encapsulates statistics relevant to a connection.
struct ConnStats {
  uint64_t in_messages;
  uint64_t in_bytes;
  uint64_t out_messages;
  uint64_t out_bytes;
  uint64_t num_reconnects;
};

/// Encapsulates statistics relevant to a subscription.
struct SubscriptionStats {
  int pending_messages;
  int pending_bytes;
  int max_pending_messages;
  int max_pending_bytes;
  int64_t delivered_messages;
  int64_t dropped_messages;
};

// Maps a natsStatus to a value in the Status enum class.
static Status convert_natsStatus(natsStatus status) {
  switch (status) {
    case NATS_OK:
      return Status::OK;

    case NATS_ERR:
      return Status::Err;

    case NATS_PROTOCOL_ERROR:
      return Status::ProtocolError;

    case NATS_IO_ERROR:
      return Status::IOError;

    case NATS_LINE_TOO_LONG:
      return Status::LineTooLong;

    case NATS_CONNECTION_CLOSED:
      return Status::ConnectionClosed;

    case NATS_NO_SERVER:
      return Status::NoServer;

    case NATS_STALE_CONNECTION:
      return Status::StaleConnection;

    case NATS_SECURE_CONNECTION_WANTED:
      return Status::SecureConnectionWanted;

    case NATS_SECURE_CONNECTION_REQUIRED:
      return Status::SecureConnectionRequired;

    case NATS_CONNECTION_DISCONNECTED:
      return Status::ConnectionDisconnected;

    case NATS_CONNECTION_AUTH_FAILED:
      return Status::ConnectionAuthFailed;

    case NATS_NOT_PERMITTED:
      return Status::NotPermitted;

    case NATS_NOT_FOUND:
      return Status::NotFound;

    case NATS_ADDRESS_MISSING:
      return Status::AddressMissing;

    case NATS_INVALID_SUBJECT:
      return Status::InvalidSubject;

    case NATS_INVALID_ARG:
      return Status::InvalidArg;

    case NATS_INVALID_SUBSCRIPTION:
      return Status::InvalidSubscription;

    case NATS_INVALID_TIMEOUT:
      return Status::InvalidTimeout;

    case NATS_ILLEGAL_STATE:
      return Status::IllegalState;

    case NATS_SLOW_CONSUMER:
      return Status::SlowConsumer;

    case NATS_MAX_PAYLOAD:
      return Status::MaxPayload;

    case NATS_MAX_DELIVERED_MSGS:
      return Status::MaxDeliveredMessages;

    case NATS_INSUFFICIENT_BUFFER:
      return Status::InsufficientBuffer;

    case NATS_NO_MEMORY:
      return Status::NoMemory;

    case NATS_SYS_ERROR:
      return Status::SystemError;

    case NATS_TIMEOUT:
      return Status::Timeout;

    case NATS_FAILED_TO_INITIALIZE:
      return Status::FailedToInitialize;

    case NATS_NOT_INITIALIZED:
      return Status::NotInitialized;

    case NATS_SSL_ERROR:
      return Status::SSLError;

    default:
      return Status::Err;
  }
}

/// Handle a natsStatus and throw an exception if not OK.
static void HANDLE_STATUS(natsStatus status) {
  if (status == NATS_OK) {
    return;
  }

  throw std::runtime_error(nats_GetLastError(nullptr));
}

/** \brief Represents the status of a Connection.
 *
 * These have been taken verbatim from the CNATS API and encapsulated in an
 * enum class.
 */
enum class ConnectionStatus {
  Disconnected,
  Connecting,
  Connected,
  Closed,
  Reconnecting
};

// Maps a natsConnStatus to a value in the ConnectionStatus enum class.
static ConnectionStatus convert_natsConnStatus(natsConnStatus status) {
  switch (status) {
    case DISCONNECTED:
      return ConnectionStatus::Disconnected;

    case CONNECTING:
      return ConnectionStatus::Connecting;

    case CONNECTED:
      return ConnectionStatus::Connected;

    case CLOSED:
      return ConnectionStatus::Closed;

    case RECONNECTING:
      return ConnectionStatus::Reconnecting;

    default:  // unknown state
      return ConnectionStatus::Closed;
  }
}

/** \brief A message sent to/received from the NATS server.
 *
 * \warning The message doesn't own the data and takes a const char* to save
 * any data copying. Thus, care must be taken that data isn't a dangling
 * pointer at any time.
 */
struct Message {
  std::string subject;
  std::string reply;
  const char* data;
  size_t data_length;

  Message(const std::string& subject, const char* data, size_t data_length)
      : subject(subject), data(data), data_length(data_length) {}

  Message(const std::string& subject, std::string& data)
      : subject(subject),
        reply(""),
        data(data.data()),
        data_length(data.length()) {}

  Message(const std::string& subject, const std::string& reply,
          const char* data, size_t data_length)
      : subject(subject), reply(reply), data(data), data_length(data_length) {}

  Message(const std::string& subject, const std::string& reply,
          std::string& data)
      : subject(subject),
        reply(reply),
        data(data.data()),
        data_length(data.length()) {}

  /** \brief Construct a Message by taking ownership of a natsMsg*.
   *
   * This is an ugly workaround but it works.
   *
   * \warning Take care that this constructor isn't called twice for the same
   * message.
   */
  Message(natsMsg* nats_msg) : nats_msg_(nats_msg) {
    assert(nats_msg != nullptr);

    subject = natsMsg_GetSubject(nats_msg);
    reply = natsMsg_GetReply(nats_msg);
    data = natsMsg_GetData(nats_msg);
    data_length = natsMsg_GetDataLength(nats_msg);
  }

  ~Message() {
    if (nats_msg_) natsMsg_Destroy(nats_msg_);
  }

  // Cannot be copied.
  void operator=(const Message&) = delete;
  Message(const Message&) = delete;

  // Move constructor.
  Message(Message&& other) noexcept : subject(other.subject),
                                      reply(other.reply),
                                      data(other.data),
                                      data_length(other.data_length),
                                      nats_msg_(other.nats_msg_) {
    other.nats_msg_ = nullptr;  // Don't want destructor to destroy this.
  }

 private:
  natsMsg* nats_msg_ = nullptr;  // Default value should be nullptr
};

/** \brief Options passed to Connection to open up a new connection.
 *
 * This class is a thin wrapper around natsOptions and internally manages a
 * pointer to it.
 */
class Options {
 public:
  /// The function that is called in case of a connection disconnect, reconnect
  /// or close.
  typedef std::function<void(Connection&)> connection_handler;

  /** \brief Construct a Options and specify a URL to connect to.
   *
   * Constructs Options and sets the URL of the `NATS Server` the client should
   * try to connect to.
   *
   * @see url
   *
   * @param connect_url url the string representing the URL the connection
   *                    should use to connect to the server.
   */
  Options(const std::string& connect_url) {
    natsOptions_Create(&opts_);
    HANDLE_STATUS(natsOptions_SetURL(opts_, connect_url.c_str()));
  }

  ~Options() {
    if (opts_) natsOptions_Destroy(opts_);
  }

  // Class cannot be copied. NATS does have a natsOptions_clone but it isn't
  // exposed so we can do nothing about it. It could theoretically still be
  // copied but we aren't going to bother with it.
  void operator=(const Options&) = delete;
  Options(const Options&) = delete;

  /// Move constructor.
  Options(Options&& other) noexcept : opts_(other.opts_),
                                      closed_cb_(other.closed_cb_),
                                      disconnected_cb_(other.disconnected_cb_),
                                      reconnected_cb_(other.reconnected_cb_) {
    other.opts_ = nullptr;
  }

  /// Return the underlying natsOptions pointer.
  natsOptions* _get_ptr() const { return opts_; }

  /** \brief Sets the URL to connect to.
   *
   * Sets the URL of the `NATS Server` the client should try to connect to.
   * The URL can contain optional user name and password.
   *
   * Some valid URLS:
   *
   * - nats://localhost:4222
   * - nats://user\@localhost:4222
   * - nats://user:password\@localhost:4222
   *
   * @param url the string representing the URL the connection should use
   *            to connect to the server.
   *
   */
  /*
   * The above is for doxygen. The proper syntax for username/password
   * is without the '\' character:
   *
   * nats://localhost:4222
   * nats://user@localhost:4222
   * nats://user:password@localhost:4222
   */
  Options url(const std::string& url) {
    HANDLE_STATUS(natsOptions_SetURL(opts_, url.c_str()));
    return std::move(*this);
  }

  /** \brief Set the list of servers to try to (re)connect to.
   *
   * This specifies a list of servers to try to connect (or reconnect) to.
   * Note that if you call #url() too, the actual list will contain
   * the one from #url() and the ones specified in this call.
   *
   * @param servers the vector of strings representing the server URLs.
   * @param randomize indicate if the servers list should be randomized. If
   *                  false, then the list of server URLs is used in the order
   *                  provided by #url() + #servers(). Otherwise, the list is
   *                  formed in a random order.
   */
  Options servers(const std::vector<std::string>& servers,
                  bool randomize = false) {
    // Convert vector<string> to vector<const char*>
    std::vector<const char*> temp(servers.size());
    std::transform(servers.begin(), servers.end(), temp.begin(),
                   [](const std::string& server) { return server.c_str(); });

    HANDLE_STATUS(natsOptions_SetServers(opts_, &temp[0], temp.size()));
    HANDLE_STATUS(natsOptions_SetNoRandomize(opts_, !randomize));
    return std::move(*this);
  }

  /** \brief Sets the (re)connect process timeout.
   *
   * This timeout is used to interrupt a (re)connect attempt to a `NATS Server`.
   * This timeout is used both for the low level TCP connect call, and for
   * timing out the response from the server to the client's initial `PING`
   * protocol.
   *
   * @param timeout the time allowed for an individual connect (or reconnect)
   *                to complete.
   *
   */
  template <class Rep, class Period>
  Options timeout(const std::chrono::duration<Rep, Period>& timeout) {
    HANDLE_STATUS(natsOptions_SetTimeout(
        opts_, std::chrono::duration_cast<std::chrono::milliseconds>(timeout)
                   .count()));
    return std::move(*this);
  }

  /** \brief Sets the name.
   *
   * This name is sent as part of the `CONNECT` protocol. There is no default
   * name.
   *
   * @param name the name to set.
   */
  Options name(const std::string& name) {
    HANDLE_STATUS(natsOptions_SetName(opts_, name.c_str()));
    return std::move(*this);
  }

  //
  // Functions dealing with security/TLS.
  //

  /** \brief Sets the secure mode.
   *
   * Indicates to the server if the client wants a secure (SSL/TLS) connection.
   * The default is `false`.
   *
   * @param secure `true` for a secure connection, `false` otherwise.
   */
  Options secure(bool secure) {
    HANDLE_STATUS(natsOptions_SetSecure(opts_, secure));
    return std::move(*this);
  }

  /** \brief Loads the trusted CA certificates from a file.
   *
   * Loads the trusted CA certificates from a file.
   *
   * Note that the certificates are added to a SSL context at the time of this
   * call, so possible errors while loading the certificates will be thrown
   * now instead of when a connection is created.
   *
   * @param filename the file containing the CA certificates.
   *
   */
  Options load_ca_trusted_certificates(const std::string& filename) {
    HANDLE_STATUS(
        natsOptions_LoadCATrustedCertificates(opts_, filename.c_str()));
    return std::move(*this);
  }

  /** \brief Loads the certificate chain from a file, using the given key.
   *
   * The certificates must be in PEM format and must be sorted starting with
   * the subject's certificate, followed by intermediate CA certificates if
   * applicable, and ending at the highest level (root) CA.
   *
   * The private key file format supported is also PEM.
   *
   * See #load_ca_trusted_certificates regarding error reports.
   *
   * @param certs_filename the file containing the client certificates.
   * @param key_filename the file containing the client private key.
   */
  Options load_certificates_chain(const std::string& certs_filename,
                                  const std::string& key_filename) {
    HANDLE_STATUS(natsOptions_LoadCertificatesChain(
        opts_, certs_filename.c_str(), key_filename.c_str()));
    return std::move(*this);
  }

  /** \brief Sets the list of available ciphers.
   *
   * Sets the list of available ciphers.
   * Check https://www.openssl.org/docs/manmaster/apps/ciphers.html for the
   * proper syntax. Here is an example:
   *
   * > "-ALL:HIGH"
   *
   * See #load_ca_trusted_certificates regarding error reports.
   *
   * @param ciphers the ciphers suite.
   */
  Options ciphers(const std::string& ciphers) {
    HANDLE_STATUS(natsOptions_SetCiphers(opts_, ciphers.c_str()));
    return std::move(*this);
  }

  /** \brief Sets the server certificate's expected hostname.
   *
   * If set, the library will check that the hostname in the server certificate
   * matches the given `hostname`. This will occur when a connection is
   * created, not at the time of this call.
   *
   * @param hostname the expected server certificate hostname.
   */
  Options expected_hostname(const std::string& hostname) {
    HANDLE_STATUS(natsOptions_SetExpectedHostname(opts_, hostname.c_str()));
    return std::move(*this);
  }

  //
  // Debug functions.
  //

  /** \brief Sets the verbose mode.
   *
   * Sets the verbose mode. If `true`, sends are echoed by the server with
   * an `OK` protocol message.
   *
   * The default is `false`.
   *
   * @param verbose `true` for a verbose protocol, `false` otherwise.
   */
  Options verbose(bool verbose) {
    HANDLE_STATUS(natsOptions_SetVerbose(opts_, verbose));
    return std::move(*this);
  }

  /** \brief Sets the pedantic mode.
   *
   * Sets the pedantic mode. If `true` some extra checks will be performed
   * by the server.
   *
   * The default is `false`
   *
   * @param pedantic `true` for a pedantic protocol, `false` otherwise.
   */
  Options pedantic(bool pedantic) {
    HANDLE_STATUS(natsOptions_SetPedantic(opts_, pedantic));
    return std::move(*this);
  }

  //
  // Connection management.
  //

  /** \brief Sets the ping interval.
   *
   * Interval, in which the client sends `PING` protocols to the `NATS Server`.
   *
   * @param interval the interval at which the connection will send `PING`
   *                 protocols to the server.
   */

  template <class Rep, class Period>
  Options ping_interval(const std::chrono::duration<Rep, Period>& interval) {
    HANDLE_STATUS(natsOptions_SetPingInterval(
        opts_, std::chrono::duration_cast<std::chrono::milliseconds>(interval)
                   .count()));
    return std::move(*this);
  }

  /** \brief Sets the limit of outstanding `PING`s without corresponding
   *         `PONG`s.
   *
   * Specifies the maximum number of `PING`s without corresponding `PONG`s
   * (which should be received from the server) before closing the connection
   * with Status::StaleConnection. If reconnection is allowed, the client
   * library will try to reconnect.
   *
   * @param num_pings_out the maximum number of `PING`s without `PONG`s
   *                      (positive number).
   */
  Options max_pings_out(int num_pings_out) {
    HANDLE_STATUS(natsOptions_SetMaxPingsOut(opts_, num_pings_out));
    return std::move(*this);
  }

  /** \brief Indicates if the connection will be allowed to reconnect.
   *
   * Specifies whether or not the client library should try to reconnect when
   * losing the connection to the `NATS Server`.
   *
   * The default is `true`.
   *
   * @param allow `true` if the connection is allowed to reconnect, `false`
   *              otherwise.
   */
  Options allow_reconnect(bool allow) {
    HANDLE_STATUS(natsOptions_SetAllowReconnect(opts_, allow));
    return std::move(*this);
  }

  /** \brief Sets the maximum number of reconnect attempts.
   *
   * Specifies the maximum number of reconnect attempts.
   *
   * @param num_reconnect the maximum number of reconnects (positive number).
   */
  Options max_reconnect(int num_reconnect) {
    HANDLE_STATUS(natsOptions_SetMaxReconnect(opts_, num_reconnect));
    return std::move(*this);
  }

  /** \brief Sets the time between reconnect attempts.
   *
   * Specifies how long to wait between two reconnect attempts from the same
   * server. This means that if you have a list with S1,S2 and are currently
   * connected to S1, and get disconnected, the library will immediately
   * attempt to connect to S2. If this fails, it will go back to S1, and this
   * time will wait for specified time interval since the last attempt to
   * connect to S1.
   *
   * @param wait the time to wait between attempts to reconnect to the same
   *             server.
   */
  template <class Rep, class Period>
  Options reconnect_wait(const std::chrono::duration<Rep, Period>& wait) {
    HANDLE_STATUS(natsOptions_SetReconnectWait(
        opts_,
        std::chrono::duration_cast<std::chrono::milliseconds>(wait).count()));
    return std::move(*this);
  }

  /** \brief Sets the size of the backing buffer used during reconnect.
   *
   * Sets the size, in bytes, of the backing buffer holding published data
   * while the library is reconnecting. Once this buffer has been exhausted,
   * publish operations will return Status::InsufficientBuffer. If not
   * specified, or the value is 0, the library will use a default value,
   * currently set to 8MB.
   *
   * @param buf_size the size, in bytes, of the backing buffer for
   *                 write operations during a reconnect.
   */
  Options reconnect_buf_size(int buf_size) {
    HANDLE_STATUS(natsOptions_SetReconnectBufSize(opts_, buf_size));
    return std::move(*this);
  }

  /** \brief Sets the maximum number of pending messages per subscription.
   *
   * Specifies the maximum number of inbound messages that can be buffered in
   * the library, for each subscription, before inbound messages are dropped
   * and Status::SlowConsumer is reported to the #error_handler callback (if
   * one has been set).
   *
   * @see error_handler()
   *
   * @param max_pending the number of messages allowed to be buffered by the
   *                    library before triggering a slow consumer scenario.
   */
  Options max_pending_messages(int max_pending) {
    HANDLE_STATUS(natsOptions_SetMaxPendingMsgs(opts_, max_pending));
    return std::move(*this);
  }

  /**
   * \bug Not implemented yet.
   */
  void error_handler(std::function<void(Connection&, Subscription&, Status)> f);

  /** \brief Sets the callback to be invoked when a connection to a server
   *         is permanently lost.
   *
   * Specifies the callback to invoke when a connection is terminally closed,
   * that is, after all reconnect attempts have failed (when reconnection is
   * allowed).
   *
   * @param f the callback to be invoked when the connection is closed.
   */
  Options closed_callback(connection_handler f) {
    closed_cb_ = f;
    return std::move(*this);
  }

  /// Return the close connection callback set using #closed_callback.
  connection_handler get_closed_callback() const { return closed_cb_; }

  /** \brief Sets the callback to be invoked when the connection to a server is
   *         lost.
   *
   * Specifies the callback to invoke when a connection to the `NATS Server`
   * is lost. There could be two instances of the callback when reconnection
   * is allowed: one before attempting the reconnect attempts, and one when
   * all reconnect attempts have failed and the connection is going to be
   * permanently closed.
   *
   * \warning Invocation of this callback is asynchronous, which means that
   * the state of the connection may have changed when this callback is
   * invoked.
   *
   * @param f the callback to be invoked when a connection to a server is lost.
   */
  Options disconnected_callback(connection_handler f) {
    disconnected_cb_ = f;
    return std::move(*this);
  }

  /// Return the disconnected connection callback set using
  /// #disconnected_callback.
  connection_handler get_disconnected_callback() const {
    return disconnected_cb_;
  }

  /** \brief Sets the callback to be invoked when the connection has
   *         reconnected.
   *
   * Specifies the callback to invoke when the client library has successfully
   * reconnected to a `NATS Server`.
   *
   * \warning Invocation of this callback is asynchronous, which means that
   * the state of the connection may have changed when this callback is
   * invoked.
   *
   * @param f the callback to be invoked when the connection to a server has
   *          been re-established.
   */
  Options reconnected_callback(connection_handler f) {
    reconnected_cb_ = f;
    return std::move(*this);
  }

  /// Return the reconnected connection callback set using
  /// #reconnected_callback.
  connection_handler get_reconnected_callback() const {
    return reconnected_cb_;
  }

 private:
  natsOptions* opts_;
  connection_handler closed_cb_;
  connection_handler disconnected_cb_;
  connection_handler reconnected_cb_;
};

/** \brief Wraps a natsConnection and provides a high-level API.
 *
 * \warning The class assumes that any callback functions specified in Options
 * have a longer lifetime than the connection itself.
 */
class Connection {
 public:
  /** \brief Connects to a `NATS Server` using the provided options.
   *
   * Attempts to connect to a `NATS Server` with multiple options.
   *
   * This call is cloning the Options object. Once this call returns, changes
   * made to the `opts` will not have an effect to this connection. The `opts`
   * can however be changed prior to be passed to another Connection
   * if desired.
   *
   * @see Options
   *
   * @param opts the options to use for this connection.
   */
  Connection(const Options& opts) {
    set_closed_cb(opts);
    set_disconnected_cb(opts);
    set_reconnected_cb(opts);

    HANDLE_STATUS(natsConnection_Connect(&conn_, opts._get_ptr()));
  }

  ~Connection() {
    if (conn_) natsConnection_Destroy(conn_);
  }

  // Class cannot be copied.
  void operator=(const Connection&) = delete;
  Connection(const Connection&) = delete;

  /// Define a move constructor.
  Connection(Connection&& other) noexcept
      : conn_(other.conn_),
        closed_handler_(other.closed_handler_),
        disconnected_handler_(other.disconnected_handler_),
        reconnected_handler_(other.reconnected_handler_) {
    other.conn_ = nullptr;
  }

  /// Return the underlying natsConnection pointer.
  natsConnection* _get_ptr() { return conn_; }

  /// Tests if connection has been closed.
  bool is_closed() const { return natsConnection_IsClosed(conn_); }

  /// Tests if connection is reconnecting.
  bool is_reconnecting() const { return natsConnection_IsReconnecting(conn_); }

  /** \brief Returns the current state of the connection.
   *
   * Returns the current state of the connection.
   *
   * @see ConnectionStatus
   */
  ConnectionStatus get_status() const {
    return convert_natsConnStatus(natsConnection_Status(conn_));
  }

  /** \brief Returns the number of bytes to be sent to the server.
   *
   * When calling any of the publish functions, data is not necessarily
   * immediately sent to the server. Some buffering occurs, allowing
   * for better performance. This function indicates if there is any
   * data not yet transmitted to the server.
   *
   * @return the number of bytes to be sent to the server, or -1 if the
   * connection is closed.
   */
  int get_buffered_bytes() const { return natsConnection_Buffered(conn_); }

  /** \brief Flushes the connection.
   *
   * Performs a round trip to the server and return when it receives the
   * internal reply.
   *
   * Note that if this call occurs when the connection to the server is
   * lost, the `PING` will not be echoed even if the library can connect
   * to a new (or the same) server. Therefore, in such situation, this
   * call will fail with Status::ConnectionDisconnected.
   *
   * If the connection is closed while this call is in progress, then
   * Status::Closed would be returned instead.
   */
  Status flush() { return convert_natsStatus(natsConnection_Flush(conn_)); }

  /** \brief Flushes the connection with a given timeout.
   *
   * Performs a round trip to the server and return when it receives the
   * internal reply, or if the call times-out.
   *
   * See possible failure case described in #flush().
   *
   * @param timeout is the time allowed for the flush to complete before
   *                #Status::Timeout error is returned.
   */
  template <class Rep, class Period>
  Status flush_timeout(const std::chrono::duration<Rep, Period>& timeout) {
    return convert_natsStatus(natsConnection_FlushTimeout(
        conn_, std::chrono::duration_cast<std::chrono::milliseconds>(timeout)
                   .count()));
  }

  /** \brief Returns the maximum message payload.
   *
   * Returns the maximum message payload accepted by the server. The
   * information is gathered from the `NATS Server` when the connection is
   * first established.
   *
   * @return the maximum message payload.
   */
  int64_t get_max_payload() const {
    return natsConnection_GetMaxPayload(conn_);
  }

  /// Gets the connection statistics.
  ConnStats get_stats() const {
    natsStatistics* nats_stats;
    natsStatistics_Create(&nats_stats);
    auto res = natsConnection_GetStats(conn_, nats_stats);
    if (res != NATS_OK) {
      natsStatistics_Destroy(nats_stats);  // Make sure we don't leak anything.
      HANDLE_STATUS(res);                  // Throw the exception.
    }

    ConnStats stats;

    // Don't need to check for error here as the function returns one only if
    // the arg is null.
    natsStatistics_GetCounts(nats_stats, &stats.in_messages, &stats.in_bytes,
                             &stats.out_messages, &stats.out_bytes,
                             &stats.num_reconnects);
    natsStatistics_Destroy(nats_stats);

    return stats;
  }

  /// Gets the URL of the currently connected server.
  std::string get_connected_url() const {
    std::array<char, 256> buf;
    HANDLE_STATUS(natsConnection_GetConnectedUrl(conn_, &buf[0], buf.size()));
    return std::string(&buf[0]);
  }

  /// Gets the server ID.
  std::string get_connected_server_id() const {
    std::array<char, 256> buf;
    HANDLE_STATUS(
        natsConnection_GetConnectedServerId(conn_, &buf[0], buf.size()));
    return std::string(&buf[0]);
  }

  /// Gets the last connection error.
  std::string get_last_error() const {
    const char* err;
    natsConnection_GetLastError(conn_, &err);
    return std::string(err);
  }

  /** \brief Closes the connection.
   *
   * Closes the connection to the server. This call will release all blocking
   * calls, such as #flush() and Subscription::next_msg().
   * The connection object is still usable until the destructor is called.
   */
  void close() { natsConnection_Close(conn_); }

  /** \brief Publishes a message on a subject.
   *
   * Publishes the Message, which includes the subject, an optional reply and
   * optional data.
   *
   * @see Message
   *
   * @param msg the message to be published.
   */
  Status publish_message(const Message& msg) {
    if (msg.subject.length() == 0) return Status::InvalidSubject;

    if (msg.reply.length() == 0) {
      return convert_natsStatus(natsConnection_Publish(
          conn_, msg.subject.c_str(), msg.data, msg.data_length));
    } else {
      return convert_natsStatus(natsConnection_PublishRequest(
          conn_, msg.subject.c_str(), msg.reply.c_str(), msg.data,
          msg.data_length));
    }
  }

  /** \brief Sends a request and waits for a reply.
   *
   * Sends the message to the specified subject and data (but ignores the reply
   * field), internally creates its own reply subject, subscribes to it, and
   * returns the first reply received.
   * This is optimized for the case of multiple responses.
   *
   * @param req the request message.
   * @param timeout before this call returns nullptr if no response is received
   *                in this alloted time.
   */
  template <class Rep, class Period>
  std::unique_ptr<Message> request(
      const Message& req, const std::chrono::duration<Rep, Period>& timeout) {
    natsMsg* nats_msg;
    natsStatus status = natsConnection_Request(
        &nats_msg, conn_, req.subject.c_str(), req.data, req.data_length,
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count());
    if (status == NATS_TIMEOUT)
      return nullptr;
    else
      HANDLE_STATUS(status);

    return std::make_unique<Message>(nats_msg);
  }

 protected:
  // The following 3 functions are here instead of Options because of the way
  // CNATS is designed. The callback function needs to receive a Connection
  // object and the neatest way of doing this is setting the 3 callbacks here.

  /// Set the closed callback handler in options.
  void set_closed_cb(const Options& opts) {
    closed_handler_ = opts.get_closed_callback();
    if (!closed_handler_) return;

    natsOptions_SetClosedCB(opts._get_ptr(), closed_handler_nats,
                            reinterpret_cast<void*>(this));
  }

  /// Set the disconnected callback handler in options.
  void set_disconnected_cb(const Options& opts) {
    disconnected_handler_ = opts.get_disconnected_callback();
    if (!disconnected_handler_) return;

    natsOptions_SetDisconnectedCB(opts._get_ptr(), disconnected_handler_nats,
                                  reinterpret_cast<void*>(this));
  }

  /// Set the reconnected callback handler in options.
  void set_reconnected_cb(const Options& opts) {
    reconnected_handler_ = opts.get_reconnected_callback();
    if (!reconnected_handler_) return;

    natsOptions_SetReconnectedCB(opts._get_ptr(), reconnected_handler_nats,
                                 reinterpret_cast<void*>(this));
  }

 private:
  natsConnection* conn_;

  // The following variables and functions are useful for connection callbacks.
  // This is because we cannot take function pointer to a lambda and thus must
  // rely on passing around the Connection instance.
  Options::connection_handler closed_handler_;
  Options::connection_handler disconnected_handler_;
  Options::connection_handler reconnected_handler_;

  static void closed_handler_nats(natsConnection*, void* data) {
    auto f = reinterpret_cast<Connection*>(data);
    f->closed_handler_(*f);
  }

  static void disconnected_handler_nats(natsConnection*, void* data) {
    auto f = reinterpret_cast<Connection*>(data);
    f->disconnected_handler_(*f);
  }

  static void reconnected_handler_nats(natsConnection*, void* data) {
    auto f = reinterpret_cast<Connection*>(data);
    f->reconnected_handler_(*f);
  }
};

/** \brief Wraps a natsSubscription and provides a high-level API.
 *
 * A subscription may be async/sync and a normal/queue subscription.
 */
class Subscription {
 public:
  /// The function that is called when a new message is received.
  typedef std::function<void(const Message&)> message_handler;

  /** \brief Creates an asynchronous subscription.
   *
   * Expresses interest in the given subject. The subject can have wildcards
   * (see CNATS documentation). Messages will be delivered to the associated
   * #message_handler.
   *
   * @param conn reference to the Connection object.
   * @param subject the subject this subscription is created for.
   * @param f the message_handler callback.
   */
  static Subscription Async(Connection& conn, const std::string& subject,
                            message_handler f) {
    return Subscription(conn, subject, "", f, false);
  };

  /** \brief Creates a synchronous subcription.
   *
   * Similar to Subscription::Async, but creates a synchronous subscription
   * that can be polled via #next_msg().
   *
   * @param conn reference to the Connection object.
   * @param subject the subject this subscription is created for.
   */
  static Subscription Sync(Connection& conn, const std::string& subject) {
    Subscription sub(conn, subject, "", nullptr, true);
    HANDLE_STATUS(natsConnection_SubscribeSync(&sub.sub_, conn._get_ptr(),
                                               subject.c_str()));
    return sub;
  }

  /** \brief Creates an asynchronous queue subscriber.
   *
   * Creates an asynchronous queue subscriber on the given subject.
   * All subscribers with the same queue name will form the queue group and
   * only one member of the group will be selected to receive any given
   * message asynchronously.
   *
   * @param conn reference to the Connection object.
   * @param subject the subject this subscription is created for.
   * @param queue_group the name of the group.
   * @param f the message_handler callback.
   */
  static Subscription AsyncQueue(Connection& conn, const std::string& subject,
                                 const std::string& queue_group,
                                 message_handler f) {
    return Subscription(conn, subject, queue_group, f, false);
  }

  /** \brief Creates a synchronous queue subscriber.
   *
   * Similar to Subscription::AsyncQueue, but creates a synchronous
   * subscription that can be polled via #next_msg().
   *
   * @param conn reference to the Connection object.
   * @param subject the subject this subscription is created for.
   * @param queue_group the name of the group.
   */
  static Subscription SyncQueue(Connection& conn, const std::string& subject,
                                const std::string& queue_group) {
    Subscription sub(conn, subject, queue_group, nullptr, true);
    HANDLE_STATUS(natsConnection_QueueSubscribeSync(
        &sub.sub_, conn._get_ptr(), subject.c_str(), queue_group.c_str()));
    return sub;
  }

  /** \brief Destroys the subscription.
   *
   * Destroys the subscription object, freeing up memory.
   * If not already done, this call will removes interest on the subject.
   */
  ~Subscription() {
    if (sub_) natsSubscription_Destroy(sub_);
  }

  // Class cannot be copied.
  void operator=(const Subscription&) = delete;
  Subscription(const Subscription&) = delete;

  /// Define a move constructor so that the factory methods work.
  Subscription(Subscription&& other) noexcept
      : Subscription(other.conn_, other.subject_, other.queue_group_,
                     other.msg_handler_, other.sync_) {
    other.sub_ = nullptr;
  }

  /** \brief Start the async subscription.
   *
   * Starts an async subscription with the server.
   * This method must not be called on synchronous subscriptions or an exception
   * will be thrown.
   */
  Status start() {
    if (sync_)
      throw std::logic_error(
          "cannot start a sync subscription; use next_msg to poll for new "
          "messages");

    if (queue_)
      return convert_natsStatus(natsConnection_QueueSubscribe(
          &sub_, conn_._get_ptr(), subject_.c_str(), queue_group_.c_str(),
          message_handler_func, reinterpret_cast<Subscription*>(this)));

    return convert_natsStatus(natsConnection_Subscribe(
        &sub_, conn_._get_ptr(), subject_.c_str(), message_handler_func,
        reinterpret_cast<void*>(this)));
  }

  /** \brief Enables the No Delivery Delay mode.
   *
   * By default, messages that arrive are not immediately delivered. This
   * generally improves performance. However, in case of request-reply,
   * this delay has a negative impact. In such case, call this function
   * to have the subscriber be notified immediately each time a message
   * arrives.
   */
  void set_no_delivery_delay() {
    HANDLE_STATUS(natsSubscription_NoDeliveryDelay(sub_));
  }

  /** \brief Returns the next available message.
   *
   * Return the next message available to a synchronous subscriber or block
   * until one is available.
   * A timeout can be used to return when no message has been delivered. If the
   * value is zero or less than 1 ms, then this call will not wait and return
   * the next message that was pending in the client, and nullptr otherwise.
   *
   * @param timeout time after which this call will return nullptr if no message
   *                is available.
   */
  template <class Rep, class Period>
  std::unique_ptr<Message> next_msg(
      const std::chrono::duration<Rep, Period>& timeout) {
    if (!sync_)
      throw std::logic_error("sychronous method called on async subscription");

    natsMsg* nats_msg;
    natsStatus status = natsSubscription_NextMsg(
        &nats_msg, sub_,
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count());
    if (status == NATS_TIMEOUT)
      return nullptr;
    else
      HANDLE_STATUS(status);

    return std::make_unique<Message>(nats_msg);
  }

  /** \brief Unsubscribes.
   *
   * Removes interest on the subject. Asynchronous subscription may still have
   * a callback in progress, in that case, the subscription will still be valid
   * until the callback returns.
   */
  void unsubscribe() { HANDLE_STATUS(natsSubscription_Unsubscribe(sub_)); }

  /** \brief Auto-Unsubscribes.
   *
   * This call issues an automatic unsubscribe() that is processed by the server
   * when 'max' messages have been received. This can be useful when sending a
   * request to an unknown number of subscribers.
   *
   * @param max the maximum number of message you want this subscription
   * to receive.
   */
  void auto_unsubscribe(int max) {
    HANDLE_STATUS(natsSubscription_AutoUnsubscribe(sub_, max));
  }

  /** \brief Sets the limit for pending messages and bytes.
   *
   * Specifies the maximum number and size of incoming messages that can be
   * buffered in the library for this subscription, before new incoming messages
   * are dropped and Status::NatsSlowConsumer is reported to the
   * Options::error_handler callback (if one has been set).
   *
   * If no limit is set at the subscription level, the limit set by
   * Options::max_pending_messages before creating the connection will be used.
   *
   * \note If no option is set, there is still a default of `65536` messages and
   * `65536 * 1024` bytes.
   *
   * @see Options::max_pending_messages
   * @see Subscription::get_stats
   *
   * @param msg_limit the limit in number of messages that the subscription can
   *                  hold.
   * @param bytes_limit the limit in bytes that the subscription can hold.
   */
  void set_pending_limits(int msg_limit, int bytes_limit) {
    HANDLE_STATUS(
        natsSubscription_SetPendingLimits(sub_, msg_limit, bytes_limit));
  }

  /// Get various statistics from this subscription.
  SubscriptionStats get_stats() {
    SubscriptionStats stats;
    HANDLE_STATUS(natsSubscription_GetStats(
        sub_, &stats.pending_messages, &stats.pending_bytes,
        &stats.max_pending_messages, &stats.max_pending_bytes,
        &stats.delivered_messages, &stats.dropped_messages));
    return stats;
  }

  /** \brief Checks the validity of the subscription.
   *
   * Returns a boolean indicating whether the subscription is still active.
   * This will return false if the subscription has already been closed,
   * or auto unsubscribed.
   */
  bool is_valid() const { return natsSubscription_IsValid(sub_); }

 private:
  // Private constructor.
  Subscription(Connection& conn, const std::string& subject,
               const std::string& queue_group, message_handler f, bool sync)
      : conn_(conn),
        subject_(subject),
        queue_group_(queue_group),
        msg_handler_(f),
        sync_(sync) {
    if (queue_group.length() != 0)
      queue_ = true;
    else
      queue_ = false;
  }

  // Set at the time of construction.
  Connection& conn_;
  std::string subject_;
  std::string queue_group_;
  message_handler msg_handler_;  // For async callbacks.
  bool sync_;
  bool queue_;

  // Maybe set at the time of construction or with subscribe() in case of async
  // subscriptions.
  natsSubscription* sub_;

  // Message handler for passing to cnats. The data passed is interpreted as
  // Subscription*.
  static void message_handler_func(natsConnection*, natsSubscription*,
                                   natsMsg* nats_msg, void* data) {
    auto f = reinterpret_cast<Subscription*>(data);
    Message msg(nats_msg);
    f->msg_handler_(msg);
  }
};

/// Clean up any global data allocated by cnats (for a clean Valgrind report).
void ShutdownLibrary() { nats_Close(); }

}  // namespace cppnats

#endif
