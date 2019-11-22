#include <exception>
#include <functional>
#include <system_error>

#include <atomic>
#include <thread>
#include <chrono>
#include <deque>

#include <string>
#include <set>

using namespace std::literals::chrono_literals;

template<auto& t>
using tag_of = std::decay_t<decltype(t)>;

constexpr inline struct set_value_fn {
    template<class Receiver, class... An>
    void operator()(Receiver&& r, An&&... an) const {r.set_value((An&&)an...);}
} set_value{};
constexpr inline struct set_error_fn {
    template<class Receiver, class E>
    void operator()(Receiver&& r, E&& e) const noexcept {r.set_error((E&&)e);}
} set_error{};
constexpr inline struct set_done_fn {
    template<class Receiver>
    void operator()(Receiver&& r) const noexcept {r.set_done();}
} set_done{};

constexpr inline struct schedule_fn {
    template<class Scheduler>
    auto operator()(Scheduler s) const {return s.schedule();}
} schedule{};
constexpr inline struct now_fn {
    template<class Scheduler>
    auto operator()(Scheduler s) const {return s.now();}
} now{};
constexpr inline struct schedule_at_fn {
    template<class Scheduler, class TimePoint>
    auto operator()(Scheduler s, TimePoint when) const {return s.schedule_at(when);}
} schedule_at{};
constexpr inline struct connect_fn {
    template<class Sender, class Receiver>
    auto operator()(Sender s, Receiver&& r) const {
      return s.connect((Receiver&&)r);
    }
} connect{};
constexpr inline struct start_fn {
    template<class Operation>
    void operator()(Operation& o) const {
      o.start();
    }
} start{};

struct empty_receiver {
  template<class... An>
  void set_value(An&&...) {}
  template<class E>
  void set_error(E) noexcept {
    std::terminate();
  }
  void set_done() noexcept {}
};
struct any_void_receiver_base {
  virtual ~any_void_receiver_base() {}
  virtual void set_value() = 0;
  virtual void set_error(std::exception_ptr) noexcept = 0;
  virtual void set_done() noexcept = 0;
};
template<class Receiver>
struct any_void_receiver_impl : any_void_receiver_base {
  Receiver r_;
  explicit any_void_receiver_impl(Receiver r) : r_(std::move(r)) {}
  ~any_void_receiver_impl() {}
  void set_value() override {
    ::set_value(r_);
  }
  void set_error(std::exception_ptr ep) noexcept override {
    ::set_error(r_, ep);
  }
  void set_done() noexcept override {
    ::set_done(r_);
  }
};
struct any_void_receiver {
  std::unique_ptr<any_void_receiver_base> r_;
  void set_value() {
    ::set_value(*r_);
  }
  void set_error(std::exception_ptr ep) noexcept {
    ::set_error(*r_, ep);
  };
  void set_done() noexcept {
    ::set_done(*r_);
  };
};
template<class TimePoint>
struct what_when {
  any_void_receiver what_;
  TimePoint when_;
};
template<class TimePoint>
bool operator < (const what_when<TimePoint>& lhs, const what_when<TimePoint>& rhs) { return lhs.when_ < rhs.when_; }
struct loop_base {
  using what_when_t = what_when<std::chrono::system_clock::time_point>;
  std::deque<what_when_t> queue_;
  bool running_ = false;
  std::exception_ptr ep_;
  template<class What>
  void run(What what, std::chrono::system_clock::time_point when) {
    if (running_) {
      queue_.emplace_back(what_when_t{std::make_unique<any_void_receiver_impl<What>>(std::move(what)), when});
      return;
    } else {
      struct unwinder { 
        loop_base* that_; 
        ~unwinder() {
          that_->running_ = false;
          if (that_->ep_) {
            while (!that_->queue_.empty()) {
              set_error(that_->queue_.front().what_, that_->ep_);
              that_->queue_.pop_front();
            }
          } else {
            while (!that_->queue_.empty()) {
              set_done(that_->queue_.front().what_);
              that_->queue_.pop_front();
            }
          }
        }
      } running_guard{this};
      running_ = true;
      try {
        if (when > std::chrono::system_clock::now() && (queue_.empty() || when > queue_.front().when_)) {
          queue_.emplace_back(what_when_t{std::make_unique<any_void_receiver_impl<What>>(std::move(what)), when});
        } else {
          set_value(what);
        }
        while (!queue_.empty()) {
          std::sort(std::begin(queue_), std::end(queue_));
          auto next = std::move(queue_.front());
          queue_.pop_front();
          std::this_thread::sleep_until(next.when_);
          set_value(next.what_);
        }
      } catch(...) {
        ep_ = std::current_exception();
        throw;
      }
    }
  }
};

struct system_loop {
  loop_base loop_;
  template<class Receiver>
  struct system_loop_scheduler_at_operation {
    system_loop* loop_;
    Receiver what_;
    std::chrono::system_clock::time_point when_;
    void start() {
      loop_->loop_.queue_.emplace_back(loop_base::what_when_t{std::make_unique<any_void_receiver_impl<Receiver>>(std::move(what_)), when_});
    }
  };
  struct system_loop_scheduler_at_sender {
    system_loop* loop_;
    std::chrono::system_clock::time_point when_;
    template<class Receiver>
    auto connect(Receiver what) {
      return system_loop_scheduler_at_operation<Receiver>{loop_, what, when_};
    }
  };
  struct system_loop_scheduler {
    system_loop* loop_;
    auto now() const { return std::chrono::system_clock::now(); }
    auto schedule_at(std::chrono::system_clock::time_point when) const {
      return system_loop_scheduler_at_sender{loop_, when};
    }
    auto schedule() const {
      return schedule_at(now());
    }
  };
  system_loop_scheduler get_scheduler() { return {this}; }
  void run() {
    loop_.run(empty_receiver{}, std::chrono::system_clock::now());
  }
};

static thread_local loop_base current_thread_{};
template<class Receiver>
struct current_thread_scheduler_at_operation {
  Receiver what_;
  std::chrono::system_clock::time_point when_;
  void start() {
    current_thread_.run(std::move(what_), when_);
  }
};
struct current_thread_scheduler_at_sender {
  std::chrono::system_clock::time_point when_;
  template<class Receiver>
  auto connect(Receiver what) {
    return current_thread_scheduler_at_operation<Receiver>{what, when_};
  }
};
constexpr inline struct current_thread_scheduler_impl {
  auto now() const { return std::chrono::system_clock::now(); }
  auto schedule_at(std::chrono::system_clock::time_point when) const {
    return current_thread_scheduler_at_sender{when};
  }
  auto schedule() const {
    return schedule_at(now());
  }
} current_thread_scheduler{};

struct wait_until_receiver {
  std::atomic<bool>* signaled_;
  template<class... An>
  void set_value(An&&...){signaled_->store(true);}
  template<class E>
  void set_error(E){signaled_->store(true);}
  void set_done(){signaled_->store(true);}
};
constexpr inline struct wait_until_fn {
    template<class Sender, class Booster>
    void operator()(Sender s, Booster& b ) const {
      std::atomic<bool> signaled{false};
      auto op = connect(s, wait_until_receiver{&signaled});
      start(op);
      while (!signaled) b.run();
    }
} wait_until{};

template<class AnTuple, class Receiver>
struct justoperation {
  AnTuple anTuple_;
  Receiver r_;
  void start() {
    std::apply([&](auto... an){
      set_value(r_, an...);
    }, anTuple_); 
  }
};
template<class AnTuple>
struct justsender {
  AnTuple anTuple_;
  template<class Receiver>
  justoperation<AnTuple, Receiver> connect(Receiver r) { 
    return {anTuple_, r}; 
  }
};
constexpr inline struct just_fn {
  template<class... An>
  auto operator()(An&&... an) const {
    return justsender<std::tuple<std::decay_t<An>...>>{{(An&&)an...}};
  }
} just;

template<class E, class Receiver>
struct erroroperation {
  E e_;
  Receiver r_;
  void start() {
    set_error(r_, e_); 
  }
};
template<class E>
struct errorsender {
  E e_;
  template<class Receiver>
  erroroperation<E, Receiver> connect(Receiver r) { 
    return {e_, r}; 
  }
};
constexpr inline struct error_fn {
  template<class E>
  auto operator()(E e) const {
    return errorsender<E>{{e}};
  }
} error;

template<class Fn, class Receiver>
struct error_ifreceiver {
  Fn fn_;
  Receiver r_;
  template<class... An>
  void set_value(An&&... an){
    auto e = fn_((An&&)an...);
    if (!e) {
      ::set_value(r_, (An&&)an...);
    } else {
      ::set_error(r_, e);
    }
  }
  template<class E>
  void set_error(E e) noexcept {
    ::set_error(r_, e);
  }
  void set_done() noexcept {
    ::set_done(r_);
  }
};
template<class Sender, class Fn>
struct error_ifsender {
  Sender s_;
  Fn fn_;
  template<class Receiver>
  auto connect(Receiver r) { 
    return ::connect(s_, error_ifreceiver<Fn, Receiver>{fn_, r}); 
  }
};
constexpr inline struct error_if_fn {
  template<class Sender, class Fn>
  auto operator()(Sender s, Fn fn) const {
    return error_ifsender<Sender, Fn>{s, fn};
  }
} error_if;

template<class Fn, class Receiver>
struct transformreceiver {
  Fn fn_;
  Receiver r_;
  template<class... An>
  void set_value(An&&... an){
    if constexpr (std::is_void_v<std::invoke_result_t<Fn, An...>>) {
      fn_((An&&)an...);
      ::set_value(r_);
    } else {
      ::set_value(r_, fn_((An&&)an...));
    }
  }
  template<class E>
  void set_error(E e) noexcept {
    ::set_error(r_, e);
  }
  void set_done() noexcept {
    ::set_done(r_);
  }
};
template<class Sender, class Fn>
struct transformsender {
  Sender s_;
  Fn fn_;
  template<class Receiver>
  auto connect(Receiver r) { 
    return ::connect(s_, transformreceiver<Fn, Receiver>{fn_, r}); 
  }
};
constexpr inline struct transform_fn {
  template<class Sender, class Fn>
  auto operator()(Sender s, Fn fn) const {
    return transformsender<Sender, Fn>{s, fn};
  }
} transform;

template<class Fn, class Receiver>
struct filterreceiver {
  Fn fn_;
  Receiver r_;
  template<class... An>
  void set_value(An&&... an){
    auto b = fn_((An&&)an...);
    if (b) {
      ::set_value(r_, (An&&)an...);
    } else {
      ::set_done(r_);
    }
  }
  template<class E>
  void set_error(E e) noexcept {
    ::set_error(r_, e);
  }
  void set_done() noexcept {
    ::set_done(r_);
  }
};
template<class Sender, class Fn>
struct filtersender {
  Sender s_;
  Fn fn_;
  template<class Receiver>
  auto connect(Receiver r) { 
    return ::connect(s_, filterreceiver<Fn, Receiver>{fn_, r}); 
  }
};
constexpr inline struct filter_fn {
  template<class Sender, class Fn>
  auto operator()(Sender s, Fn fn) const {
    return filtersender<Sender, Fn>{s, fn};
  }
} filter;

template<class TapReceiver, class Receiver>
struct tapreceiver {
  TapReceiver tr_;
  Receiver r_;
  template<class... An>
  void set_value(An&&... an){
    ::set_value(tr_, (An&&)an...);
    ::set_value(r_, (An&&)an...);
  }
  template<class E>
  void set_error(E e) noexcept {
    ::set_error(tr_, e);
    ::set_error(r_, e);
  }
  void set_done() noexcept {
    ::set_done(tr_);
    ::set_done(r_);
  }
};
template<class Sender, class TapReceiver>
struct tapsender {
  Sender s_;
  TapReceiver tr_;
  template<class Receiver>
  auto connect(Receiver r) { 
    return ::connect(s_, tapreceiver<TapReceiver, Receiver>{tr_, r}); 
  }
};
constexpr inline struct tap_fn {
  template<class Sender, class TapReceiver>
  auto operator()(Sender s, TapReceiver tr) const {
    return tapsender<Sender, TapReceiver>{s, tr};
  }
} tap;

template<class Receiver>
struct take_untilreceiver {
  Receiver r_;
  bool stopped_ = false;
  template<class... An>
  void set_value(An&&... an){
    if (std::exchange(stopped_, true)) { return; }
    ::set_value(r_, (An&&)an...);
  }
  template<class E>
  void set_error(E e) noexcept {
    if (std::exchange(stopped_, true)) { return; }
    ::set_error(r_, e);
  }
  void set_done() noexcept {
    if (std::exchange(stopped_, true)) { return; }
    ::set_done(r_);
  }
};
template<class Receiver>
struct take_untiltrigger {
  Receiver r_;
  template<class... An>
  void set_value(An&&...){
    ::set_done(r_);
  }
  template<class E>
  void set_error(E) noexcept {
    ::set_done(r_);
  }
  void set_done() noexcept {
    ::set_done(r_);
  }
};
template<class Receiver>
struct receiver_ref {
  Receiver* r_;
  template<class... An>
  void set_value(An&&... an){
    ::set_value(*r_, (An&&)an...);
  }
  template<class E>
  void set_error(E e) noexcept {
    ::set_error(*r_, e);
  }
  void set_done() noexcept {
    ::set_done(*r_);
  }
};
template<class Sender, class Trigger, class Receiver>
struct take_untiloperation {
  using receiver_t = take_untilreceiver<Receiver>;
  using op_t = std::invoke_result_t<tag_of<::connect>, Sender&, receiver_ref<receiver_t>>;
  using trigger_t = std::invoke_result_t<tag_of<::connect>, Trigger&, take_untiltrigger<receiver_ref<receiver_t>>>;
  receiver_t r_;
  op_t op_;
  trigger_t t_;
  explicit take_untiloperation(Sender& s, Trigger& t, Receiver r) : 
    r_(take_untilreceiver<Receiver>{r}), 
    op_(::connect(s, receiver_ref<receiver_t>{&r_})),
    t_(::connect(t, take_untiltrigger<receiver_ref<receiver_t>>{{&r_}})) {}
  
  take_untiloperation(const take_untiloperation&) = delete;
  take_untiloperation(take_untiloperation&&) = delete;
  void start(){
    ::start(op_);
    ::start(t_);
  }
};
template<class Sender, class Trigger>
struct take_untilsender {
  Sender s_;
  Trigger t_;
  template<class Receiver>
  auto connect(Receiver r) { 
    return take_untiloperation<Sender, Trigger, Receiver>{s_, t_, r};
  }
};
constexpr inline struct take_until_fn {
  template<class Sender, class Trigger>
  auto operator()(Sender s, Trigger t) const {
    return take_untilsender<Sender, Trigger>{s, t};
  }
} take_until;

template<class Operation>
struct on_scheduler_receiver {
  Operation op_;
  void set_value() noexcept { op_.start(); }
  template<class E>
  void set_error(E) noexcept { std::terminate(); }
  void set_done() noexcept {  }
};
template<class Sender, class Scheduler>
struct onsender {
  Sender s_;
  Scheduler on_;
  template<class Receiver>
  auto connect(Receiver r) { 
    auto s = schedule(on_);
    return ::connect(
      s,
      on_scheduler_receiver<decltype(::connect(s_, r))>{::connect(s_, r)});
  }
};
constexpr inline struct on_fn {
  template<class Sender, class Scheduler>
  auto operator()(Sender s, Scheduler on) const {
    return onsender<Sender, Scheduler>{s, on};
  }
} on;

template<class Sender, class Receiver>
struct enforce_receiver;

template<class Sender, class Receiver>
struct enforcer {
  using op_t = std::invoke_result_t<tag_of<::connect>, Sender&, enforce_receiver<Sender, Receiver>>;

  op_t* current_operation_ = nullptr;
  op_t* started_operation_ = nullptr;
  std::set<op_t*> expired_operations_;
  std::set<Receiver*> expired_receivers_;
  std::set<Receiver*> completed_receivers_;

  void check_receiver(Receiver* r, const char* message) {
    auto completed_receiver = completed_receivers_.count(r) > 0;
    if (started_operation_ != nullptr && completed_receiver) {
        auto valid_receiver = expired_receivers_.count(r) == 0;
        if (valid_receiver && completed_receiver) {return;}
        printf("%s:\n\tfn - %s\n\t%s\n", typeid(*r).name(), message, 
          valid_receiver ? "valid receiver" : "invalid receiver");
    }
  }
  void check_running_receiver(Receiver* r, const char* message) {
    auto valid_receiver = expired_receivers_.count(r) == 0;
    auto completed_receiver = completed_receivers_.count(r) > 0;
    if (nullptr == started_operation_) {
      printf("%s:\n\t%s\n\t%s\n", typeid(*r).name(), message, "no operation was started");
    } else if (current_operation_ != started_operation_ || !valid_receiver || completed_receiver) {
      printf("%s:\n\t%s\n\t%s\n\t%s\n\t%s\n", typeid(*r).name(), message, 
        current_operation_ == started_operation_ ? "valid operation" : "invalid operation",
        valid_receiver ? "valid receiver" : "invalid receiver",
        completed_receiver ? "already completed" : "completing");
    }
  }

  void receiver(Receiver* r) {
    check_receiver(r, "~receiver");
    expired_receivers_.insert(r);
  }
  template<class... An>
  void set_value(Receiver* r, An&&... an) {
    check_running_receiver(r, __FUNCTION__);
    completed_receivers_.insert(r);
    ::set_value(*r, (An&&)an...);
  }
  template<class E>
  void set_error(Receiver* r, E e) noexcept {
    check_running_receiver(r, __FUNCTION__);
    completed_receivers_.insert(r);
    ::set_error(*r, e);
  }
  void set_done(Receiver* r) noexcept {
    check_running_receiver(r, __FUNCTION__);
    completed_receivers_.insert(r);
    ::set_done(*r);
  }

  void check_operation(op_t* op, const char* message) {
   if (started_operation_ != op) { return; }
    if (expired_operations_.count(op) > 0 ||
        completed_receivers_.size() != 1) 
        {
      printf("%s:\n\tfn - %s\n\t%s\n\t%s\n\t%s\n", typeid(*op).name(), message, 
        expired_operations_.count(op) > 0 ? "expired" : "valid",
        completed_receivers_.size() > 0 ? "completed" : "not completed",
        !!current_operation_ ? "running" : "not running");
    }
  }

  void operation(op_t* op) {
    check_operation(op, "~operation");
    current_operation_ = nullptr;
    expired_operations_.insert(op);
  }
  void start(op_t* op) {
    if (nullptr != started_operation_) {
      printf("%s:\n\tfn - %s\n\t%s\n", typeid(*op).name(), __FUNCTION__, "operation is already started");
    }
    current_operation_ = op;
    started_operation_ = op;
    ::start(*op);
  }
};
template<class Sender, class Receiver>
struct enforce_receiver {
  Receiver r_;
  std::shared_ptr<enforcer<Sender, Receiver>> enforcer_;
  ~enforce_receiver() {
    enforcer_->receiver(&r_);
  }
  template<class... An>
  void set_value(An&&... an) {
    enforcer_->set_value(&r_, (An&&)an...);
  }
  template<class E>
  void set_error(E e) noexcept {
    enforcer_->set_error(&r_, e);
  }
  void set_done() noexcept {
    enforcer_->set_done(&r_);
  }
};
template<class Sender, class Receiver>
struct enforce_operation {
  using op_t = std::invoke_result_t<tag_of<::connect>, Sender&, enforce_receiver<Sender, Receiver>>;
  op_t op_;
  std::shared_ptr<enforcer<Sender, Receiver>> enforcer_;
  ~enforce_operation() {
    enforcer_->operation(&op_);
  }
  void start() {
    enforcer_->start(&op_);
  }
};
template<class Sender>
struct enforce_sender {
  Sender s_;
  template<class Receiver>
  auto connect(Receiver r) {
    auto e = std::make_shared<enforcer<Sender, Receiver>>();
    return enforce_operation<Sender, Receiver>{
      ::connect(s_, enforce_receiver<Sender, Receiver>{r, e}), 
      e
    };
  }
};
template<class Sender>
enforce_sender(Sender) -> enforce_sender<Sender>;
