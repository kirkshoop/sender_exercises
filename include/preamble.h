#include <exception>
#include <functional>
#include <system_error>

constexpr inline struct set_value_fn {
    template<class Receiver, class... An>
    void operator()(Receiver& r, An&&... an) const {r.set_value((An&&)an...);}
} set_value{};
constexpr inline struct set_error_fn {
    template<class Receiver, class E>
    void operator()(Receiver& r, E&& e) const noexcept {r.set_error((E&&)e);}
} set_error{};
constexpr inline struct set_done_fn {
    template<class Receiver>
    void operator()(Receiver& r) const noexcept {r.set_done();}
} set_done{};

constexpr inline struct schedule_fn {
    template<class Scheduler>
    auto operator()(Scheduler& s) const {return s.schedule();}
} schedule{};
constexpr inline struct now_fn {
    template<class Scheduler>
    auto operator()(Scheduler& s) const {return s.now();}
} now{};
constexpr inline struct schedule_at_fn {
    template<class Scheduler, class TimePoint>
    auto operator()(Scheduler& s, TimePoint when) const {return s.schedule_at(when);}
} schedule_at{};

#include <thread>
#include <chrono>
#include <deque>

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

constexpr inline struct connect_fn {
    template<class Sender, class Receiver>
    auto operator()(Sender& s, Receiver&& r) const {
      return s.connect((Receiver&&)r);
    }
} connect{};
template<class Operation>
struct start_scheduler_receiver {
  Operation op_;
  void set_value() noexcept { op_.start(); }
  template<class E>
  void set_error(E) noexcept { std::terminate(); }
  void set_done() noexcept {  }
};
constexpr inline struct start_fn {
    template<class Operation>
    void operator()(Operation& o) const {
      auto s = schedule(current_thread_scheduler);
      auto op = connect(
        s,
        start_scheduler_receiver<Operation>{std::move(o)});
      op.start();
    }
} start{};

struct wait_until_receiver {
  std::atomic<bool>* signaled_;
  template<class... An>
  void set_value(An&&...){*signaled_ = true;}
  template<class E>
  void set_error(E){*signaled_ = true;}
  void set_done(){*signaled_ = true;}
};
constexpr inline struct wait_until_fn {
    template<class Sender, class Booster>
    void operator()(Sender& s, Booster& b ) const {
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
    ::set_value(r_, fn_((An&&)an...));
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
