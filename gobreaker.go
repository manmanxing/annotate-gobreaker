// Package gobreaker implements the Circuit Breaker pattern.
// See https://msdn.microsoft.com/en-us/library/dn589784.aspx.
package gobreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is a type that represents a state of CircuitBreaker.
type State int

// These constants are states of CircuitBreaker.
const (
	StateClosed   State = iota //熔断器关闭状态, 服务正常访问，默认值
	StateHalfOpen              //熔断器半开状态，部分请求，验证是否可以访问
	StateOpen                  //熔断器开启状态，禁止请求，服务异常
)

var (
	// ErrTooManyRequests 在半开启状态下，请求数大于最大请求数时的报错，在半开启状态下，防止海量请求的影响
	ErrTooManyRequests = errors.New("too many requests")
	// ErrOpenState 熔断器已经开启，阻止请求进入
	ErrOpenState = errors.New("circuit breaker is open")
)

// String implements stringer interface.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

// Counts holds the numbers of requests and their successes/failures.
// CircuitBreaker clears the internal Counts either
// on the change of the state or at the closed-state intervals.
// Counts ignores the results of the requests sent before clearing.
type Counts struct {
	Requests             uint32 // 请求数
	TotalSuccesses       uint32 // 成功
	TotalFailures        uint32 // 失败
	ConsecutiveSuccesses uint32 // 连续成功
	ConsecutiveFailures  uint32 // 连续失败
}

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

func (c *Counts) clear() {
	c.Requests = 0
	c.TotalSuccesses = 0
	c.TotalFailures = 0
	c.ConsecutiveSuccesses = 0
	c.ConsecutiveFailures = 0
}

type Settings struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts Counts) bool
	OnStateChange func(name string, from State, to State)
	IsSuccessful  func(err error) bool
}

// CircuitBreaker 结构体
type CircuitBreaker struct {
	// 熔断器的名字
	name string
	// 半开状态期最大允许放行请求：即进入Half-Open状态时，一个时间周期内允许最大同时请求数（如果还达不到切回closed状态条件，则不能再放行请求）
	// 当在最大请求数下，均请求正常的情况下，会关闭熔断器，熔断器状态为closed状态
	// 如果最大允许放行请求为0，则只允许一个请求通过
	maxRequests uint32
	// 用于在closed状态下，断路器多久清除一次Counts信息，如果设置为<=0，则在closed状态下不会清除Counts
	interval time.Duration
	// 熔断器进入Open状态后，多长时间会自动切成 Half-open，如果值<=0，则默认60s，。
	timeout time.Duration
	// ReadyToTrip回调函数：通过Counts 判断是否开启熔断，也就是熔断器进入Open状态的条件。
	// 如果返回true，则熔断器会进入Open状态，如果 readyToTrip 为nil，则会使用默认的 readyToTrip ，默认是连接5次出错，即进入Open状态
	readyToTrip func(counts Counts) bool
	// 使用请求返回的 error 作为参数，来调用 isSuccessful
	// 如果 isSuccessful 返回 false，则认为该 error 是一个失败，并计入熔断器的 counts
	// 如果 isSuccessful 返回 true，该 error 会返回给回调者，并不会计入熔断器的 counts
	// 如果 isSuccessful 为 nil，则使用默认的 isSuccessful，默认针对所有非nil的error，返回 false
	isSuccessful func(err error) bool
	// 状态变更的钩子函数(看是否需要)
	onStateChange func(name string, from State, to State)
	// 互斥锁，下面数据的更新都需要加锁
	mutex sync.Mutex
	// 记录了熔断器当前的状态
	state State
	// 标记属于哪个周期
	// generation 是一个递增值，相当于当前熔断器状态切换的次数。
	// 为了避免状态切换后，未完成请求对新状态的统计的影响，如果发现一个请求的 generation 同当前的 generation 不同，则不会进行统计计数
	generation uint64
	// 计数器，统计了 成功、失败、连续成功、连续失败等，用于决策是否进入熔断
	counts Counts
	// 进入下个周期的时间（注意是绝对时间），比如当超时后，会从open状态切换到half-open状态
	expiry time.Time
}

// TwoStepCircuitBreaker 它只检查请求是否可以继续，并期望调用者使用回调在单独的步骤中报告结果。
type TwoStepCircuitBreaker struct {
	cb *CircuitBreaker
}

// NewCircuitBreaker 通过给的配置返回一个熔断器指针
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := new(CircuitBreaker)

	cb.name = st.Name
	cb.onStateChange = st.OnStateChange

	if st.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = st.MaxRequests
	}

	if st.Interval <= 0 {
		cb.interval = defaultInterval
	} else {
		cb.interval = st.Interval
	}

	if st.Timeout <= 0 {
		cb.timeout = defaultTimeout
	} else {
		cb.timeout = st.Timeout
	}

	if st.ReadyToTrip == nil {
		cb.readyToTrip = defaultReadyToTrip
	} else {
		cb.readyToTrip = st.ReadyToTrip
	}

	if st.IsSuccessful == nil {
		cb.isSuccessful = defaultIsSuccessful
	} else {
		cb.isSuccessful = st.IsSuccessful
	}
	//设置
	cb.toNewGeneration(time.Now())

	return cb
}

// NewTwoStepCircuitBreaker returns a new TwoStepCircuitBreaker configured with the given Settings.
func NewTwoStepCircuitBreaker(st Settings) *TwoStepCircuitBreaker {
	return &TwoStepCircuitBreaker{
		cb: NewCircuitBreaker(st),
	}
}

const defaultInterval = time.Duration(0) * time.Second
const defaultTimeout = time.Duration(60) * time.Second

// 默认为连续失败次数>5
func defaultReadyToTrip(counts Counts) bool {
	return counts.ConsecutiveFailures > 5
}

//判断 err 是否为 nil
func defaultIsSuccessful(err error) bool {
	return err == nil
}

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current state of the CircuitBreaker.
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Counts returns internal counters
func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.counts
}

// Execute 主要包括三个阶段：1 请求之前的判定；2 服务的请求执行；3 请求后的状态和计数的更新
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	//请求之前的判断，返回所属的周期
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	defer func() {
		//panic 的捕获
		e := recover()
		if e != nil {
			//如果期间遇到panic，就会默认记为失败
			cb.afterRequest(generation, false)
			//还需要将 panic 传递给上层调用者
			panic(e)
		}
	}()
	//请求与执行
	result, err := req()
	//更新计数
	cb.afterRequest(generation, cb.isSuccessful(err))
	return result, err
}

// Name returns the name of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) Name() string {
	return tscb.cb.Name()
}

// State returns the current state of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) State() State {
	return tscb.cb.State()
}

// Counts returns internal counters
func (tscb *TwoStepCircuitBreaker) Counts() Counts {
	return tscb.cb.Counts()
}

// Allow checks if a new request can proceed. It returns a callback that should be used to
// register the success or failure in a separate step. If the circuit breaker doesn't allow
// requests, it returns an error.
func (tscb *TwoStepCircuitBreaker) Allow() (done func(success bool), err error) {
	generation, err := tscb.cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	return func(success bool) {
		tscb.cb.afterRequest(generation, success)
	}, nil
}

func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	//根据当前时间返回熔断器此时的状态state 与所处的周期generation
	state, generation := cb.currentState(now)
	//如果已经是开启状态了，就阻止当前请求
	if state == StateOpen {
		return generation, ErrOpenState
	} else if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		//如果是半开启状态，且请求数已经大于最大请求数
		//这里有一个限流的操作，是避免海量请求对处于恢复服务的影响
		return generation, ErrTooManyRequests
	}
	//统计计数
	cb.counts.onRequest()
	return generation, nil
}

func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	//获取当前时间的熔断器所属的状态与所处的周期
	state, generation := cb.currentState(now)
	//如果已经不是在同一个周期，直接返回
	if generation != before {
		return
	}
	//统计计数
	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		//如果熔断器是关闭的，更新成功请求次数
		cb.counts.onSuccess()
	case StateHalfOpen:
		//如果熔断器是半开启的，更新成功请求次数的同时，还需要判断熔断器是否需要向关闭状态过渡
		cb.counts.onSuccess()
		if cb.counts.ConsecutiveSuccesses >= cb.maxRequests {
			//如果连续请求成功次数已经大于最大请求次数，则说明服务趋于稳定，可以关闭熔断器
			cb.setState(StateClosed, now)
		}
	}
}

func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		//如果熔断器是关闭的，更新失败请求次数，还需要根据回调来判断是否需要开启熔断器
		cb.counts.onFailure()
		if cb.readyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		///如果熔断器是半开启的，回到开启状态
		cb.setState(StateOpen, now)
	}
}

//周期长度的设定，也是以据当前状态来的。
//如果当前正常（熔断器关闭），则设置为一个interval 的周期；如果当前熔断器是开启状态，则设置为超时时间（超时后，才能变更为半开状态）。
func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	case StateClosed://如果当前状态为关闭状态，则通过周期判断是否进入下一个周期
		//如果进入下个周期的时间已经过了，就需要进入下一个计数周期
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen://如果当前状态为已开启，则判断是否已经超时，超时就可以变更状态到半开启
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state//记录前一个状态
	cb.state = state

	cb.toNewGeneration(now)
	//记录状态的变更
	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

//通过时间，设置周期，Counts，interval，expiry
func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts.clear()

	var zero time.Time
	switch cb.state {//这里 state 默认是 0
	case StateClosed:
		if cb.interval == 0 {
			cb.expiry = zero //不会清除 Counts
		} else {
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	default: // StateHalfOpen
		cb.expiry = zero
	}
}
