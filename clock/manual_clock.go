package clock

import "time"

type ManualClock struct {
	Timers  []*ManualTimer
	Tickers []*ManualTicker
	S       chan time.Time
}

func NewManualClock() *ManualClock {
	return &ManualClock{
		Timers:  make([]*ManualTimer, 0),
		Tickers: make([]*ManualTicker, 0),
		S:       make(chan time.Time),
	}
}

func (mc *ManualClock) NewTimer(duration time.Duration) Timer {
	t := &ManualTimer{c: make(chan time.Time)}
	mc.Timers = append(mc.Timers, t)
	return t
}

func (mc *ManualClock) NewTicker(duration time.Duration) Ticker {
	t := &ManualTicker{c: make(chan time.Time)}
	mc.Tickers = append(mc.Tickers, t)
	return t
}

func (mc *ManualClock) Sleep(duration time.Duration) {
	<-mc.S
}

type ManualTimer struct {
	c chan time.Time
}

func (mt *ManualTimer) C() <-chan time.Time {
	return mt.c
}

func (mt *ManualTimer) Stop() {
	// nop
}

func (mt *ManualTimer) T() {
	mt.c <- time.Now()
}

type ManualTicker struct {
	c chan time.Time
}

func (mt *ManualTicker) C() <-chan time.Time {
	return mt.c
}

func (mt *ManualTicker) Stop() {
	// nop
}

func (mt *ManualTicker) T() {
	mt.c <- time.Now()
}
