package clock

import (
	"time"
)

type Clock interface {
	NewTimer(duration time.Duration) Timer
	NewTicker(duration time.Duration) Ticker
	Sleep(duration time.Duration)
}

type Timer interface {
	C() <-chan time.Time
	Stop()
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

func NewClock() Clock {
	return &clock{}
}

type clock struct{}

func (c *clock) NewTimer(duration time.Duration) Timer {
	return &timer{timer: time.NewTimer(duration)}
}

func (c *clock) NewTicker(duration time.Duration) Ticker {
	return &ticker{ticker: time.NewTicker(duration)}
}

func (c *clock) Sleep(duration time.Duration) {
	time.Sleep(duration)
}

type timer struct {
	timer *time.Timer
}

func (t *timer) C() <-chan time.Time {
	return t.timer.C
}

func (t *timer) Stop() {
	if !t.timer.Stop() {
		<-t.timer.C
	}
}

type ticker struct {
	ticker *time.Ticker
}

func (t *ticker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *ticker) Stop() {
	t.ticker.Stop()
}
