package clock

import (
	"time"
)

type Clock interface {

	// Step blocks until the next clock cycle arrives. This method should be
	// called in each "step" of the state machine's event loop, so that
	// in tests we have a way to control the progress of the state machine.
	Step()

	NewTimer(duration time.Duration) Timer

	NewTicker(duration time.Duration) Ticker

	Sleep(duration time.Duration)

	Stop()
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
	c := &clock{step: make(chan time.Time)}
	go func() {
		c.stepTicker = time.NewTicker(time.Millisecond)
		for t := range c.stepTicker.C {
			c.step <- t
		}
	}()
	return c
}

type clock struct {
	stepTicker *time.Ticker
	step       chan time.Time
}

func (c *clock) Step() {
	<-c.step
}

func (c *clock) Stop() {
	c.stepTicker.Stop()
}

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
