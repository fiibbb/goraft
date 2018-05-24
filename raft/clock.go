package raft

import (
	"time"
)

type clock interface {

	// Step blocks until the next realClock cycle arrives. This method should be
	// called in each "step" of the state machine's event loop, so that
	// in tests we have a way to control the progress of the state machine.
	Step()

	NewTimer(duration time.Duration) timer

	NewTicker(duration time.Duration) ticker

	Sleep(duration time.Duration)

	Stop()
}

type timer interface {
	C() <-chan time.Time
	Stop()
}

type ticker interface {
	C() <-chan time.Time
	Stop()
}

func newClock() *realClock {
	c := &realClock{step: make(chan time.Time)}
	go func() {
		c.stepTicker = time.NewTicker(time.Millisecond)
		for t := range c.stepTicker.C {
			c.step <- t
		}
	}()
	return c
}

type realClock struct {
	stepTicker *time.Ticker
	step       chan time.Time
}

func (c *realClock) Step() {
	<-c.step
}

func (c *realClock) Stop() {
	c.stepTicker.Stop()
}

func (c *realClock) NewTimer(duration time.Duration) timer {
	return &realTimer{timer: time.NewTimer(duration)}
}

func (c *realClock) NewTicker(duration time.Duration) ticker {
	return &realTicker{ticker: time.NewTicker(duration)}
}

func (c *realClock) Sleep(duration time.Duration) {
	time.Sleep(duration)
}

type realTimer struct {
	timer *time.Timer
}

func (t *realTimer) C() <-chan time.Time {
	return t.timer.C
}

func (t *realTimer) Stop() {
	if !t.timer.Stop() {
		<-t.timer.C
	}
}

type realTicker struct {
	ticker *time.Ticker
}

func (t *realTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *realTicker) Stop() {
	t.ticker.Stop()
}
