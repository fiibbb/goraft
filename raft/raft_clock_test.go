package raft

import (
	"time"
)

/////////////////////////////////////////////////////////////////////////////////
//////////////////////// Manual realClock Implementation ////////////////////////////
/////////////////////////////////////////////////////////////////////////////////

type manualClock struct {
	timers    []*manualTimer
	tickers   []*manualTicker
	sleepChan chan time.Time
	stepChan  chan time.Time
}

func newManualClock() *manualClock {
	return &manualClock{
		timers:    make([]*manualTimer, 0),
		tickers:   make([]*manualTicker, 0),
		sleepChan: make(chan time.Time),
		stepChan:  make(chan time.Time),
	}
}

func (mc *manualClock) Step() {
	<-mc.stepChan
}

func (mc *manualClock) NewTimer(duration time.Duration) timer {
	t := &manualTimer{c: make(chan time.Time)}
	mc.timers = append(mc.timers, t)
	return t
}

func (mc *manualClock) NewTicker(duration time.Duration) ticker {
	t := &manualTicker{c: make(chan time.Time)}
	mc.tickers = append(mc.tickers, t)
	return t
}

func (mc *manualClock) Sleep(duration time.Duration) {
	<-mc.sleepChan
}

func (mc *manualClock) Stop() {
	// nop
}

type manualTimer struct {
	c chan time.Time
}

func (mt *manualTimer) C() <-chan time.Time {
	return mt.c
}

func (mt *manualTimer) Stop() {
	// nop
}

func (mt *manualTimer) T() {
	mt.c <- time.Now()
}

type manualTicker struct {
	c chan time.Time
}

func (mt *manualTicker) C() <-chan time.Time {
	return mt.c
}

func (mt *manualTicker) Stop() {
	// nop
}

func (mt *manualTicker) T() {
	mt.c <- time.Now()
}
