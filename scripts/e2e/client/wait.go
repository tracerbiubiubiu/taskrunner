package main

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	requestTimeout     = 5 * time.Second
	statusPollInterval = time.Second
	// Asynq v0.26.0 server.go DefaultRetryDelayFunc(0): 15 + rand.IntN(30).
	firstRetryMaximumDelay   = (15 + 30 - 1) * time.Second
	delayedTaskCheckInterval = 5 * time.Second
	workerTaskCheckInterval  = time.Second
	// 44s backoff + 5s delayed check + 1s worker poll + 5s bounded HTTP +
	// 1s observation poll = 56s, starting after the first failed attempt.
	// Keep these terms aligned with the pinned Asynq defaults on upgrades.
	retryExhaustionBudget = firstRetryMaximumDelay + delayedTaskCheckInterval + workerTaskCheckInterval + requestTimeout + statusPollInterval
)

type taskState struct {
	Status    string `json:"status"`
	LiveState string `json:"live_state"`
	Attempts  int    `json:"attempts"`
}

type stateFetch func(remaining time.Duration) (taskState, error)

// Only clock and HTTP observation are replaceable; tests advance virtual time
// without changing the production worker's random backoff or scheduling.
type waitClock interface {
	Now() time.Time
	Sleep(time.Duration)
}

type realClock struct{}

func (realClock) Now() time.Time        { return time.Now() }
func (realClock) Sleep(d time.Duration) { time.Sleep(d) }

func waitForState(fetch stateFetch, matches func(taskState) bool, timeout time.Duration, clock waitClock) error {
	deadline := clock.Now().Add(timeout)
	var last taskState
	var lastErr error
	for clock.Now().Before(deadline) {
		last, lastErr = fetch(deadline.Sub(clock.Now()))
		// A delayed response cannot turn an expired wait into a success.
		if !clock.Now().Before(deadline) {
			break
		}
		if lastErr == nil && matches(last) {
			return nil
		}
		clock.Sleep(min(statusPollInterval, deadline.Sub(clock.Now())))
	}
	return fmt.Errorf("wait deadline %s reached (last state=%+v, error=%v)", timeout, last, lastErr)
}

func waitForDeadArchived(fetch stateFetch, clock waitClock) error {
	// Start the retry budget after observing the first completed failed attempt,
	// so initial HTTP acceptance/worker pickup do not consume its backoff window.
	// A task already dead+archived is valid even if that intermediate state was
	// missed by the observer; do not introduce a new asynchronous false failure.
	alreadyTerminal := false
	if err := waitForState(fetch, func(s taskState) bool {
		alreadyTerminal = isDeadArchived(s)
		return alreadyTerminal || s.Status == "failed" && s.Attempts == 1
	}, 15*time.Second, clock); err != nil {
		return fmt.Errorf("first failed attempt not observed: %w", err)
	}
	if alreadyTerminal {
		return nil
	}
	return waitForState(fetch, isDeadArchived, retryExhaustionBudget, clock)
}

func isDeadArchived(s taskState) bool {
	return s.Status == "dead" && s.LiveState == "archived" && s.Attempts == 2
}

func decodeTaskState(body []byte) (taskState, error) {
	var response struct {
		Code *int       `json:"code"`
		Data *taskState `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return taskState{}, fmt.Errorf("invalid task JSON: %w", err)
	}
	if response.Code == nil || *response.Code != 0 || response.Data == nil || response.Data.Status == "" {
		return taskState{}, fmt.Errorf("invalid task response envelope: %s", body)
	}
	return *response.Data, nil
}
