package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type virtualClock struct{ elapsed time.Duration }

func (c *virtualClock) Now() time.Time        { return time.Unix(0, 0).Add(c.elapsed) }
func (c *virtualClock) Sleep(d time.Duration) { c.elapsed += d }

// A first retry may consume 44s of backoff, 5s of delayed polling and 1s of
// worker polling. This expected timeline comes from the pinned Asynq contract,
// independently of the fixture's chosen deadline.
func TestDeadArchivedWaitCoversLatestLegalRetry(t *testing.T) {
	clock := &virtualClock{}
	fetch := func(remaining time.Duration) (taskState, error) {
		if remaining <= 0 {
			t.Fatal("request issued after deadline")
		}
		if clock.elapsed >= 50*time.Second {
			return taskState{Status: "dead", LiveState: "archived", Attempts: 2}, nil
		}
		return taskState{Status: "failed", LiveState: "retry", Attempts: 1}, nil
	}
	if err := waitForDeadArchived(fetch, clock); err != nil {
		t.Fatalf("latest legal retry must reach dead+archived before replay: %v", err)
	}
	if clock.elapsed != 50*time.Second {
		t.Fatalf("returned before joint terminal state: %s", clock.elapsed)
	}
}

func TestDeadArchivedWaitAnchorsAfterFirstFailure(t *testing.T) {
	clock := &virtualClock{}
	fetch := func(time.Duration) (taskState, error) {
		switch {
		case clock.elapsed < 14*time.Second:
			return taskState{Status: "pending", LiveState: "pending"}, nil
		case clock.elapsed < 64*time.Second:
			return taskState{Status: "failed", LiveState: "retry", Attempts: 1}, nil
		default:
			return taskState{Status: "dead", LiveState: "archived", Attempts: 2}, nil
		}
	}
	if err := waitForDeadArchived(fetch, clock); err != nil {
		t.Fatalf("initial delivery must not consume the retry deadline: %v", err)
	}
	if clock.elapsed != 64*time.Second {
		t.Fatalf("wrong observed terminal time: %s", clock.elapsed)
	}
}

func TestDeadArchivedWaitAcceptsAlreadyFinishedTask(t *testing.T) {
	clock := &virtualClock{}
	fetch := func(time.Duration) (taskState, error) {
		return taskState{Status: "dead", LiveState: "archived", Attempts: 2}, nil
	}
	if err := waitForDeadArchived(fetch, clock); err != nil {
		t.Fatalf("a valid terminal task must not require observing an intermediate state: %v", err)
	}
	if clock.elapsed != 0 {
		t.Fatalf("already terminal task should return immediately: %s", clock.elapsed)
	}
}

func TestDeadArchivedWaitRejectsInvalidFirstObservation(t *testing.T) {
	for _, state := range []taskState{
		{Status: "dead", LiveState: "archived"},
		{Status: "dead", LiveState: "archived", Attempts: 1},
		{Status: "dead", LiveState: "archived", Attempts: 3},
		{Status: "dead", Attempts: 2},
		{LiveState: "archived", Attempts: 2},
	} {
		clock := &virtualClock{}
		fetch := func(time.Duration) (taskState, error) { return state, nil }
		if err := waitForDeadArchived(fetch, clock); err == nil {
			t.Fatalf("partial terminal state must not satisfy the initial observation: %+v", state)
		}
	}
}

func TestDeadArchivedWaitRejectsIncompleteTerminalState(t *testing.T) {
	for _, state := range []taskState{
		{Status: "dead", LiveState: "retry", Attempts: 2},
		{Status: "failed", LiveState: "archived", Attempts: 2},
		{Status: "dead", LiveState: "pending", Attempts: 2},
	} {
		t.Run(state.Status+"_"+state.LiveState, func(t *testing.T) {
			clock := &virtualClock{}
			calls := 0
			fetch := func(time.Duration) (taskState, error) {
				calls++
				if calls == 1 {
					return taskState{Status: "failed", LiveState: "retry", Attempts: 1}, nil
				}
				return state, nil
			}
			if err := waitForDeadArchived(fetch, clock); err == nil {
				t.Fatal("incomplete terminal state must block replay")
			}
			if clock.elapsed != retryExhaustionBudget {
				t.Fatalf("must stop at the wall-clock deadline, got %s", clock.elapsed)
			}
		})
	}
}

func TestWaitForStateUsesElapsedTimeAndCapsRequests(t *testing.T) {
	clock := &virtualClock{}
	var budgets []time.Duration
	fetch := func(remaining time.Duration) (taskState, error) {
		budgets = append(budgets, remaining)
		clock.Sleep(min(3*time.Second, remaining))
		return taskState{Status: "pending"}, nil
	}
	err := waitForState(fetch, func(s taskState) bool { return s.Status == "succeeded" }, 10*time.Second, clock)
	if err == nil || clock.elapsed != 10*time.Second {
		t.Fatalf("slow requests must consume the deadline: elapsed=%s err=%v", clock.elapsed, err)
	}
	if len(budgets) != 3 || budgets[0] != 10*time.Second || budgets[2] != 2*time.Second {
		t.Fatalf("remaining request budgets: %v", budgets)
	}
}

func TestWaitForStateDoesNotAcceptLateSuccess(t *testing.T) {
	clock := &virtualClock{}
	fetch := func(time.Duration) (taskState, error) {
		clock.Sleep(6 * time.Second)
		return taskState{Status: "succeeded"}, nil
	}
	if err := waitForState(fetch, func(s taskState) bool { return s.Status == "succeeded" }, 5*time.Second, clock); err == nil {
		t.Fatal("a response after the deadline is not a successful wait")
	}
}

func TestWaitForStateDoesNotAcceptTransportErrors(t *testing.T) {
	clock := &virtualClock{}
	fetch := func(time.Duration) (taskState, error) { return taskState{}, errors.New("synthetic transport failure") }
	err := waitForState(fetch, func(taskState) bool { return true }, 3*time.Second, clock)
	if err == nil || !strings.Contains(err.Error(), "synthetic transport failure") || clock.elapsed != 3*time.Second {
		t.Fatalf("transport errors must remain a bounded failure: elapsed=%s err=%v", clock.elapsed, err)
	}
}

func TestDecodeTaskStateRejectsMalformedResponses(t *testing.T) {
	for _, raw := range []string{
		`not JSON`, `{"code":0,"data":null}`, `{"data":{"status":"dead","live_state":"archived"}}`,
		`{"code":1,"data":{"status":"dead","live_state":"archived"}}`,
		`{"code":0,"data":{}}`, `{"code":0,"data":{"status":123}}`,
	} {
		if _, err := decodeTaskState([]byte(raw)); err == nil {
			t.Fatalf("malformed response accepted: %s", raw)
		}
	}
}

func TestDecodeTaskStateKeepsJointTerminalFields(t *testing.T) {
	state, err := decodeTaskState([]byte(`{"code":0,"data":{"status":"dead","live_state":"archived","attempts":2}}`))
	if err != nil || state.Status != "dead" || state.LiveState != "archived" || state.Attempts != 2 {
		t.Fatalf("valid response: %+v %v", state, err)
	}
}

// Execute the real scenario block with a failing waiter. A failed terminal
// precondition must stop the shell before either dead-list lookup or replay.
func TestScenarioStopsBeforePrematureReplay(t *testing.T) {
	script, err := os.ReadFile("../e2e.sh")
	if err != nil {
		t.Fatal(err)
	}
	_, block, ok := strings.Cut(string(script), "echo \"—— ④ 5xx → 死信 → RetryTask 重放\"")
	if !ok {
		t.Fatal("scenario 4 entry missing")
	}
	block, _, ok = strings.Cut(block, "echo \"—— ⑤ reclaim 宕机修复\"")
	if !ok {
		t.Fatal("scenario 4 end missing")
	}
	dir := t.TempDir()
	client := filepath.Join(dir, "client")
	stub := "#!/bin/sh\nprintf '%s\\n' \"$1\" >> \"$CALLS\"\ncase \"$1\" in\nsubmit) exit 0;;\nwait-dead-archived) exit 1;;\n*) exit 99;;\nesac\n"
	if err := os.WriteFile(client, []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-c", "set -euo pipefail\nok() { :; }; fail() { :; };\n"+block)
	cmd.Env = append(os.Environ(), "C="+client, "CALLS="+filepath.Join(dir, "calls"))
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("terminal wait failure must fail the scenario: %s", output)
	}
	calls, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	if string(calls) != "submit\nwait-dead-archived\n" {
		t.Fatalf("commands after failed terminal precondition: %q, output=%s", calls, output)
	}
}
