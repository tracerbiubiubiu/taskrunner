package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSendHonorsDeadlineWhileReadingResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = send(req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled response body must fail at the request deadline: %v", err)
	}
}

func TestSendRejectsOversizedResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 8193)))
	}))
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	if _, _, err := send(req); err == nil {
		t.Fatal("an oversized body must not be parsed as a partial successful response")
	}
}

func TestSendReadsCompleteResponse(t *testing.T) {
	const body = `{"code":0,"data":{"status":"dead","live_state":"archived","attempts":2}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body[:20]))
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte(body[20:]))
	}))
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	status, got, err := send(req)
	if err != nil || status != http.StatusOK || string(got) != body {
		t.Fatalf("complete HTTP response required: status=%d body=%q err=%v", status, got, err)
	}
}
