// cbstub E2E回调桩：按签名params.e2e_case选择合成响应，W0b后目标URL由服务端固定。
// /cb404恒404、/cb500前两次500之后200，其余200。
// 每次调用追加一行「时间 路径 body」到 $E2E_WORK/callbacks.log 供断言。
package main

import (
	"encoding/json"
	"fmt"
	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

func main() {
	work := os.Getenv("E2E_WORK")
	if work == "" {
		work = "/tmp/taskrunner-e2e"
	}
	f, err := os.OpenFile(work+"/callbacks.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	var c500 int64
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "fixture body read failed", 400)
			return
		}
		if err := (&aksk.Verifier{Keys: map[string][]byte{"taskrunner": []byte("sk-e2e-self")}}).Verify(r, body); err != nil {
			http.Error(w, "fixture signature rejected", 401)
			return
		}
		var in struct {
			Params struct {
				Case string `json:"e2e_case"`
			} `json:"params"`
		}
		if err := json.Unmarshal(body, &in); err != nil {
			http.Error(w, "fixture JSON rejected", 400)
			return
		}
		fmt.Fprintf(f, "%s %s %s\n", time.Now().Format("15:04:05.000"), r.URL.Path, string(body))
		f.Sync()
		w.Header().Set("Content-Type", "application/json")
		switch in.Params.Case {
		case "/cb404":
			w.WriteHeader(http.StatusNotFound)
		case "/cb500":
			if atomic.AddInt64(&c500, 1) <= 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	})
	if err := http.ListenAndServe("127.0.0.1:6398", nil); err != nil {
		panic(err)
	}
}
