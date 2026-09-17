// cbstub E2E 回调桩：按路径分流响应——/cb404 恒 404（4xx 非重试终败）、
// /cb500 前两次 500 之后 200（触发重试→死信→RetryTask 后成功）、其余 200。
// 每次调用追加一行「时间 路径 body」到 $E2E_WORK/callbacks.log 供断言。
package main

import (
	"fmt"
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
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		fmt.Fprintf(f, "%s %s %s\n", time.Now().Format("15:04:05.000"), r.URL.Path, string(buf[:n]))
		f.Sync()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
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
