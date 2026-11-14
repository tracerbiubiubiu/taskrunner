// client E2E 签名客户端：以 AK/SK（与 e2e.sh 注入 serve 的密钥一致）对本地 API 签名。
// 用法：client <submit <id> <syntheticCase> | get <id> | cancel <id> | retry <id> | dead>
// 输出「HTTP状态码 + 响应体」单行；e2e.sh 以 sed 剥离状态码后交给 python3 断言。
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"
)

const base = "http://127.0.0.1:6397"

var fixtureHTTPClient = &http.Client{Timeout: requestTimeout}

func signedRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	aksk.Sign(req, body, aksk.SignOptions{AK: "zhuzhao", SK: []byte("sk-e2e-zhuzhao"), Operator: "e2e"})
	return req, nil
}

func send(req *http.Request) (int, []byte, error) {
	resp, err := fixtureHTTPClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8193))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if len(body) > 8192 {
		return resp.StatusCode, nil, fmt.Errorf("fixture HTTP response exceeds 8192 bytes")
	}
	return resp.StatusCode, body, nil
}

func do(method, path string, body []byte) {
	req, err := signedRequest(context.Background(), method, path, body)
	if err != nil {
		panic(err)
	}
	status, response, err := send(req)
	if err != nil {
		fmt.Println("0 ERR", err)
		os.Exit(1)
	}
	fmt.Printf("%d %s\n", status, response)
}

func fetchTask(taskID string) stateFetch {
	return func(remaining time.Duration) (taskState, error) {
		ctx, cancel := context.WithTimeout(context.Background(), min(requestTimeout, remaining))
		defer cancel()
		req, err := signedRequest(ctx, http.MethodGet, "/v1/tasks/"+taskID, nil)
		if err != nil {
			return taskState{}, err
		}
		status, body, err := send(req)
		if err != nil {
			return taskState{}, err
		}
		if status != http.StatusOK {
			return taskState{}, fmt.Errorf("get task returned HTTP %d: %s", status, body)
		}
		return decodeTaskState(body)
	}
}

func waitCommand() {
	var err error
	switch os.Args[1] {
	case "wait":
		if len(os.Args) != 5 {
			fmt.Fprintln(os.Stderr, "usage: client wait <id> <status> <seconds>")
			os.Exit(2)
		}
		seconds, parseErr := strconv.Atoi(os.Args[4])
		if parseErr != nil || seconds <= 0 {
			fmt.Fprintln(os.Stderr, "wait seconds must be a positive integer")
			os.Exit(2)
		}
		err = waitForState(fetchTask(os.Args[2]), func(s taskState) bool {
			return s.Status == os.Args[3]
		}, time.Duration(seconds)*time.Second, realClock{})
	case "wait-dead-archived":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: client wait-dead-archived <id>")
			os.Exit(2)
		}
		err = waitForDeadArchived(fetchTask(os.Args[2]), realClock{})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		os.Exit(2)
	}
	switch os.Args[1] {
	case "wait", "wait-dead-archived":
		waitCommand()
	case "submit": // 服务端定callback目标；第三参数仅选择合成桩响应，放签名params。
		fixtureCase := ""
		if len(os.Args) > 3 {
			fixtureCase = os.Args[3]
		}
		do("POST", "/v1/tasks", fmt.Appendf(nil,
			`{"task_id":%q,"action":"audit_archive","params":{"e2e_case":%q},"dept":"e2e"}`,
			os.Args[2], fixtureCase))
	case "cancel": // cancel <id>
		do("POST", "/v1/tasks/cancel", fmt.Appendf(nil, `{"task_id":%q}`, os.Args[2]))
	case "retry": // retry <id>
		do("POST", "/v1/tasks/retry", fmt.Appendf(nil, `{"task_id":%q}`, os.Args[2]))
	case "get": // get <id>
		do("GET", "/v1/tasks/"+os.Args[2], nil)
	case "dead":
		do("GET", "/v1/dead-letters?page=1&page_size=50", nil)
	case "createjob":
		do("POST", "/v1/jobs", []byte(`{"action_id":"audit_archive","trigger_type":"cron","cron_spec":"* * * * *","params":{},"dept":"e2e","timeout_secs":30}`))
	default:
		os.Exit(2)
	}
}
