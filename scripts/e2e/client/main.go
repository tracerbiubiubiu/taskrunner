// client E2E 签名客户端：以 AK/SK（与 e2e.sh 注入 serve 的密钥一致）对本地 API 签名。
// 用法：client <submit <id> <cbPath> | get <id> | cancel <id> | retry <id> | dead>
// 输出「HTTP状态码 + 响应体」单行；e2e.sh 以 sed 剥离状态码后交给 python3 断言。
package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"

	"github.com/tracerbiubiubiu/zhuzhao-utils/aksk"
)

const base = "http://127.0.0.1:6397"

func do(method, path string, body []byte) {
	req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	aksk.Sign(req, body, aksk.SignOptions{AK: "zhuzhao", SK: []byte("sk-e2e-zhuzhao"), Operator: "e2e"})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("0 ERR", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	b := make([]byte, 8192)
	n, _ := resp.Body.Read(b)
	fmt.Printf("%d %s\n", resp.StatusCode, string(b[:n]))
}

func main() {
	switch os.Args[1] {
	case "submit": // submit <id> <cbPath>
		do("POST", "/v1/tasks", fmt.Appendf(nil,
			`{"task_id":%q,"action":"audit_archive","callback_url":"http://127.0.0.1:6398%s","params":{},"dept":"e2e"}`,
			os.Args[2], os.Args[3]))
	case "cancel": // cancel <id>
		do("POST", "/v1/tasks/cancel", fmt.Appendf(nil, `{"task_id":%q}`, os.Args[2]))
	case "retry": // retry <id>
		do("POST", "/v1/tasks/retry", fmt.Appendf(nil, `{"task_id":%q}`, os.Args[2]))
	case "get": // get <id>
		do("GET", "/v1/tasks/"+os.Args[2], nil)
	case "dead":
		do("GET", "/v1/dead-letters?page=1&page_size=50", nil)
	case "createjob":
		do("POST", "/v1/jobs", []byte(`{"action_id":"audit_archive","callback_url":"http://127.0.0.1:6398/cb","trigger_type":"cron","cron_spec":"* * * * *","params":{},"dept":"e2e","timeout_secs":30}`))
	default:
		os.Exit(2)
	}
}
