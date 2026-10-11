// Command httpserver 演示 batcher 的典型用法：一个 HTTP 服务把
// 并发到达的单次查询（GET /user?id=N）扇入为对下游批量查询接口的
// 一次调用，结果按位置归还各请求；并演示批内 key 去重——热点 id
// 的并发查询在交付边界折叠，下游不再重复查询同一 id——以及逐条
// 业务失败（id 7 在下游不存在）：以 Result 表达、结果切片仍等长，
// 只有受影响的请求得到 404，其余照常 200。
//
// 运行：go run ./examples/httpserver
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jninng/batcher"
)

// user 是查询结果的领域类型。
type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// batchEntry 是下游批量接口的单条返回：Error 非空即该条失败
// （模拟 mget 缺 key 之类的逐条业务失败）。
type batchEntry struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Error string `json:"error,omitempty"`
}

func main() {
	ctx := context.Background()

	// 1. 模拟下游批量查询服务：GET /batch-users?ids=1,2,3。
	batchAddr := serve(newBatchService())
	client := &http.Client{Timeout: 5 * time.Second}

	// 2. Batcher：把零散的单 id 查询扇入为一次批量查询。key 函数声明
	// 「id 相同 ⇒ 语义等价」：批内重复 id 在交付边界折叠为一条，热点
	// key 不再重复打下游；无重复时折叠透明，行为同普通 New。Res 用
	// batcher.Result 逐条表达业务失败：批次函数返回与入参等长的结果
	// 切片，单条缺失只影响该条（Do 的 error 仍只报告批级失败）。
	b := batcher.NewKeyed(func(id int) int { return id },
		func(ctx context.Context, ids []int) ([]batcher.Result[user], error) {
			fmt.Printf("[下游] 一次批量查询 ids=%v\n", ids)
			ss := make([]string, len(ids))
			for i, id := range ids {
				ss[i] = strconv.Itoa(id)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet,
				fmt.Sprintf("http://%s/batch-users?ids=%s", batchAddr, strings.Join(ss, ",")), nil)
			if err != nil {
				return nil, err
			}
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			var entries []batchEntry
			if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
				return nil, err
			}
			if len(entries) != len(ids) { // 逐条失败也等长返回，长度契约不变
				return nil, fmt.Errorf("downstream returned %d entries for %d ids", len(entries), len(ids))
			}
			results := make([]batcher.Result[user], len(entries))
			for i, e := range entries {
				if e.Error != "" {
					results[i] = batcher.Result[user]{Err: errors.New(e.Error)}
					continue
				}
				results[i] = batcher.Result[user]{Value: user{ID: e.ID, Name: e.Name}}
			}
			return results, nil // 与 ids 按位置对位
		},
		batcher.WithMaxSize(8),
		batcher.WithInterval(100*time.Millisecond),
		batcher.WithObserver(printObserver{}), // 类型化旁路观测
	)

	// 3. 上游服务：单次查询入口，内部走 batcher。
	upMux := http.NewServeMux()
	upMux.HandleFunc("GET /user", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.URL.Query().Get("id"))
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		res, err := b.Do(r.Context(), id) // 调用方 ctx 治理本次提交与等待；取消即提前返回
		if err != nil {                   // 批级失败（下游整体挂掉）才 500
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if res.Err != nil { // 逐条失败（单条不存在）→ 404
			http.Error(w, res.Err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res.Value)
	})
	upAddr := serve(upMux)

	// 4. 并发打 10 个单次请求（id 互异，去重透明）：预期合并为 8+2 两个批次。
	fmt.Println("[上游] 并发 10 个单次查询 GET /user?id=1..10")
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fetchAndPrint(upAddr, i)
		}(i)
	}
	wg.Wait()

	// 5. 热点 key：16 个并发查询全部命中 3 个热点 id（其中 7 在下游
	// 不存在）。重复 Item 照常占槽（maxSize 按原始条数计数），每批
	// 8 条在交付边界折叠为至多 3 条 unique——下游每批只见到 3 个 id，
	// 每个请求仍拿回自己的答案（id 7 各次都得 404）。
	hot := []int{7, 42, 99}
	fmt.Printf("[上游] 热点 key：并发 16 个查询，全部命中 id∈%v\n", hot)
	var hotWg sync.WaitGroup
	for i := 0; i < 16; i++ {
		hotWg.Add(1)
		go func(id int) {
			defer hotWg.Done()
			fetchAndPrint(upAddr, id)
		}(hot[i%len(hot)])
	}
	hotWg.Wait()

	if err := b.Close(ctx); err != nil {
		log.Fatalf("Close: %v", err)
	}
}

// fetchAndPrint 发起单次查询并把结果（含逐条 404）打印到标准输出。
func fetchAndPrint(upAddr string, id int) {
	resp, err := http.Get(fmt.Sprintf("http://%s/user?id=%d", upAddr, id))
	if err != nil {
		log.Printf("id=%d: %v", id, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound { // 逐条失败：只有该请求受影响
		fmt.Printf("[请求] id=%-2d → 不存在\n", id)
		return
	}
	var u user
	_ = json.NewDecoder(resp.Body).Decode(&u)
	fmt.Printf("[请求] id=%-2d → %s\n", id, u.Name)
}

// newBatchService 构造模拟的下游批量查询服务：
// 按 ids 顺序等长返回结果（id 7 不存在，以 Error 表达），
// 附带模拟查询开销。
func newBatchService() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /batch-users", func(w http.ResponseWriter, r *http.Request) {
		var ids []int
		for _, s := range strings.Split(r.URL.Query().Get("ids"), ",") {
			id, err := strconv.Atoi(s)
			if err != nil {
				http.Error(w, "bad ids", http.StatusBadRequest)
				return
			}
			ids = append(ids, id)
		}
		time.Sleep(30 * time.Millisecond) // 模拟下游批量查询开销
		entries := make([]batchEntry, len(ids))
		for i, id := range ids {
			if id == 7 { // 模拟单条不存在
				entries[i] = batchEntry{ID: id, Error: "not found"}
				continue
			}
			entries[i] = batchEntry{ID: id, Name: fmt.Sprintf("user-%d", id)}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(entries)
	})
	return mux
}

// serve 在 127.0.0.1 随机端口运行 mux，返回监听地址。
func serve(mux *http.ServeMux) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	return ln.Addr().String()
}

// printObserver 演示 observ 风格的类型化旁路观测。
type printObserver struct{}

func (printObserver) OnCreated(e batcher.CreatedEvent) {
	fmt.Printf("[观测] created maxSize=%d interval=%v\n", e.MaxSize, e.Interval)
}

func (printObserver) OnBatchFlushed(e batcher.BatchFlushedEvent) {
	fmt.Printf("[观测] flush size=%d unique=%d wait=%.1fms duration=%.1fms err=%v\n",
		e.Size, e.UniqueSize, e.WaitSeconds*1000, e.DurationSeconds*1000, e.Err)
}

func (printObserver) OnClosed(batcher.ClosedEvent) {
	fmt.Println("[观测] closed")
}
