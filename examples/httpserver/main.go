// Command httpserver 演示 batcher 的典型用法：一个 HTTP 服务把
// 并发到达的单次查询（GET /user?id=N）扇入为对下游批量查询接口的
// 一次调用，结果按位置归还各请求。
//
// 运行：go run ./examples/httpserver
package main

import (
	"context"
	"encoding/json"
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

func main() {
	ctx := context.Background()

	// 1. 模拟下游批量查询服务：GET /batch-users?ids=1,2,3。
	batchAddr := serve(newBatchService())
	client := &http.Client{Timeout: 5 * time.Second}

	// 2. Batcher：把零散的单 id 查询扇入为一次批量查询。
	b := batcher.New[int, user](
		func(ctx context.Context, ids []int) ([]user, error) {
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
			var users []user
			if err := json.NewDecoder(resp.Body).Decode(&users); err != nil {
				return nil, err
			}
			return users, nil // 与 ids 按位置对位：下游须等长、同序返回
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
		u, err := b.Do(r.Context(), id) // 调用方 ctx 只治理本次等待
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(u)
	})
	upAddr := serve(upMux)

	// 4. 并发打 10 个单次请求：预期合并为 8+2 两个批次。
	fmt.Println("[上游] 并发 10 个单次查询 GET /user?id=1..10")
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := http.Get(fmt.Sprintf("http://%s/user?id=%d", upAddr, i))
			if err != nil {
				log.Printf("id=%d: %v", i, err)
				return
			}
			defer resp.Body.Close()
			var u user
			_ = json.NewDecoder(resp.Body).Decode(&u)
			fmt.Printf("[请求] id=%-2d → %s\n", i, u.Name)
		}(i)
	}
	wg.Wait()

	if err := b.Close(ctx); err != nil {
		log.Fatalf("Close: %v", err)
	}
}

// newBatchService 构造模拟的下游批量查询服务：
// 按 ids 顺序等长返回同名结果，附带模拟查询开销。
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
		users := make([]user, len(ids))
		for i, id := range ids {
			users[i] = user{ID: id, Name: fmt.Sprintf("user-%d", id)}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(users)
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
	fmt.Printf("[观测] flush size=%d wait=%.1fms duration=%.1fms err=%v\n",
		e.Size, e.WaitSeconds*1000, e.DurationSeconds*1000, e.Err)
}

func (printObserver) OnClosed(batcher.ClosedEvent) {
	fmt.Println("[观测] closed")
}
