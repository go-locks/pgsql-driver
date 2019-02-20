[go-locks/distlock](https://github.com/go-locks/distlock) 的 `Postgres` 驱动。客户端使用 [lib/pq](https://github.com/lib/pq) 实现。本驱动支持互斥锁 `mutex` 和读写锁 `rwmutex`。更多使用案例详见 [examples](https://github.com/go-locks/examples)


## 代码实例

使用前需创建一个 `dbname`，然后导入 [structure.sql](structure.sql)

```go
var pgDriver = New(
	"host=192.168.0.110 port=5432 user=postgres password= dbname=gotest sslmode=disable",
	// 可继续指定其他节点，当有多个节点时，在过半数节点上加锁成功才算真的成功
	// "host=192.168.0.111 port=5432 user=postgres password= dbname=gotest sslmode=disable",
	// "host=192.168.0.112 port=5432 user=postgres password= dbname=gotest sslmode=disable",
)
```