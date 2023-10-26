
## 使用方法

本地导入：
```go
git clone git@github.com:zxpeach/Nil-KV.git
```

使用方法：
```go
package main

import (
	"bufio"
	"fmt"
	"github.com/zxpeach/Nil-KV"
	"github.com/zxpeach/Nil-KV/config"
	"os"
	"time"
)

func main(){
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
			inputReader := bufio.NewReader(os.Stdin)
			_, _ = inputReader.ReadString('\n')
		}
	}()
	lsm.Start(config.Config{
		DataDir:       `E:\test`,
		Level0Size:    100,
		PartSize:      4,
		Threshold:     3000,
		CheckInterval: 3,
	})
	lsm.Set("key1", "value1")
	v, _ := lsm.Get[TestValue]("key1")
}
```
