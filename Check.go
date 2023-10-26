package lsm

import (
	"github.com/zxpeach/Lsm-Tree/config"
	"log"
	"time"
)

func Check() {
	con := config.GetConfig()
	ticker := time.Tick(time.Duration(con.CheckInterval) * time.Second)
	for _ = range ticker {
		log.Println("LSM-TREE: Performing background checks...")
		// 检查内存
		checkMemory()
		// 检查压缩数据库文件
		database.TableTree.Check()
	}
}

func checkMemory() {
	con := config.GetConfig()
	count := database.MemoryTree.GetCount()
	if count < con.Threshold {
		return
	}
	// 交互内存
	log.Println("LSM-TREE: Compressing memory")
	tmpTree := database.MemoryTree.Swap()

	// 将内存表存储到 SsTable 中
	database.TableTree.CreateNewTable(tmpTree.GetValues())
	database.Wal.Reset()
}
