package lsm

import (
	"github.com/zxpeach/Nil-KV/config"
	"github.com/zxpeach/Nil-KV/skipList"
	"github.com/zxpeach/Nil-KV/ssTable"
	"github.com/zxpeach/Nil-KV/wal"
	"log"
	"os"
)

// Start 启动！
func Start() {
	con := database.con
	if database != nil { // 已有数据库，返回
		return
	}
	// 配置保存到内存中
	log.Println("Nil-KV : Loading a Configuration File")
	config.Init(con)
	// 初始化
	log.Println("Nil-KV : Initializing the database")
	// 启动前进行一次数据压缩
	log.Println("Nil-KV : Performing the data compression......")
	initDatabase(con.DataDir)
	// 检查内存
	checkMemory()
	// 检查压缩数据库文件
	database.TableTree.Check()
	// 启动后台线程
	go Check()
}

// 初始化 Database，从磁盘文件中还原 SSTable、WalF、内存表等
func initDatabase(dir string) {
	database = &Database{ // 创建实例
		MemoryTree: &skipList.SkipList{},
		Wal:        &wal.Wal{},
		TableTree:  &ssTable.TableTree{},
	}
	// 从磁盘文件中恢复数据
	// 如果目录不存在，则为空数据库
	if _, err := os.Stat(dir); err != nil {
		log.Printf("Nil-KV : The %s directory does not exist. The directory is being created\r\n", dir)
		err := os.Mkdir(dir, 0666)
		if err != nil {
			log.Println("Nil-KV : Failed to create the database directory")
			panic(err)
		}
	}
	// 从数据目录中，加载 WalF、database 文件
	// 非空数据库，则开始恢复数据，加载 WalF 和 SSTable 文件
	memoryTree := database.Wal.Init(dir)

	database.MemoryTree = memoryTree
	log.Println("Nil-KV : Loading database...")
	database.TableTree.Init(dir)
}
