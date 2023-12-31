package ssTable

import (
	"github.com/zxpeach/Nil-KV/config"
	"github.com/zxpeach/Nil-KV/kv"
	"github.com/zxpeach/Nil-KV/skipList"
	"log"
	"os"
	"time"
)

/*
TableTree 检查是否需要压缩 SSTable
*/

// Check 检查是否需要压缩数据库文件
func (tree *TableTree) Check() {
	tree.majorCompaction()
}

// 压缩文件
func (tree *TableTree) majorCompaction() {
	con := config.GetConfig()
	for levelIndex, _ := range tree.levels {
		tableSize := int(tree.GetLevelSize(levelIndex) / 1000 / 1000) // 转为 MB
		// 当前层 SSTable 数量是否已经到达阈值
		// 当前层的 SSTable 总大小已经到底阈值
		if tree.getCount(levelIndex) > con.PartSize || tableSize > levelMaxSize[levelIndex] {
			tree.majorCompactionLevel(levelIndex)
		}
	}
}

// 压缩当前层的文件到下一层，只能被 majorCompaction() 调用
func (tree *TableTree) majorCompactionLevel(level int) {
	log.Println("Nil-KV : Compressing layer ", level, " files")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Nil-KV : Completed compression,consumption of time : ", elapse)
	}()

	log.Printf("Nil-KV : Compressing layer %d.db files\r\n", level)
	// 用于加载 一个 SSTable 的数据区到缓存中
	tableCache := make([]byte, levelMaxSize[level])
	currentNode := tree.levels[level]

	// 将当前层的 SSTable 合并到一个随机化跳表中
	memoryTree := &skipList.SkipList{}
	memoryTree.Init()

	tree.lock.Lock()
	for currentNode != nil {
		table := currentNode.table
		// 将 SSTable 的数据区加载到 tableCache 内存中
		if int64(len(tableCache)) < table.tableMetaInfo.dataLen {
			tableCache = make([]byte, table.tableMetaInfo.dataLen)
		}
		newSlice := tableCache[0:table.tableMetaInfo.dataLen]
		// 读取 SSTable 的数据区
		if _, err := table.f.Seek(0, 0); err != nil {
			log.Println("Nil-KV :  error open file ", table.filePath)
			panic(err)
		}
		if _, err := table.f.Read(newSlice); err != nil {
			log.Println("Nil-KV :  error read file ", table.filePath)
			panic(err)
		}
		// 读取每一个元素
		for k, position := range table.sparseIndex {
			if position.Deleted == false {
				value, err := kv.Decode(newSlice[position.Start:(position.Start + position.Len)])
				if err != nil {
					log.Fatal(err)
				}
				memoryTree.Set(k, value.Value)
			} else {
				memoryTree.Delete(k)
			}
		}
		currentNode = currentNode.next
	}
	tree.lock.Unlock()

	// 将 SortTree 压缩合并成一个 SSTable
	values := memoryTree.GetValues()
	newLevel := level + 1
	// 目前最多支持 10 层
	if newLevel > 10 {
		newLevel = 10
	}
	// 创建新的 SSTable
	tree.createTable(values, newLevel)
	// 清理该层的文件
	oldNode := tree.levels[level]
	// 重置该层
	if level < 10 {
		tree.levels[level] = nil
		tree.clearLevel(oldNode)
	}

}

func (tree *TableTree) clearLevel(oldNode *tableNode) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	// 清理当前层的每个的 SSTable
	for oldNode != nil {
		err := oldNode.table.f.Close()
		if err != nil {
			log.Println("Nil-KV :  error close file,", oldNode.table.filePath)
			panic(err)
		}
		err = os.Remove(oldNode.table.filePath)
		if err != nil {
			log.Println("Nil-KV :  error delete file,", oldNode.table.filePath)
			panic(err)
		}
		oldNode.table.f = nil
		oldNode.table = nil
		oldNode = oldNode.next
	}
}
