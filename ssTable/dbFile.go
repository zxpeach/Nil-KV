package ssTable

import (
	"encoding/binary"
	"log"
	"os"
)

/*
管理 SSTable 的磁盘文件
*/

// GetDbSize 获取 .db 数据文件大小
func (table *SSTable) GetDbSize() int64 {
	info, err := os.Stat(table.filePath)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}

// GetLevelSize 获取指定层的 SSTable 总大小
func (tree *TableTree) GetLevelSize(level int) int64 {
	//todo：提前存一下每层的大小
	var size int64
	node := tree.levels[level]
	for node != nil {
		size += node.table.GetDbSize()
		node = node.next
	}
	return size
}

// 将数据写入文件
func writeDataToFile(filePath string, dataArea []byte, indexArea []byte, meta MetaInfo) {
	//todo: 可以换成更优的 mmap 读写文件
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666) //打开目录
	if err != nil {
		log.Fatal("Nil-KV :  error create file,", err)
	}
	_, err = f.Write(dataArea) //写数据区
	if err != nil {
		log.Fatal("Nil-KV :  error write file,", err)
	}
	_, err = f.Write(indexArea) //写稀疏索引区
	if err != nil {
		log.Fatal("Nil-KV :  error write file,", err)
	}
	// 写入元数据到文件末尾
	_ = binary.Write(f, binary.LittleEndian, &meta.version)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataLen)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexLen)
	err = f.Sync()
	if err != nil {
		log.Fatal("Nil-KV :  error write file,", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatal("Nil-KV :  error close file,", err)
	}
}
