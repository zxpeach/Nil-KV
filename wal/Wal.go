package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/zxpeach/Lsm-Tree/kv"
	"github.com/zxpeach/Lsm-Tree/skipList"
	"log"
	"os"
	"path"
	"sync"
	"time"
)

type Wal struct {
	f    *os.File
	path string
	lock sync.Locker
}

func (w *Wal) Init(dir string) *skipList.SkipList {
	log.Println("Nil-KV : Loading wal.log...")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Nil-KV : Loaded wal.log,Consumption of time : ", elapse)
	}()

	walPath := path.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("Nil-KV : The wal.log file cannot be created")
		panic(err)
	}
	w.f = f
	w.path = walPath
	w.lock = &sync.Mutex{}
	return w.WalToMemory()
}

// 通过wal.log文件初始化 Wal，加载文件中的日记到内存
func (w *Wal) WalToMemory() *skipList.SkipList {
	w.lock.Lock()
	defer w.lock.Unlock()

	info, _ := os.Stat(w.path)
	size := info.Size()
	list := &skipList.SkipList{}
	list.Init()

	// 空的,返回
	if size == 0 {
		return list
	}

	_, err := w.f.Seek(0, 0)
	if err != nil {
		log.Println("Nil-KV : Failed to open wal.log")
		panic(err)
	}
	// 文件指针移动到最后，以便追加
	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Println("Nil-KV : Failed to open wal.log")
			panic(err)
		}
	}(w.f, size-1, 0)

	// 将文件内容全部读取到内存
	data := make([]byte, size)
	_, err = w.f.Read(data)
	if err != nil {
		log.Println("Nil-KV : Failed to open wal.log")
		panic(err)
	}

	dataLen := int64(0) // 元素的字节数量
	index := int64(0)   // 当前索引
	for index < size {
		// 前面的 8 个字节表示元素的长度
		indexData := data[index:(index + 8)]
		// 获取元素的字节长度
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen)
		if err != nil {
			log.Println("Nil-KV : Failed to open wal.log")
			panic(err)
		}
		// 将元素的所有字节读取出来，并还原为 kv.Value
		index += 8
		dataArea := data[index:(index + dataLen)]
		var value kv.Value
		err = json.Unmarshal(dataArea, &value)
		if err != nil {
			log.Println("Nil-KV : Failed to open wal.log")
			panic(err)
		}

		if value.Deleted {
			list.Delete(value.Key)
		} else {
			list.Set(value.Key, value.Value)
		}
		// 读取下一个元素
		index = index + dataLen
	}
	return list
}

// 记录日志
func (w *Wal) Write(value kv.Value) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if value.Deleted {
		log.Println("Nil-KV : wal.log:	delete ", value.Key)
	} else {
		log.Println("Nil-KV : wal.log:	insert ", value.Key)
	}

	data, _ := json.Marshal(value)
	err := binary.Write(w.f, binary.LittleEndian, int64(len(data)))
	if err != nil {
		log.Println("Nil-KV : Failed to write the wal.log")
		panic(err)
	}

	err = binary.Write(w.f, binary.LittleEndian, data)
	if err != nil {
		log.Println("Nil-KV : Failed to write the wal.log")
		panic(err)
	}
}

func (w *Wal) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()

	log.Println("Nil-KV : Resetting the wal.log file")

	err := w.f.Close()
	if err != nil {
		panic(err)
	}
	w.f = nil
	err = os.Remove(w.path)
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	w.f = f
}
