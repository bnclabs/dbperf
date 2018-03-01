package main

import "os"
import "fmt"
import "bytes"
import "unsafe"
import "reflect"
import "path/filepath"

func Fixbuffer(buffer []byte, size int64) []byte {
	if buffer == nil || int64(cap(buffer)) < size {
		buffer = make([]byte, size)
	}
	return buffer[:size]
}

func Bytes2str(bytes []byte) string {
	if bytes == nil {
		return ""
	}
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	st := &reflect.StringHeader{Data: sl.Data, Len: sl.Len}
	return *(*string)(unsafe.Pointer(st))
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			size += f.Size()
		}
		return err
	})
	return size, err
}

func comparekeyvalue(key, value []byte, vlen int) bool {
	if vlen > 0 && len(value) > 0 {
		value := value[:len(value)]
		if len(key) >= vlen {
			if k := key[len(key)-len(value):]; bytes.Compare(k, value) != 0 {
				panic(fmt.Errorf("expected %q, got %q", k, value))
			}

		} else {
			m := len(value) - len(key)
			for _, ch := range value[:m] {
				if ch != 0 {
					fmt.Printf("%q\n", value[:m])
					panic(fmt.Errorf("expected %v, got %v", 0, ch))
				}
			}
			if bytes.Compare(value[m:], key) != 0 {
				panic(fmt.Errorf("expected %q, got %q", key, value[m:]))
			}
		}

	}
	return true
}
