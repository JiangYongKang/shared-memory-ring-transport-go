package shmring

import "unsafe"

func unsafeSlicePtr(b []byte) unsafe.Pointer {
	return unsafe.Pointer(&b[0])
}
