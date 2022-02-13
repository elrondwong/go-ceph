package rados

/*
#cgo LDFLAGS: -lrados
#include <errno.h>
#include <stdlib.h>
#include <rados/librados.h>

extern void watch2Callback(uintptr_t,
				 uint64_t,
				 uint64_t,
				 uint64_t,
				 uintptr_t,
				 size_t);

extern void watchErrCallback(uintptr_t, uint64_t, int);

// inline wrapper to cast uintptr_t to void*
static inline int wrap_rados_watch3(rados_ioctx_t io,
    const char *o,
    uint64_t *cookie,
    uint32_t timeout,
    uintptr_t arg) {
        return rados_watch3(io,
        o,
        cookie,
        (void *)watch2Callback,
        (void *)watchErrCallback,
        timeout,
        (void *)arg);
    };
*/
import "C"

import (
	"unsafe"

	"github.com/ceph/go-ceph/internal/callbacks"
)

// watchCallbacks  what to do when a notify is received on this object
// watchErrCallbacks what to do when the watch session encounters an error
var watch2Callbacks, watchErrCallbacks = callbacks.New(), callbacks.New()


// Watch represents on an ongoing object metadata watch.
type Watch struct {
	ctx     *IOContext
	wcc     watch2Ctx
	cookie  C.uint64_t
	cbIndex uintptr
}

// Watcher2Callback define the function signature needed for the Watch
type Watcher2Callback func(arg interface{},
	notifyID uint64,
	handle uint64,
	notifierID uint64,
	data interface{},
	dataLen uint64)

// Watcher2ErrorCallback define the function signature needed for the Watch
type Watcher2ErrorCallback func(arg interface{}, cookie uint64, err int)

type watch2Ctx struct {
	callback    Watcher2Callback
	errCallback Watcher2ErrorCallback
	data        interface{}
}

// Watch register an interest in an object
//
// Implements:
//   int rados_watch3(rados_ioctx_t ctx,
//                    const char *o,
//                    uint64_t *cookie,
//                    rados_watchcb2_t watchcb,
//                    rados_watcherrcb_t watcherrcb,
//                    uint32_t timeout,
//                    void *arg);
func (ioctx *IOContext) Watch(oid string,
	cb Watcher2Callback,
	eb Watcher2ErrorCallback,
	timeout uint32,
	data interface{}) (*Watch, error) {
	wcc := watch2Ctx{
		callback:    cb,
		errCallback: eb,
		data:        data,
	}
	w := &Watch{
		ctx:     ioctx,
		wcc:     wcc,
		cbIndex: watch2Callbacks.Add(wcc),
	}
	var cObjectName *C.char
	cObjectName = C.CString(oid)
	defer C.free(unsafe.Pointer(cObjectName))
	ret := C.wrap_rados_watch3(
		ioctx.ioctx,
		cObjectName,
		&w.cookie,
		C.uint32_t(timeout),
		C.uintptr_t(w.cbIndex))
	if ret != 0 {
		return nil, getError(ret)
	}
	return w, nil

}

//export watch2Callback
func watch2Callback(args uintptr, notifyID, handle, notifierID C.uint64_t, data uintptr, dataLen C.size_t) {
	v := watch2Callbacks.Lookup(args)
	wc := v.(watch2Ctx)
	wc.callback(wc.data, uint64(notifyID), uint64(handle), uint64(notifierID), data, uint64(dataLen))
}

//export watchErrCallback
func watchErrCallback(pre uintptr, cookie C.uint64_t, err C.int) {
	v := watchErrCallbacks.Lookup(pre)
	wec := v.(watch2Ctx)
	wec.errCallback(wec.data, uint64(cookie), int(err))
}

// Notify sychronously notify watchers of an object
//
// Implements:
//   int rados_notify2(rados_ioctx_t io, const char *o,
//				 const char *buf, int buf_len,
//				 uint64_t timeout_ms,
//				 char **reply_buffer, size_t *reply_buffer_len);
func (ioctx *IOContext) Notify(oid string, data []byte, timeout uint64) error {
	cOid := C.CString(oid)
	defer C.free(unsafe.Pointer(cOid))

	dataPointer := unsafe.Pointer(nil)
	if len(data) > 0 {
		dataPointer = unsafe.Pointer(&data[0])
	}

	length := 0
	var replyPointer *C.char
	lengthPointer := unsafe.Pointer(&length)

	ret := C.rados_notify2(
		ioctx.ioctx,
		cOid,
		(*C.char)(dataPointer),
		(C.int)(len(data)),
		(C.uint64_t)(timeout),
		(**C.char)(&replyPointer),
		(*C.size_t)(lengthPointer))

	if replyPointer != nil {
		C.rados_buffer_free(replyPointer)
	}

	return getError(ret)
}

// NotifyAck acknolwedge receipt of a notify.
//
// Implements:
//  int rados_notify_ack(rados_ioctx_t io, const char *o,
//				    uint64_t notify_id, uint64_t cookie,
//				    const char *buf, int buf_len);
func (ioctx *IOContext) NotifyAck(oid string, notifyID, cookie uint64, data []byte) error {
	cOid := C.CString(oid)
	defer C.free(unsafe.Pointer(cOid))

	dataPointer := unsafe.Pointer(nil)
	if len(data) > 0 {
		dataPointer = unsafe.Pointer(&data[0])
	}

	ret := C.rados_notify_ack(
		ioctx.ioctx,
		cOid,
		C.uint64_t(notifyID),
		C.uint64_t(cookie),
		(*C.char)(dataPointer),
		(C.int)(len(data)))

	return getError(ret)
}

// UnWatch un-register the object watch.
//
// Implements:
//   int rados_unwatch2(rados_ioctx_t io, uint64_t cookie);
func (w *Watch) UnWatch() error {
    if w.ctx == nil {
        return ErrInvalidIOContext
    }
	ret := C.rados_unwatch2(
        w.ctx.ioctx,
		w.cookie)
    watch2Callbacks.Remove(w.cbIndex)
	return getError(ret)
}

// WatchFlush flush watch/notify callbacks
//
// Implements:
//   int rados_watch_flush(rados_t cluster);
func (c *Conn) WatchFlush() error {
	ret := C.rados_watch_flush(c.cluster)

	return getError(ret)
}
