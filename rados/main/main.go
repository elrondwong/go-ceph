package main

import (
    "fmt"
    "github.com/ceph/go-ceph/rados"
    "time"
)

func main() {
    conn, _ := rados.NewConnWithClusterAndUser("ceph", "client.admin")
    err := conn.SetConfigOption("mon_host", "[v2:172.16.80.86:3300,v1:172.16.80.86:6789],[v2:172.16.80.136:3300,v1:172.16.80.136:6789],[v2:172.16.80.10:3300,v1:172.16.80.10:6789]")
    if err != nil {
        fmt.Printf("set mon_host:" + err.Error())
    }
    err = conn.SetConfigOption("key", "AQC5Z1Rh6Nu3BRAAc98ORpMCLu9kXuBh/k3oHA==")
    // err = conn.SetConfigOption("keyring", "/tmp/user3")
    if err != nil {
        fmt.Printf("set name and key: " + err.Error())
    }
    err = conn.Connect()
    if err != nil {
        fmt.Printf("con conn: "+err.Error())
    }

    defer conn.Shutdown()

    oid := "rbd_id.test001"
    ioctx2, err := conn.OpenIOContext("volumes")
    if err != nil {
        fmt.Println("open ioctx errors"+ err.Error())
    }

    w, err := ioctx2.Watch(oid,
       func(arg interface{}, notifyID uint64, handle uint64, notifierID uint64, data interface{}, dataLen uint64) {
       err := ioctx2.NotifyAck(oid, notifyID, handle, nil)
       if err != nil {
           fmt.Printf("ack notify error: %+v", err)
       }
       fmt.Printf("notify ack ok\n")

    }, func(i interface{}, u uint64, i2 int) {

       }, 30, nil)

    if err != nil {
       fmt.Printf("watch error: "+err.Error())
    }

    err = ioctx2.Notify(oid, []byte("xxx"), 30)
    if err != nil {
      fmt.Printf("notify failed: %+v", err)
    }
    fmt.Printf("notify successful\n")

    err = conn.WatchFlush()
    if err != nil {
       fmt.Printf("flush error %+v", err)
    }

    fmt.Printf("flush successful\n")
    time.Sleep(5000 * time.Millisecond)

    err = w.UnWatch()
    if err != nil {
        fmt.Printf("UnWatch failed: %+v", err)
    }

    fmt.Printf("finally successful\n")
    //fmt.Printf("www: %+v", w)
}
