About
=====

This is the log *D*ata *A*ggregation and *S*elect *Util* package, So I called dasUtil.

这是一个日志数据聚合、查询的通用组件。


Table of Contents
=================
* [About](#about)
* [Installing](#installing)
* [Example](#example)
* [Docs](#docs)
* [Author](#author)
* [Copyright and License](#copyright-and-license)

Installing
==========

Using *go get*
-------------

    $ go get github.com/vislee/dasUtil

After this command *dasUtil* is ready to use. Its source will be in:
    $GOPATH/src/github.com/vislee/dasUtil

[Back to TOC](#table-of-contents)


Example
=======

```go
package main

import (
    "fmt"
    "github.com/vislee/dasUtil"
)


func main() {
    // the log delimiter
    sep := "\\t"
    // the log format
    colms := []string{"host", "remote_addr", "uri", "byt", "status"}

    tb := dasUtil.NewTable("host", []string{"remote_addr", "uri"}, []string{}, nil)
    defer tb.Destroy()

    r1 := dasUtil.NewRow()
    log1 := "www.liwq.com\\t192.168.1.1\\t/test.php\\t30\\t200"
    r1.SplitParse(&log1, &sep, colms)
    tb.Insert(r1)

    r2 := dasUtil.NewRow()
    log2 := "www.liwq.com\\t192.168.1.1\\t/test2.php\\t10\\t200"
    r2.SplitParse(&log2, &sep, colms)
    tb.Insert(r2)

    r3 := dasUtil.NewRow()
    log3 := "www.liwq.com\\t192.168.1.1\\t/test3.php\\t10\\t504"
    r3.SplitParse(&log3, &sep, colms)
    tb.Insert(r3)

    r4 := dasUtil.NewRow()
    log4 := "www.liwq.com\\t192.168.1.2\\t/test.php\\t20\\t504"
    r4.SplitParse(&log4, &sep, colms)
    tb.Insert(r4)

    r5 := dasUtil.NewRow()
    log5 := "www.liwq.com\\t192.168.1.2\\t/test.php\\t20\\t502"
    r5.SplitParse(&log5, &sep, colms)
    tb.Insert(r5)

    r6 := dasUtil.NewRow()
    log6 := "www.liwq.com\\t192.168.1.3\\t/test.php\\t1000\\t500"
    r6.SplitParse(&log6, &sep, colms)
    tb.Insert(r6)

    r7 := dasUtil.NewRow()
    log7 := "www.liwq.com\\t192.168.1.1\\t/test.php\\t10\\t500"
    r7.SplitParse(&log7, &sep, colms)
    tb.Insert(r7)

    // query the remote_addr is "192.168.1.1" log record
    rs := tb.Select(map[string]string{"remote_addr": "192.168.1.2"})
    for _, r := range rs {
        fmt.Printf("host: %s remote_addr: %s uri: %s status: %s\n", r.GetNull("host"), r.GetNull("remote_addr"), r.GetNull("uri"), r.GetNull("status"))
    }
    // output:
    // host: www.liwq.com remote_addr: 192.168.1.2 uri: /test.php status: 504
    // host: www.liwq.com remote_addr: 192.168.1.2 uri: /test.php status: 502

    // group by remote_addr
    gtb := tb.GroupBy([]string{"remote_addr"}, []string{}, []string{"byt"}, map[string][]string{"status": []string{"200", "5xx"}})
    defer gtb.Destroy()

    // remote_addr request times max top1
    maxTop1Tabs := gtb.OrderbyTopN(1)
    fmt.Printf("remote_addr: %s sum bytes: %f status_200 : %d status_5xx: %d\n",
        maxTop1Tabs[0].TabName, maxTop1Tabs[0].SumCol["byt"], maxTop1Tabs[0].CountCol["status:200"], maxTop1Tabs[0].CountCol["status:5xx"])
    // output:
    // remote_addr: 192.168.1.1 sum bytes: 60.000000 status_200 : 2 status_5xx: 2


    for _, r := range maxTop1Tabs[0].Rows {
        fmt.Printf("remote_addr: %s uri: %s byt: %s status: %s\n", r.GetNull("remote_addr"), r.GetNull("uri"), r.GetNull("byt"), r.GetNull("status"))
    }
    // output:
    // remote_addr: 192.168.1.1 uri: /test.php byt: 30 status: 200
    // remote_addr: 192.168.1.1 uri: /test2.php byt: 10 status: 200
    // remote_addr: 192.168.1.1 uri: /test3.php byt: 10 status: 504
    // remote_addr: 192.168.1.1 uri: /test.php byt: 10 status: 500

    // max byte ip
    maxBytTop1 := gtb.OrderbyItemTopN("byt", 1)
    fmt.Println(maxBytTop1[0].TabName)
    // output:
    // 192.168.1.3


    return
}

```


[Back to TOC](#table-of-contents)

Docs
====

https://godoc.org/github.com/vislee/dasUtil


[Back to TOC](#table-of-contents)


Author
======

wenqiang li(vislee)

[Back to TOC](#table-of-contents)


Copyright and License
=====================

This module is licensed under the BSD license.

Copyright (C) 2018, by vislee.

All rights reserved.

[Back to TOC](#table-of-contents)
