package dasUtil

import (
	"fmt"
	"testing"
)

func TestInsert(t *testing.T) {
	tb := NewTable("domain", []string{}, []string{}, nil)
	r1 := NewRow()
	log := "liwq.com\\t192.168.1.1\\t/test.php"
	sep := "\\t"
	r1.SplitParse(&log, &sep, []string{"domain", "ip", "path"})
	tb.Insert(r1)
	if len(tb.Rows) != 1 || tb.TabName != "domain" || tb.TabSize != 1 || tb.Rows[0].GetNull("ip") != "192.168.1.1" {
		t.Error("table insert error")
	}
	tb.Destroy()
}

func TestIndex(t *testing.T) {
	sep := "\\t"
	tb := NewTable("domain", []string{"ip", "path"}, []string{}, nil)
	r1 := NewRow()
	log := "liwq.com\\t192.168.1.1\\t/test.php"
	r1.SplitParse(&log, &sep, []string{"domain", "ip", "path"})
	tb.Insert(r1)
	if len(tb.index) != 2 {
		t.Error("table index error")
	}
	if _, ok := tb.index["ip"]; !ok {
		t.Error("table index no ip")
	}
	if _, ok := tb.index["path"]; !ok {
		t.Error("table index no path")
	}
	tb.Destroy()
}

func TestSelect(t *testing.T) {
	sep := "\\t"
	tb := NewTable("domain", []string{"ip"}, []string{}, nil)
	r1 := NewRow()
	// r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php", "sent": "10"}
	log := "liwq.com\\t192.168.1.1\\t/test.php\\t10"
	r1.SplitParse(&log, &sep, []string{"domain", "ip", "path", "sent"})
	tb.Insert(r1)
	r2 := NewRow()
	// r2 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/test.php", "sent": "20"}
	log = "liwq.com\\t192.168.1.2\\t/test.php\\t20"
	r2.SplitParse(&log, &sep, []string{"domain", "ip", "path", "sent"})
	tb.Insert(r2)
	rs := tb.Select(map[string]string{"ip": "192.168.1.2"})
	if len(rs) != 1 || rs[0].GetNull("ip") != "192.168.1.2" {
		t.Error("table select error")
	}
	tb.Destroy()
}

func TestGroupTable(t *testing.T) {
	sep := "\\t"
	colms := []string{"domain", "ip", "path", "sent", "status"}
	tb := NewTable("domain", []string{"ip", "path"}, []string{}, nil)

	r1 := NewRow()
	log1 := "liwq.com\\t192.168.1.1\\t/test.php\\t10\\t200"
	r1.SplitParse(&log1, &sep, colms)
	tb.Insert(r1)

	r2 := NewRow()
	log2 := "liwq.com\\t192.168.1.1\\t/test2.php\\t10\\t200"
	r2.SplitParse(&log2, &sep, colms)
	tb.Insert(r2)

	r3 := NewRow()
	log3 := "liwq.com\\t192.168.1.1\\t/test3.php\\t10\\t504"
	r3.SplitParse(&log3, &sep, colms)
	tb.Insert(r3)

	r4 := NewRow()
	log4 := "liwq.com\\t192.168.1.2\\t/test.php\\t20\\t504"
	r4.SplitParse(&log4, &sep, colms)
	tb.Insert(r4)

	r5 := NewRow()
	log5 := "liwq.com\\t192.168.1.2\\t/test.php\\t20\\t502"
	r5.SplitParse(&log5, &sep, colms)
	tb.Insert(r5)

	r6 := NewRow()
	log6 := "liwq.com\\t192.168.1.3\\t/test.php\\t10\\t603"
	r6.SplitParse(&log6, &sep, colms)
	tb.Insert(r6)

	grptb := tb.GroupBy([]string{"ip"}, []string{}, []string{"sent"}, map[string][]string{"status": []string{"502", "504", "200", "50x", "5xx"}})
	if len(grptb.GrptabList) != 3 || grptb.ParentSize != 6 {
		t.Errorf("group table error %d %d", len(grptb.GrptabList), grptb.ParentSize)
	}

	tbs := grptb.OrderbyTopN(1)
	if len(tbs) != 1 {
		t.Error("group table order error")
	}

	if len(tbs[0].Rows) != 3 || tbs[0].TabSize != 3 {
		t.Error("group table order rows error")
	}

	if tbs[0].Rows[0].GetNull("ip") != "192.168.1.1" {
		t.Error("group table order row error")
	}

	if tbs[0].SumCol["sent"] != 30 {
		t.Error("group table sum error")
	}

	if tbs[0].CountCol["status:200"] != 2 || tbs[0].CountCol["status:504"] != 1 {
		t.Error("group table count error")
	}

	tbsBysent := grptb.OrderbyItemTopN("sent", 1)
	if len(tbsBysent) != 1 {
		t.Error("group table order by item error")
	}

	if len(tbsBysent[0].Rows) != 2 || tbsBysent[0].TabSize != 2 {
		t.Error("group table order by item rows error")
	}

	if tbsBysent[0].Rows[0].GetNull("ip") != "192.168.1.2" {
		t.Error("group table order by item row error")
	}

	if tbsBysent[0].SumCol["sent"] != 40 {
		t.Error("group table order by item sum error")
	}

	if tbsBysent[0].CountCol["status:502"] != 1 || tbsBysent[0].CountCol["status:50x"] != 2 {
		fmt.Println(tbsBysent[0].CountCol)
		t.Error("group table order by item count error", tbsBysent[0].CountCol["status:502"])
	}

	grptb2 := tb.GroupBy([]string{"ip", "path"}, []string{}, []string{}, nil)
	if len(grptb2.GrptabList) != 5 || grptb2.ParentSize != 6 {
		t.Errorf("group table many col error")
	}

	tbs2 := grptb2.OrderbyTopN(1)
	if len(tbs2) != 1 {
		t.Error("group table many col order error")
	}

	if len(tbs2[0].Rows) != 2 || tbs2[0].TabSize != 2 {
		t.Error("group table many col order rows error")
	}

	if tbs2[0].Rows[0].GetNull("ip") != "192.168.1.2" {
		t.Error("group table many col order row error")
	}

	if tbs2[0].TabName != "192.168.1.2-/test.php" {
		t.Error("group table table name error")
	}

	tbs3 := grptb.OrderbyDescTopN(1)
	if tbs3[0].TabSize != 1 || tbs3[0].Rows[0].GetNull("ip") != "192.168.1.3" {
		t.Error("group table orderdesc error")
	}
}

func TestGroupTable2(t *testing.T) {
	tb := NewTable("domain", []string{"ip", "path"}, []string{}, nil)
	r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/"}
	tb.Insert(NewColsRow(r1))
	r2 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/"}
	tb.Insert(NewColsRow(r2))
	r3 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/"}
	tb.Insert(NewColsRow(r3))
	r4 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/"}
	tb.Insert(NewColsRow(r4))

	grptb := tb.GroupBy([]string{"path"}, []string{}, []string{}, nil)
	if len(grptb.GrptabList) != 1 || grptb.ParentSize != 4 {
		t.Error("group table error")
	}

	if len(grptb.groupTable) != 1 {
		t.Error("group table num error")
	}

	tbs := grptb.OrderbyTopN(1)
	if len(tbs) != 1 {
		t.Error("group table order error")
	}
	tb.Destroy()
	grptb.Destroy()
}

func TestJoinTable(t *testing.T) {
	tb := NewTable("domain", []string{"ip", "path"}, []string{}, nil)
	r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php"}
	tb.Insert(NewColsRow(r1))
	r2 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test2.php"}
	tb.Insert(NewColsRow(r2))
	r3 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test3.php"}
	tb.Insert(NewColsRow(r3))
	r4 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/test.php"}
	tb.Insert(NewColsRow(r4))
	r5 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/test.php"}
	tb.Insert(NewColsRow(r5))
	r6 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.3", "path": "/test.php"}
	tb.Insert(NewColsRow(r6))

	tb2 := NewTable("domain", []string{}, []string{}, nil)
	r21 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php"}
	tb2.Insert(NewColsRow(r21))
	r22 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test2.php"}
	tb2.Insert(NewColsRow(r22))

	tb1size := tb.TabSize

	tb.Join(tb2)
	if tb.TabSize != tb1size+tb2.TabSize {
		t.Error("table join size error", tb.TabSize)
	}
	gtb := tb.GroupBy([]string{"ip"}, []string{}, []string{}, nil)
	tbs := gtb.OrderbyTopN(1)
	if len(tbs) != 1 {
		t.Error("table join group order error")
	}
	if len(tbs[0].Rows) != 5 || "192.168.1.1" != tbs[0].Rows[0].GetNull("ip") {
		t.Error("table join group rows error", len(tbs[0].Rows))
	}
}

