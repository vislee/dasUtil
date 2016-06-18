package dasUtil

import (
	"strconv"
	"testing"
)

func TestInsert(t *testing.T) {
	gp := NewGroup("domain", []string{}, []string{})
	r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php"}
	gp.Insert(r1)
	if len(gp.Rows) != 1 || gp.GroupName != "domain" || gp.GroupSize != 1 || gp.Rows[0]["ip"] != "192.168.1.1" {
		t.Error("group insert error")
	}
}

func TestIndex(t *testing.T) {
	gp := NewGroup("domain", []string{"ip", "path"}, []string{})
	r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php"}
	gp.Insert(r1)
	if len(gp.index) != 2 {
		t.Error("group index column error")
	}
	if _, ok := gp.index["ip"]; !ok {
		t.Error("group index colunm no ip")
	}
	if _, ok := gp.index["path"]; !ok {
		t.Error("group index column no path")
	}
}

func TestSelect(t *testing.T) {
	gp := NewGroup("domain", []string{"ip"}, []string{})
	r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php", "sent": "10"}
	gp.Insert(r1)
	r2 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/test.php", "sent": "20"}
	gp.Insert(r2)
	rs := gp.Select(map[string]string{"ip": "192.168.1.2"})
	if len(rs) != 1 || rs[0]["ip"] != "192.168.1.2" {
		t.Error("group select result error")
	}
}

func TestAssembleGroup(t *testing.T) {
	gp := NewGroup("domain", []string{"ip", "path"}, []string{})
	r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php", "sent": "10"}
	gp.Insert(r1)
	r2 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test2.php", "sent": "10"}
	gp.Insert(r2)
	r3 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test3.php", "sent": "10"}
	gp.Insert(r3)
	r4 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/test.php", "sent": "20"}
	gp.Insert(r4)
	r5 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/test.php", "sent": "20"}
	gp.Insert(r5)
	r6 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.3", "path": "/test.php", "sent": "10"}
	gp.Insert(r6)
	agp := gp.GroupBy([]string{"ip"}, []string{}, []string{"sent"})
	if len(agp.GroupList) != 3 || agp.AllGroupSize != 6 {
		t.Error("assemblegroup groupby error")
	}

	top1 := agp.OrderbyTopN(1)
	if len(top1) != 1 {
		t.Error("assemblegroup orderby error")
	}

	if len(top1[0].Rows) != 3 || top1[0].GroupSize != 3 {
		t.Error("assemblegroup orderby rows error")
	}

	if top1[0].Rows[0]["ip"] != "192.168.1.1" {
		t.Error("assemblegroup orderby row content error")
	}

	if top1[0].SumCol["sent"] != 30 {
		t.Error("assemblegroup sum error")
	}

	topsent1 := agp.OrderbyItemTopN("sent", 1)
	if len(topsent1) != 1 {
		t.Error("assemblegroup orderby item error")
	}

	if len(topsent1[0].Rows) != 2 || topsent1[0].GroupSize != 2 {
		t.Error("assemblegroup orderby item rows error")
	}

	if topsent1[0].Rows[0]["ip"] != "192.168.1.2" {
		t.Error("assemblegroup orderby item row content error")
	}

	if topsent1[0].SumCol["sent"] != 40 {
		t.Error("assemblegroup orderby item sum error")
	}

	agp2 := gp.GroupBy([]string{"ip", "path"}, []string{}, []string{})
	if len(agp2.GroupList) != 5 || agp2.AllGroupSize != 6 {
		t.Error("assemblegroup many col groupby error")
	}

	agp2top1 := agp2.OrderbyTopN(1)
	if len(agp2top1) != 1 {
		t.Error("assemblegroup many col orderby error")
	}

	if len(agp2top1[0].Rows) != 2 || agp2top1[0].GroupSize != 2 {
		t.Error("assemblegroup many col orderby rows error")
	}

	if agp2top1[0].Rows[0]["ip"] != "192.168.1.2" {
		t.Error("assemblegroup many col orderby row error")
	}

	if agp2top1[0].GroupName != "192.168.1.2-/test.php" {
		t.Error("assemblegroup group name error")
	}

	agp2dtop1 := agp2.OrderbyDescTopN(1)
	if agp2dtop1[0].GroupSize != 1 || (agp2dtop1[0].Rows[0]["ip"] != "192.168.1.3" && agp2dtop1[0].Rows[0]["ip"] != "192.168.1.1") {
		t.Error("assemblegroup orderdesc error")
	}
}

func TestAssembleGroup2(t *testing.T) {
	gp := NewGroup("domain", []string{"ip", "path"}, []string{})
	r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/"}
	gp.Insert(r1)
	r2 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/"}
	gp.Insert(r2)
	r3 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/"}
	gp.Insert(r3)
	r4 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/"}
	gp.Insert(r4)

	agp := gp.GroupBy([]string{"path"}, []string{}, []string{})
	if len(agp.GroupList) != 1 || agp.AllGroupSize != 4 {
		t.Error("assemblegroup table error")
	}

	if len(agp.groupMap) != 1 {
		t.Error("assemblegroup num error")
	}

	top1 := agp.OrderbyTopN(1)
	if len(top1) != 1 {
		t.Error("assemblegroup orderby error")
	}
}

func TestGroupJoin(t *testing.T) {
	gp := NewGroup("domain", []string{"ip", "path"}, []string{})
	r1 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php"}
	gp.Insert(r1)
	r2 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test2.php"}
	gp.Insert(r2)
	r3 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test3.php"}
	gp.Insert(r3)
	r4 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/test.php"}
	gp.Insert(r4)
	r5 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "path": "/test.php"}
	gp.Insert(r5)
	r6 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.3", "path": "/test.php"}
	gp.Insert(r6)

	gp2 := NewGroup("domain", []string{}, []string{})
	r21 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test.php"}
	gp2.Insert(r21)
	r22 := map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "path": "/test2.php"}
	gp2.Insert(r22)

	gp1size := gp.GroupSize

	gp.Join(gp2)
	if gp.GroupSize != gp1size+gp2.GroupSize {
		t.Error("group join size error", gp.GroupSize)
	}
	agp := gp.GroupBy([]string{"ip"}, []string{}, []string{})
	top1 := agp.OrderbyTopN(1)
	if len(top1) != 1 {
		t.Error("group join assemblegroup orderby error")
	}
	if len(top1[0].Rows) != 5 || "192.168.1.1" != top1[0].Rows[0]["ip"] {
		t.Error("group join assemblegroup rows error", len(top1[0].Rows))
	}
}

func TestRowsOrderby(t *testing.T) {
	rs := NewRows()
	rs.InsertRow(map[string]string{"domain": "liwq.com", "ip": "192.168.1.3", "test_order": "0"})
	rs = append(rs, map[string]string{"domain": "liwq.com", "ip": "192.168.1.2", "test_order": "1"})
	rs = append(rs, map[string]string{"domain": "liwq.com", "ip": "192.168.1.1", "test_order": "2"})
	rs.Orderby("test_order")
	for k, v := range rs {
		n, e := strconv.ParseInt(v["test_order"], 10, 64)
		if e != nil || 2-k != int(n) {
			t.Error("test order by error")
		}
	}
}
