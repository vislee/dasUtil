/*
 Copyright liwq
 the data aggregation and select util package.
*/

// Package dasUtil provides the data aggregation and select util.
//
// 数据的聚合查询组件
package dasUtil

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// index is a column index.
type index struct {
	// 索引字段值， 行引用集合
	idx map[string]Rows

	// 索引字段名
	idxName string

	sync.RWMutex
}

func newIndex(idxName string) *index {
	return &index{
		idx:     make(map[string]Rows),
		idxName: idxName,
	}
}

func (i *index) update(val string, r *Row) {
	i.Lock()
	if _, ok := i.idx[val]; !ok {
		i.idx[val] = NewRows()
	}
	i.idx[val] = append(i.idx[val], r)
	i.Unlock()
}

func (i *index) get(val string) (r Rows, ok bool) {
	i.RLock()
	r, ok = i.idx[val]
	i.RUnlock()
	return
}


// Row is a data record, save in the form of the key-val.
//
// 一条数据记录，以key-val的形式存储在内存中。
type Row struct {
	colsmap map[string]string
	cites   uint32
	sync.RWMutex
}


var rowPool = &sync.Pool{
	New: func() interface{} {
		var r *Row = new(Row)
		r.cites = 0
		r.colsmap = make(map[string]string, 16)
		return r
	},
}


// NewRow returns an empty Row struct.
//
// 返回一个空的Row结构。
func NewRow() *Row {
	r := rowPool.Get().(*Row)
	return r
}

// NewColsRow returns an Row.
//
// 返回一个Row结构体，其内容为输入的字典。
func NewColsRow(c map[string]string) *Row {
	r := rowPool.Get().(*Row)
	if c != nil {
		r.colsmap = c
	}
	return r
}


// destroy returns this row back to the pool.
func (r *Row) destroy() {
	r.Lock()
	defer r.Unlock()
	r.cites--
	if r.cites > 0 {
		return
	}
	for k, _ := range r.colsmap {
		delete(r.colsmap, k)
	}

	rowPool.Put(r)
}

// Get returns the value of the key.
//
// 返回key对应的值。
func (r *Row) Get(key string) (v string, ok bool) {
	r.RLock()
	v, ok = r.colsmap[key]
	r.RUnlock()
	return
}

// GetNull returns the value of the key, otherwise empty string.
//
// 返回key对应的值，不存在则返回空字符串。
func (r *Row) GetNull(key string) string {
	r.RLock()
	v, ok := r.colsmap[key]
	if !ok {
		v = ""
	}
	r.RUnlock()
	return v
}


// GetDefault returns the value of the key, otherwise default string.
//
// 返回key对应的值，不存在则返回缺省字符串。
func (r *Row) GetDefault(key, d string) string {
	r.RLock()
	v, ok := r.colsmap[key]
	if !ok {
		v = d
	}
	r.RUnlock()
	return v
}


// Set add a key and value.
//
// 添加一个字段和对应的值。
func (r *Row) Set(key, val string) {
	r.Lock()
	r.colsmap[key] = val
	r.Unlock()
}

// JsonParse Parsing the json string fill the Row.
//
// 解析json字符串填充该Row结构体。
func (r *Row) JsonParse(js *string) error {
	r.Lock()
	defer r.Unlock()
	return json.Unmarshal([]byte(*js), &r.colsmap)
}

// SplitParse split the string s fill the Row. sep is the delimiter, cols is collection for key.
//
// 以sep分割字符串s，填充该Row结构。cols为对应字段的key。
func (r *Row) SplitParse(s, sep *string, cols []string) {
	r.Lock()
	itms := strings.SplitN(*s, *sep, len(cols))
	ltm := len(itms)
	for i, k := range cols {
		if k == "nil" || k == "null" || k == "NIL" || k == "Nil" {
			continue
		}
		if i >= ltm {
			r.colsmap[k] = ""
		} else {
			r.colsmap[k] = itms[i]
		}
	}
	r.Unlock()
}


// Rows the collection of Row. The multiple log record.
//
// Row的集合，多条日志记录。
type Rows []*Row

// NewRows returns the Rows.
//
// 返回一个日志集合。
func NewRows() Rows {
	return Rows{}
}

//数据集：插入一集数据
func (rs *Rows) insert(rows []*Row) {
	*rs = append(*rs, rows...)
}


// InsertRow insert the Row.
//
// InsertRow 插入一行数据.
func (rs *Rows) InsertRow(r *Row) {
	*rs = append(*rs, r)
}


// orderRows 有序行集合。
type orderRows struct {
	rows      Rows
	orderItem string
}

func (rs orderRows) Len() int {
	return len(rs.rows)
}

func (rs orderRows) Less(i, j int) bool {
	ir, ei := strconv.ParseFloat(rs.rows[i].GetNull(rs.orderItem), 64)
	jr, ej := strconv.ParseFloat(rs.rows[j].GetNull(rs.orderItem), 64)
	if ei == nil && ej == nil {
		return ir > jr
	}
	return rs.rows[i].GetNull(rs.orderItem) > rs.rows[j].GetNull(rs.orderItem)
}

func (rs orderRows) Swap(i, j int) {
	rs.rows[i], rs.rows[j] = rs.rows[j], rs.rows[i]
}


// Orderby with item descending order.
//
// Orderby 根据item降序排序.
func (rs Rows) Orderby(item string) {
	or := orderRows{rs, item}
	sort.Sort(or)
}


// 表集合
type tableList []*Table


func (tl tableList) Len() int {
	return len(tl)
}

func (tl tableList) Less(i, j int) bool {
	return tl[i].TabSize > tl[j].TabSize
}

func (tl tableList) Swap(i, j int) {
	tl[i], tl[j] = tl[j], tl[i]
}

type groupTableOrderByItem struct {
	Item       string
	GrptabList tableList
}

func (t groupTableOrderByItem) Len() int {
	return len(t.GrptabList)
}

func (t groupTableOrderByItem) Less(i, j int) bool {
	return t.GrptabList[i].SumCol[t.Item] > t.GrptabList[j].SumCol[t.Item]
}

func (t groupTableOrderByItem) Swap(i, j int) {
	t.GrptabList[i], t.GrptabList[j] = t.GrptabList[j], t.GrptabList[i]
}


// GroupTable a group of table.
//
// 表集合
type GroupTable struct {
	// child table list
	GrptabList tableList

	groupTable map[string]*Table
	groupCol   []string

	// parent table size
	ParentSize uint
}

var groupTablePool = &sync.Pool{
	New: func() interface{} {
		return new(GroupTable)
	},
}

func newGroupTable(col []string) *GroupTable {
	gt, ok := groupTablePool.Get().(*GroupTable)
	if !ok {
		gt = new(GroupTable)
	}
	gt.GrptabList = make([]*Table, 0, 8)
	gt.groupTable = make(map[string]*Table, 3)
	gt.groupCol = col
	return gt
}


// Destroy destroy the a group of table.
//
// 销毁一组表集合。
func (g *GroupTable) Destroy() {
	for _, k := range g.GrptabList {
		k.Destroy()
	}
	g.GrptabList = nil
	g.groupTable = nil
	g.groupCol = nil
	groupTablePool.Put(g)
}


// OrderbyTopN
//
//子表排序，根据group以后的子表的数据量降序排序子表
func (g *GroupTable) OrderbyTopN(n int) []*Table {
	if len(g.GrptabList) == 0 {
		return make([]*Table, 0, 0)
	}

	sort.Sort(g.GrptabList)
	m := n
	if m > len(g.GrptabList) {
		m = len(g.GrptabList)
	}

	return g.GrptabList[:m]
}


// OrderbyDescTopN
//
//子表排序，根据group以后的子表的数据量升序排序子表
func (g *GroupTable) OrderbyDescTopN(n int) []*Table {
	if len(g.GrptabList) == 0 {
		return make([]*Table, 0, 0)
	}

	sort.Sort(sort.Reverse(g.GrptabList))
	m := n
	if m > len(g.GrptabList) {
		m = len(g.GrptabList)
	}
	return g.GrptabList[:m]
}

// OrderbyItemTopN
//
//子表排序，根据group时的sum某个字段降序排序子表
func (g *GroupTable) OrderbyItemTopN(item string, n int) []*Table {
	if len(g.GrptabList) == 0 {
		return make([]*Table, 0, 0)
	}

	gto := groupTableOrderByItem{item, g.GrptabList}
	sort.Sort(gto)
	m := n
	if m > len(gto.GrptabList) {
		m = len(gto.GrptabList)
	}

	return gto.GrptabList[:m]
}


// Table a set of data collection.
//
// 一组数据的集合。
type Table struct {
	// The name of this set.
	TabName string
	Rows    Rows
	// size for this set.
	TabSize uint
	// the index for the Rows.
	index     map[string]*index
	// the sum of a columns.
	SumCol    map[string]float64
	// the count of value is equal to of val in this column.
	// you can get the count of the 'key:val'.
	CountCol  map[string]uint64
	// update timestamp.
	TimeStamp int64
	sync.RWMutex
}

var tablePool = sync.Pool{
	New: func() interface{} {
		return new(Table)
	},
}

// NewTable returns a table.
//
// 创建一个table。name是table的名称。idxCol是索引字段。sumCol是求和字段。countCol是字段对应的值的次数统计。
func NewTable(name string, idxCol []string, sumCol []string, countCol map[string][]string) *Table {
	tb, ok := tablePool.Get().(*Table)
	if !ok {
		tb = new(Table)
	}
	tb.TimeStamp = time.Now().Unix()
	tb.TabName = name
	tb.TabSize = 0
	tb.Rows = NewRows()
	tb.Lock()
	tb.index = make(map[string]*index)
	for _, col := range idxCol {
		tb.index[col] = newIndex(col)
	}

	tb.SumCol = make(map[string]float64, len(sumCol))
	for _, col := range sumCol {
		tb.SumCol[col] = 0
	}

	tb.CountCol = make(map[string]uint64, 4)
	if countCol != nil {
		for k, vals := range countCol {
			for _, v := range vals {
				tb.CountCol[fmt.Sprintf("%s:%s", k, v)] = 0
			}
		}
	}

	tb.Unlock()

	return tb
}


// Destroy destroy the table.
//
// 销毁一个table。
func (tb *Table) Destroy() {
	tb.RLock()
	defer tb.RUnlock()
	for _, r := range tb.Rows {
		r.destroy()
	}
	tb.TimeStamp = 0
	tb.TabName = ""
	tb.TabSize = 0
	tb.Rows = nil
	tb.index = nil
	tb.SumCol = nil

	tablePool.Put(tb)
}


// Insert insert a Row into a Table.
//
// 插入一行数据到一个表。
func (tb *Table) Insert(r *Row) {
	tb.Lock()
	tb.Rows = append(tb.Rows, r)
	r.Lock()
	r.cites++
	r.Unlock()

	tb.TabSize += 1
	t, err := time.Parse("02/Jan/2006:15:04:05 -0700",
		strings.TrimFunc(r.GetNull("time"), func(c rune) bool {
			if c == '[' || c == ']' {
				return true
			}
			return false
		}))
	if err == nil && t.Unix() > 0 {
		tb.TimeStamp = t.Unix()
	}

	if len(tb.index) > 0 {
		r.RLock()
		for col, val := range r.colsmap {
			if idx, ok := tb.index[col]; ok {
				idx.update(val, r)
			}
		}
		r.RUnlock()
	}

	if len(tb.SumCol) > 0 {
		for col, _ := range tb.SumCol {
			if rv, ok := r.Get(col); ok {
				rvFloat64, err := strconv.ParseFloat(rv, 64)
				if err == nil {
					tb.SumCol[col] += rvFloat64
				}
			}
		}
	}

	if len(tb.CountCol) > 0 {
		for colv, _ := range tb.CountCol {
			colVal := strings.SplitN(colv, ":", 2)
			if len(colVal) == 2 {
				if val, ok := r.Get(colVal[0]); ok && (val == colVal[1] || strings.HasPrefix(val, strings.TrimRight(colVal[1], "x"))) {
					tb.CountCol[colv]++
				}
			}
		}
	}
	tb.Unlock()
}


// Select query a Rows from this Table.
//
// 查询符合条件的数据
func (tb *Table) Select(item map[string]string) Rows {
	rows := NewRows()
	for col, val := range item {
		if idx, ok := tb.index[col]; ok {
			if rs, ok := idx.get(val); ok {
				rows.insert(rs)
			}
		}
	}
	return rows
}


// GroupBy according to a set of fields group, returned to the GroupTable.
//
// 根据某一组字段分组，返回为子表的集合
//
//
// columns 分组字段
//
// index 子表索引
//
// sum 子表累加字段
//
// count 字表复合条件的记录数
func (tb *Table) GroupBy(columns []string, index []string, sum []string, count map[string][]string) *GroupTable {
	gt := newGroupTable(columns)
	tb.RLock()
	gt.ParentSize = tb.TabSize
	for _, row := range tb.Rows {
		keys := ""
		for _, col := range columns {
			keys += row.GetNull(col) + "-"
		}
		keys = strings.TrimRight(keys, "-")
		if keys == "" {
			continue
		}

		tab, ok := gt.groupTable[keys]
		if !ok {
			// todo 优化heap
			tab = NewTable(keys, index, sum, count)
			gt.GrptabList = append(gt.GrptabList, tab)
			gt.groupTable[keys] = tab
		}
		tab.Insert(row)
	}
	tb.RUnlock()
	return gt
}


// Join merge tb2 into this table.
//
// 合并两个表
func (tb *Table) Join(tb2 *Table) {
	for _, r := range tb2.Rows {
		tb.Insert(r)
	}
}
