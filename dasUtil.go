// Copyright liwq
package dasUtil

// 数据聚合分析组件

import (
	"sort"
	"strconv"
	"strings"
)

// 单个字段的索引
type index struct {
	// 索引字段值， 行集合
	idx map[string]Rows
	// 索引字段名
	idxName string
}

func newIndex(idxName string) *index {
	return &index{
		idx:     make(map[string]Rows),
		idxName: idxName,
	}
}

func (i *index) update(val string, r Row) {
	if _, ok := i.idx[val]; !ok {
		i.idx[val] = NewRows()
	}
	i.idx[val] = append(i.idx[val], r)
}

func (i *index) get(val string) (r Rows, ok bool) {
	r, ok = i.idx[val]
	return
}

type Row map[string]string

type Rows []Row

func NewRows() Rows {
	return Rows{}
}

//数据集：插入一集数据
func (rs *Rows) Insert(rows []Row) {
	*rs = append(*rs, rows...)
}

// 插入一行数据
func (rs *Rows) InsertRow(r Row) {
	*rs = append(*rs, r)
}

type orderRows struct {
	rows      Rows
	orderItem string
}

func (rs orderRows) Len() int {
	return len(rs.rows)
}

func (rs orderRows) Less(i, j int) bool {
	ir, ei := strconv.ParseFloat(rs.rows[i][rs.orderItem], 64)
	jr, ej := strconv.ParseFloat(rs.rows[j][rs.orderItem], 64)
	if ei == nil && ej == nil {
		return ir > jr
	}
	return rs.rows[i][rs.orderItem] > rs.rows[j][rs.orderItem]
}

func (rs orderRows) Swap(i, j int) {
	rs.rows[i], rs.rows[j] = rs.rows[j], rs.rows[i]
}

// 根据某一字断降序排序
func (r Rows) Orderby(item string) {
	or := orderRows{r, item}
	sort.Sort(or)
}

type groupList []*Group

func (gl groupList) Len() int {
	return len(gl)
}

func (gl groupList) Less(i, j int) bool {
	return gl[i].GroupSize > gl[j].GroupSize
}

func (gl groupList) Swap(i, j int) {
	gl[i], gl[j] = gl[j], gl[i]
}

type groupListOrderByItem struct {
	Item      string
	GroupList groupList
}

func (gloi groupListOrderByItem) Len() int {
	return len(gloi.GroupList)
}

func (gloi groupListOrderByItem) Less(i, j int) bool {
	return gloi.GroupList[i].SumCol[gloi.Item] > gloi.GroupList[j].SumCol[gloi.Item]
}

func (gloi groupListOrderByItem) Swap(i, j int) {
	gloi.GroupList[i], gloi.GroupList[j] = gloi.GroupList[j], gloi.GroupList[i]
}

type AssembleGroup struct {
	GroupList    groupList
	groupMap     map[string]*Group
	groupCol     []string
	AllGroupSize uint
}

func newAssembleGroup(column []string) *AssembleGroup {
	return &AssembleGroup{
		GroupList: make([]*Group, 0, 8),
		groupMap:  make(map[string]*Group),
		groupCol:  column,
	}
}

//子表排序：根据group以后的子表的数据量降序排序子表
func (ag *AssembleGroup) OrderbyTopN(n int) []*Group {
	if len(ag.GroupList) == 0 {
		return make([]*Group, 0, 0)
	}

	sort.Sort(ag.GroupList)
	m := n
	l := len(ag.GroupList)
	if m > l {
		m = l
	}

	return ag.GroupList[:m]
}

//子表排序：根据group以后的子表的数据量升序排序子表
func (ag *AssembleGroup) OrderbyDescTopN(n int) []*Group {
	if len(ag.GroupList) == 0 {
		return make([]*Group, 0, 0)
	}

	sort.Sort(sort.Reverse(ag.GroupList))
	m := n
	l := len(ag.GroupList)
	if m > l {
		m = l
	}
	return ag.GroupList[:m]
}

//子表排序：根据group时的sum某个字段降序排序子表
func (ag *AssembleGroup) OrderbyItemTopN(item string, n int) []*Group {
	if len(ag.GroupList) == 0 {
		return make([]*Group, 0, 0)
	}

	glo := groupListOrderByItem{item, ag.GroupList}
	sort.Sort(glo)
	m := n
	l := len(glo.GroupList)
	if m > l {
		m = l
	}

	return glo.GroupList[:m]
}

type Group struct {
	GroupName string
	Rows      Rows
	GroupSize uint
	// 索引字段 索引
	index  map[string]*index
	SumCol map[string]uint64
}

// 新建一个集合，返回该集合的引用
// groupName 结合的名字，一般是这个集合所具有的相同的属性
// idxColumn 索引字段，为可能会进行的Select、或Groupby操作
// sumColumn 求和字段，累加集合所有row的该column
func NewGroup(groupName string, idxColumn []string, sumColumn []string) *Group {
	gp := &Group{
		GroupName: groupName,
		Rows:      NewRows(),
		index:     make(map[string]*index),
	}
	for _, col := range idxColumn {
		gp.index[col] = newIndex(col)
	}

	gp.SumCol = make(map[string]uint64, len(sumColumn))
	for _, col := range sumColumn {
		gp.SumCol[col] = 0
	}
	return gp
}

// 添加一行数据到集合
func (gp *Group) Insert(r Row) {
	gp.Rows = append(gp.Rows, r)
	gp.GroupSize += 1

	if len(gp.index) > 0 {
		for col, val := range r {
			if idx, ok := gp.index[col]; ok {
				idx.update(val, r)
			}
		}
	}

	if len(gp.SumCol) > 0 {
		for col, _ := range gp.SumCol {
			if rv, ok := r[col]; ok {
				rvFloat64, err := strconv.ParseFloat(rv, 64)
				if err == nil {
					gp.SumCol[col] += uint64(rvFloat64)
				}
			}
		}
	}
}

// 从集合中查找符合某些条件的数据，强依赖创建集合时的索引
func (gp *Group) Select(item map[string]string) Rows {
	rows := NewRows()
	for col, val := range item {
		if idx, ok := gp.index[col]; ok {
			if rs, ok := idx.get(val); ok {
				rows.Insert(rs)
			}
		}
	}
	return rows
}

// 根据某一组字段分组，返回为子表的集合
// columns 分组字段
// idxColumn 子表索引
// sumColumn 子表累加字段
func (gp *Group) GroupBy(columns []string, idxColumn []string, sumColumn []string) *AssembleGroup {
	ag := newAssembleGroup(columns)
	ag.AllGroupSize = gp.GroupSize
	for _, row := range gp.Rows {
		keys := ""
		for _, col := range columns {
			keys += row[col] + "-"
		}
		keys = strings.TrimRight(keys, "-")
		if keys == "" {
			continue
		}

		group, ok := ag.groupMap[keys]
		if !ok {
			group = NewGroup(keys, idxColumn, sumColumn)
			ag.GroupList = append(ag.GroupList, group)
			ag.groupMap[keys] = group
		}
		group.Insert(row)
	}
	return ag
}

// 合并两个集合
func (gp *Group) Join(gp2 *Group) {
	for _, r := range gp2.Rows {
		gp.Insert(r)
	}
}
