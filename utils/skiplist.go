package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

//别的不多想，就先搞这个了
type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	//implement me here!!!
	return &SkipList{
		header: &Element{
			levels: make([]*Element, defaultMaxLevel),
			entry:  nil,
			score:  0,
			
		},
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel: defaultMaxLevel,
		length:   0,
		lock:     sync.RWMutex{},
		size:     0,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	//随机选择一个层级
	pickLevels := list.randLevel()
	pres := make([]*Element, list.maxLevel)
	newElement := newElement(list.calcScore(data.Key), data, pickLevels)
	header := list.header
    prev:=list.header
	maxLevel := list.maxLevel
	for i := 0; i < maxLevel; i++ {
		for curHeader := header.levels[i]; curHeader !=nil; curHeader=curHeader.levels[i] {
			if curHeader==nil ||list.compare(newElement.score,data.Key,curHeader)==-1{
				prev=curHeader
				continue
			}
		}
		pres[i]=prev
	}
    
	for i := 0; i < pickLevels; i++ {
		preElement := pres[i]
		newElement.levels[i]=preElement.levels[i]
		preElement.levels[i]=newElement
	}


	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!
	allDummyHeader := list.header
	levels := list.maxLevel

	for i := levels - 1; i >= 0; i-- {
		pre := allDummyHeader.levels[i]
		for  pre!=nil {
			if list.compare(pre.score, key, pre) == 0 {
				return pre.entry
			}
			pre = pre.levels[i]
		}
	}

	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

//通过分数判断相等，先从分数判断。如果当前分数和下一个元素分数相等时，再返回比较结果
//坐标小于右边是-1
func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	r2 := list.rand

	i := 1
	for ; i < list.maxLevel; i++ {
		if r2.Intn(i)%2 == 0 {
			return i
		}
	}
	return i
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}
