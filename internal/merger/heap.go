package merger

import (
	"kafka-pipeline/pkg/models"
)

type Item struct {
	Record  models.Record
	CSVLine string
	FileID  int
}

type MinHeap struct {
	Items    []Item
	LessFunc func(a, b Item) bool
}

func (h *MinHeap) Len() int { return len(h.Items) }

func (h *MinHeap) Less(i, j int) bool {
	return h.LessFunc(h.Items[i], h.Items[j])
}

func (h *MinHeap) Swap(i, j int) {
	h.Items[i], h.Items[j] = h.Items[j], h.Items[i]
}

func (h *MinHeap) Push(x interface{}) {
	h.Items = append(h.Items, x.(Item))
}

func (h *MinHeap) Pop() interface{} {
	old := h.Items
	n := len(old)
	item := old[n-1]
	h.Items = old[:n-1]
	return item
}
