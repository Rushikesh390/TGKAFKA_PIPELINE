package merger

import (
	"kafka-pipeline/pkg/models"
)

type Item struct {
	Record  models.Record
	CSVLine string // OPTIMIZATION: Cache CSV to avoid repeated formatting
	FileID  int
}

type MinHeap struct {
	Items    []Item
	LessFunc func(csvA, csvB string) bool // Now accepts CSV strings directly
}

func (h *MinHeap) Len() int { return len(h.Items) }

// Use cached CSV strings for comparison - NO formatting at comparison time!
func (h *MinHeap) Less(i, j int) bool {
	return h.LessFunc(h.Items[i].CSVLine, h.Items[j].CSVLine)
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
