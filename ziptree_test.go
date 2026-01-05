package btree

import (
	"fmt"
	"testing"
)

func TestZipTreeG_Basic(t *testing.T) {
	tr := NewZipTreeG(func(a, b int) bool {
		return a < b
	})

	// Test Set and Get
	tr.Set(5)
	tr.Set(3)
	tr.Set(7)
	tr.Set(1)
	tr.Set(9)

	if tr.Len() != 5 {
		t.Errorf("Expected length 5, got %d", tr.Len())
	}

	// Test Get
	if val, ok := tr.Get(5); !ok || val != 5 {
		t.Errorf("Expected to get 5, got %v, %v", val, ok)
	}

	if val, ok := tr.Get(3); !ok || val != 3 {
		t.Errorf("Expected to get 3, got %v, %v", val, ok)
	}

	if val, ok := tr.Get(10); ok {
		t.Errorf("Expected not to get 10, got %v", val)
	}

	// Test Min/Max
	if min, ok := tr.Min(); !ok || min != 1 {
		t.Errorf("Expected min 1, got %v, %v", min, ok)
	}

	if max, ok := tr.Max(); !ok || max != 9 {
		t.Errorf("Expected max 9, got %v, %v", max, ok)
	}

	// Test Delete
	if val, ok := tr.Delete(3); !ok || val != 3 {
		t.Errorf("Expected to delete 3, got %v, %v", val, ok)
	}

	if tr.Len() != 4 {
		t.Errorf("Expected length 4 after delete, got %d", tr.Len())
	}

	if _, ok := tr.Get(3); ok {
		t.Errorf("Expected 3 to be deleted")
	}

	// Test Clear
	tr.Clear()
	if tr.Len() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", tr.Len())
	}

	if _, ok := tr.Get(5); ok {
		t.Errorf("Expected all items to be cleared")
	}
}

func TestZipTreeG_Scan(t *testing.T) {
	tr := NewZipTreeG(func(a, b int) bool {
		return a < b
	})

	items := []int{5, 3, 7, 1, 9, 2, 8, 4, 6}
	for _, item := range items {
		tr.Set(item)
	}

	// Test Scan
	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	idx := 0
	tr.Scan(func(item int) bool {
		if item != expected[idx] {
			t.Errorf("Expected %d at index %d, got %d", expected[idx], idx, item)
		}
		idx++
		return true
	})

	if idx != len(expected) {
		t.Errorf("Expected to scan %d items, scanned %d", len(expected), idx)
	}

	// Test Ascend
	idx = 0
	tr.Ascend(4, func(item int) bool {
		if idx == 0 && item != 4 {
			t.Errorf("Expected first item 4, got %d", item)
		}
		idx++
		return true
	})

	// Test Descend
	idx = 0
	tr.Descend(6, func(item int) bool {
		if idx == 0 && item != 6 {
			t.Errorf("Expected first item 6, got %d", item)
		}
		idx++
		return true
	})
}

func TestZipTreeG_PopMinMax(t *testing.T) {
	tr := NewZipTreeG(func(a, b int) bool {
		return a < b
	})

	items := []int{5, 3, 7, 1, 9}
	for _, item := range items {
		tr.Set(item)
	}

	// Test PopMin
	if val, ok := tr.PopMin(); !ok || val != 1 {
		t.Errorf("Expected PopMin to return 1, got %v, %v", val, ok)
	}

	if tr.Len() != 4 {
		t.Errorf("Expected length 4 after PopMin, got %d", tr.Len())
	}

	// Test PopMax
	if val, ok := tr.PopMax(); !ok || val != 9 {
		t.Errorf("Expected PopMax to return 9, got %v, %v", val, ok)
	}

	if tr.Len() != 3 {
		t.Errorf("Expected length 3 after PopMax, got %d", tr.Len())
	}

	// Verify remaining items
	expected := []int{3, 5, 7}
	idx := 0
	tr.Scan(func(item int) bool {
		if item != expected[idx] {
			t.Errorf("Expected %d at index %d, got %d", expected[idx], idx, item)
		}
		idx++
		return true
	})
}

func TestZipTreeG_CopyOnWrite(t *testing.T) {
	tr1 := NewZipTreeG(func(a, b int) bool {
		return a < b
	})

	tr1.Set(5)
	tr1.Set(3)
	tr1.Set(7)

	// Create a copy
	tr2 := tr1.Copy()

	// Modify original
	tr1.Set(9)

	// Verify copy is unchanged
	if tr2.Len() != 3 {
		t.Errorf("Expected copy length 3, got %d", tr2.Len())
	}

	if _, ok := tr2.Get(9); ok {
		t.Errorf("Expected copy not to have 9")
	}

	// Modify copy
	tr2.Set(2)

	// Verify original is unchanged
	if tr1.Len() != 4 {
		t.Errorf("Expected original length 4, got %d", tr1.Len())
	}

	if _, ok := tr1.Get(2); ok {
		t.Errorf("Expected original not to have 2")
	}
}

func TestZipTreeG_Replace(t *testing.T) {
	// For this test, we'll use a custom type to test replacement
	type item struct {
		key   int
		value string
	}

	tr := NewZipTreeG(func(a, b item) bool {
		return a.key < b.key
	})

	tr.Set(item{1, "a"})
	tr.Set(item{2, "b"})
	tr.Set(item{3, "c"})

	// Replace value for key 2
	prev, replaced := tr.Set(item{2, "b2"})
	if !replaced {
		t.Errorf("Expected replacement to succeed")
	}
	if prev.key != 2 || prev.value != "b" {
		t.Errorf("Expected previous value {2 b}, got %v", prev)
	}

	// Verify new value
	if val, ok := tr.Get(item{2, ""}); !ok || val.value != "b2" {
		t.Errorf("Expected new value b2, got %v", val)
	}
}

func TestZipTreeG_Empty(t *testing.T) {
	tr := NewZipTreeG(func(a, b int) bool {
		return a < b
	})

	// Test operations on empty tree
	if tr.Len() != 0 {
		t.Errorf("Expected empty tree length 0, got %d", tr.Len())
	}

	if _, ok := tr.Min(); ok {
		t.Errorf("Expected Min to fail on empty tree")
	}

	if _, ok := tr.Max(); ok {
		t.Errorf("Expected Max to fail on empty tree")
	}

	if _, ok := tr.PopMin(); ok {
		t.Errorf("Expected PopMin to fail on empty tree")
	}

	if _, ok := tr.PopMax(); ok {
		t.Errorf("Expected PopMax to fail on empty tree")
	}

	if _, ok := tr.Delete(5); ok {
		t.Errorf("Expected Delete to fail on empty tree")
	}
}

func ExampleZipTreeG() {
	tr := NewZipTreeG(func(a, b string) bool {
		return a < b
	})

	tr.Set("apple")
	tr.Set("banana")
	tr.Set("cherry")

	fmt.Println("Tree length:", tr.Len())

	val, ok := tr.Get("banana")
	fmt.Println("Got banana:", val, ok)

	tr.Scan(func(item string) bool {
		fmt.Println("Item:", item)
		return true
	})

	// Output:
	// Tree length: 3
	// Got banana: banana true
	// Item: apple
	// Item: banana
	// Item: cherry
}