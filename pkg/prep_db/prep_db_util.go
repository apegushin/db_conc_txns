package prep_db

import (
	"iter"
	"maps"
)

// TODO: reimplement Set using generics.
// Also look up if an interface exists for a Set data structure
// that is more widely used and implement to the interface

type Set struct {
	items map[string]struct{}
}

func NewSet() *Set {
	return &Set{
		items: make(map[string]struct{}),
	}
}

func (s *Set) Add(item string) {
	s.items[item] = struct{}{}
}

func (s *Set) Contains(item string) bool {
	_, ok := s.items[item]
	return ok
}

func (s *Set) Items() iter.Seq[string] {
	return maps.Keys(s.items)
}

func (s *Set) Len() int {
	return len(s.items)
}

func (s *Set) IsEmpty() bool {
	return s.Len() == 0
}

func (s *Set) Remove(item string) {
	delete(s.items, item)
}

func (s *Set) Clear() {
	clear(s.items)
}
