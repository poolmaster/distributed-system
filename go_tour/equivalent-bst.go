package main

import "fmt"

type Tree struct {
  Left *Tree
  Value int
  Right *Tree
}

// Walk walks the tree t sending all values
// from the tree to the channel ch.
func Walk(t *Tree, ch chan int) {
  if t == nil { return }
  Walk(t.Left, ch)
  ch <- t.Value
  Walk(t.Right, ch)
}

// Same determines whether the trees
// t1 and t2 contain the same values.
func Same(t1, t2 *Tree) bool {
  ch1 := make(chan int)
  ch2 := make(chan int)
  go func() {
    defer close(ch1)
    Walk(t1, ch1)
  }()
  go func() {
    defer close(ch2)
    Walk(t2, ch2)
  }()
  for {
    v1, ok1 := <- ch1
    v2, ok2 := <- ch2
    if !ok1 && !ok2 { 
      break
    } else if ok1 != ok2 || v1 != v2 {
      return false
    }
  }
  return true
}

func main() {
}



