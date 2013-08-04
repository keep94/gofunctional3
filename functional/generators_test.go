// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package functional

import (
    "fmt"
    "testing"
)

func TestNewInfiniteGenerator(t *testing.T) {
  // fibonacci
  fib := NewGenerator(
      func(e Emitter) error {
        a := 0
        b := 1
        for {
          var ptr interface{}
          var opened bool
          if ptr, opened = e.EmitPtr(); !opened {
            return nil
          }
          *ptr.(*int) = a
          e.Return(nil)
          a, b = b, a + b
        }
        return nil
      })
  var results []int
  stream := Slice(fib, 0, 7)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[0 1 1 2 3 5 8]"  {
    t.Errorf("Expected [0 1 1 2 3 5 8] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
  closeVerifyResult(t, stream, nil)
}

func TestNewFiniteGenerator(t *testing.T) {
  stream := NewGenerator(
      func(e Emitter) error {
        values := []int{1, 2, 5}
        for i := range values {
          var ptr interface{}
          var opened bool
          if ptr, opened = e.EmitPtr(); !opened {
            return nil
          }
          *ptr.(*int) = values[i]
          e.Return(nil)
        }
        return nil
      })
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[1 2 5]" {
    t.Errorf("Expected [1 2 5] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
  closeVerifyResult(t, stream, nil)
}

func TestEmptyGenerator(t *testing.T) {
  stream := NewGenerator(func (e Emitter) error { return nil })
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[]" {
    t.Errorf("Expected [] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
  closeVerifyResult(t, stream, nil)
}

func TestGeneratorUsingStreams(t *testing.T) {
  s1 := Slice(Count(), 0, 3)
  s2 := Slice(Count(), 10, 12)
  var openedAfterS1, openedAfterS2 bool
  stream := newConcatGenerator(s1, s2, &openedAfterS1, &openedAfterS2)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[0 1 2 10 11]" {
    t.Errorf("Expected [0 1 2 10 11] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
  closeVerifyResult(t, stream, nil)
  assertBoolEqual(t, true, openedAfterS1)
  assertBoolEqual(t, true, openedAfterS2)
}

func TestGeneratorUsingStreamClose(t *testing.T) {
  s1 := &streamCloseChecker{Slice(Count(), 0, 3), &simpleCloseChecker{}}
  s2 := &streamCloseChecker{Slice(Count(), 10, 12), &simpleCloseChecker{closeError: closeError}}
  var openedAfterS1, openedAfterS2 bool
  stream := Slice(
      newConcatGenerator(s1, s2, &openedAfterS1, &openedAfterS2), 0, 2)
  results, err := toIntArray(stream)
  if output := fmt.Sprintf("%v", results); output != "[0 1]" {
    t.Errorf("Expected [0 1] got %v", output)
  }
  verifyDone(t, stream, new(int), err)
  verifyCloseCalled(t, s1, false)
  verifyCloseCalled(t, s2, false)
  closeVerifyResult(t, stream, closeError)
  verifyCloseCalled(t, s1, true)
  verifyCloseCalled(t, s2, true)
  assertBoolEqual(t, false, openedAfterS1)
  assertBoolEqual(t, false, openedAfterS2)
}

func newConcatGenerator(
    s1, s2 Stream, openedAfterS1, openedAfterS2 *bool) Stream {
  return NewGenerator(func(e Emitter) error {
    *openedAfterS1 = EmitAll(s1, e)
    *openedAfterS2 = EmitAll(s2, e)
    e.Finalize()
    err1 := s1.Close()
    err2 := s2.Close()
    if err1 == nil {
      return err2
    }
    return err1
  })
}

func assertBoolEqual(t *testing.T, expected, actual bool) {
  if expected != actual {
    t.Errorf("Expected %v, got %v", expected, actual)
  }
}
