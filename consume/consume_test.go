// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package consume

import (
  "errors"
  "github.com/keep94/gofunctional3/functional"
  "testing"
)

var (
  emptyError = errors.New("stream_util: Empty.")
  otherError = errors.New("stream_util: Other.")
  consumerError = errors.New("stream_util: consumer error.")
  closeError = errors.New("stream_util: close error.")
  intPtrSlice []*int
  intSlice []int
)

func TestPtrBuffer(t *testing.T) {
  stream := functional.Count()
  b := NewPtrBuffer(make([]*int, 5))
  doConsume(t, b, stream, nil)
  verifyPtrFetched(t, b, 0, 5)
}

func TestPtrBufferWithCreater(t *testing.T) {
  stream := functional.Count()
  b := NewPtrBufferWithCreater(make([]*int, 5), intCreater)
  doConsume(t, b, stream, nil)
  verifyPtrFetched(t, b, 0, 5)
}

func TestBufferSameSize(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 5)
  b := NewBuffer(make([]int, 5))
  doConsume(t, b, stream, nil)
  verifyFetched(t, b, 0, 5)
}

func TestBufferSmall(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 6)
  b := NewBuffer(make([]int, 5))
  doConsume(t, b, stream, nil)
  verifyFetched(t, b, 0, 5)
}

func TestBufferBig(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 4)
  b := NewBuffer(make([]int, 5))
  doConsume(t, b, stream, nil)
  verifyFetched(t, b, 0, 4)
}

func TestBufferError(t *testing.T) {
  stream := errorStream{otherError}
  b := NewBuffer(make([]int, 5))
  doConsume(t, b, stream, otherError)
}

func TestPtrGrowingBuffer(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 6)
  b := NewPtrGrowingBuffer(intPtrSlice, 5, nil)
  doConsume(t, b, stream, nil)
  verifyPtrFetched(t, b, 0, 6)
}

func TestPtrGrowingBuffer2(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 6)
  b := NewPtrGrowingBuffer(intPtrSlice, 1, intCreater)
  doConsume(t, b, stream, nil)
  verifyPtrFetched(t, b, 0, 6)
  if actual := cap(b.Values().([]*int)); actual != 8 {
    t.Errorf("Expected capacit of 8, got %v", actual)
  }
}

func TestGrowingBufferSameSize(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 5)
  b := NewGrowingBuffer(intSlice, 5)
  doConsume(t, b, stream, nil)
  verifyFetched(t, b, 0, 5)
}

func TestGrowingBufferSmall(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 6)
  b := NewGrowingBuffer(intSlice, 5)
  doConsume(t, b, stream, nil)
  verifyFetched(t, b, 0, 6)
  if actual := cap(b.Values().([]int)); actual != 10 {
    t.Errorf("Expected capacit of 10, got %v", actual)
  }
}

func TestGrowingBufferBig(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 4)
  b := NewGrowingBuffer(intSlice, 5)
  doConsume(t, b, stream, nil)
  verifyFetched(t, b, 0, 4)
  if actual := cap(b.Values().([]int)); actual != 5 {
    t.Errorf("Expected capacity of 5, got %v", actual)
  }
}

func TestGrowingBufferError(t *testing.T) {
  stream := errorStream{otherError}
  b := NewGrowingBuffer(intSlice, 5)
  doConsume(t, b, stream, otherError)
  if actual := len(b.Values().([]int)); actual != 0 {
    t.Errorf("Expected length of 0, got %v", actual)
  }
}

func TestPtrPageBuffer(t *testing.T) {
  stream := functional.Count()
  pb := NewPtrPageBuffer(make([]*int, 6), 0)
  doConsume(t, pb, stream, nil)
  verifyPtrPageFetched(t, pb, 0, 3, 0, false)
}

func TestPtrPageBufferWithCreater(t *testing.T) {
  stream := functional.Count()
  pb := NewPtrPageBufferWithCreater(make([]*int, 6), 0, intCreater)
  doConsume(t, pb, stream, nil)
  verifyPtrPageFetched(t, pb, 0, 3, 0, false)
}

func TestPageBufferFirstPage(t *testing.T) {
  stream := functional.Count()
  pb := NewPageBuffer(make([]int, 6), 0)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 0, 3, 0, false)
}

func TestPageBufferSecondPage(t *testing.T) {
  stream := functional.Count()
  pb := NewPageBuffer(make([]int, 6), 1)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 3, 6, 1, false)
}

func TestPageBufferThirdPage(t *testing.T) {
  stream := functional.Count()
  pb := NewPageBuffer(make([]int, 6), 2)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 6, 9, 2, false)
}

func TestPageBufferNegativePage(t *testing.T) {
  stream := functional.Count()
  pb := NewPageBuffer(make([]int, 6), -1)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 0, 3, 0, false)
}

func TestPageBufferParitalThird(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 7)
  pb := NewPageBuffer(make([]int, 6), 2)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 6, 7, 2, true)
}

func TestPageBufferParitalThirdToHigh(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 7)
  pb := NewPageBuffer(make([]int, 6), 3)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 6, 7, 2, true)
}

func TestPageBufferEmptyThird(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 6)
  pb := NewPageBuffer(make([]int, 6), 2)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 3, 6, 1, true)
}

func TestPageBufferEmptyThirdTooHigh(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 6)
  pb := NewPageBuffer(make([]int, 6), 3)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 3, 6, 1, true)
}

func TestPageBufferFullSecond(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 6)
  pb := NewPageBuffer(make([]int, 6), 1)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 3, 6, 1, true)
}

func TestPageBufferParitalFirst(t *testing.T) {
  stream := functional.Slice(functional.Count(), 0, 1)
  pb := NewPageBuffer(make([]int, 6), 0)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 0, 1, 0, true)
}

func TestPageBufferEmpty(t *testing.T) {
  stream := functional.NilStream()
  pb := NewPageBuffer(make([]int, 6), 0)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 0, 0, 0, true)
}

func TestPageBufferEmptyHigh(t *testing.T) {
  stream := functional.NilStream()
  pb := NewPageBuffer(make([]int, 6), 1)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 0, 0, 0, true)
}

func TestPageBufferEmptyLow(t *testing.T) {
  stream := functional.NilStream()
  pb := NewPageBuffer(make([]int, 6), -1)
  doConsume(t, pb, stream, nil)
  verifyPageFetched(t, pb, 0, 0, 0, true)
}

func TestPageBufferError(t *testing.T) {
  stream := errorStream{otherError}
  b := NewPageBuffer(make([]int, 6), 0)
  doConsume(t, b, stream, otherError)
}

func TestFirstOnly(t *testing.T) {
  stream := functional.CountFrom(3, 1)
  var value int
  if output := FirstOnly(stream, emptyError, &value); output != nil {
    t.Errorf("Got error fetching first value, %v", output)
  }
  if value != 3 {
    t.Errorf("Expected 3, got %v", value)
  }
}

func TestFirstOnlyEmpty(t *testing.T) {
  stream := functional.NilStream()
  var value int
  if output := FirstOnly(stream, emptyError, &value); output != emptyError {
    t.Errorf("Expected emptyError, got %v", output)
  }
}

func TestFirstOnlyError(t *testing.T) {
  stream := errorStream{otherError}
  var value int
  if output := FirstOnly(stream, emptyError, &value); output != otherError {
    t.Errorf("Expected emptyError, got %v", output)
  }
}

type abstractBuffer interface {
  Values() interface{}
}

type errorStream struct {
  err error
}

func (e errorStream) Next(ptr interface{}) error {
  return e.err
}

func (e errorStream) Close() error {
  return nil
}

type closeErrorStream struct {
  functional.Stream
}

func (c closeErrorStream) Close() error {
  return closeError
}

func verifyFetched(t *testing.T, b abstractBuffer, start int, end int) {
  verifyValues(t, b.Values().([]int), start, end)
}

func verifyPtrFetched(t *testing.T, b abstractBuffer, start int, end int) {
  verifyPtrValues(t, b.Values().([]*int), start, end)
}

func verifyPageFetched(t *testing.T, pb *PageBuffer, start int, end int, page_no int, is_end bool) {
  verifyValues(t, pb.Values().([]int), start, end)
  if output := pb.PageNo(); output != page_no {
    t.Errorf("Expected page %v, got %v", page_no, output)
  }
  if output := pb.End(); output != is_end {
    t.Errorf("For end, expected %v, got %v", is_end, output)
  }
}

func verifyPtrPageFetched(t *testing.T, pb *PageBuffer, start int, end int, page_no int, is_end bool) {
  verifyPtrValues(t, pb.Values().([]*int), start, end)
  if output := pb.PageNo(); output != page_no {
    t.Errorf("Expected page %v, got %v", page_no, output)
  }
  if output := pb.End(); output != is_end {
    t.Errorf("For end, expected %v, got %v", is_end, output)
  }
}

func verifyValues(t *testing.T, values []int, start int, end int) {
  if output := len(values); output != end - start {
    t.Errorf("Expected entry array to be %v, got %v", end - start, output)
    return
  }
  for i := start; i < end; i++ {
    if output := values[i - start]; output != i {
      t.Errorf("Expected %v, got %v", i, output)
    }
  }
}

func verifyPtrValues(t *testing.T, values []*int, start int, end int) {
  if output := len(values); output != end - start {
    t.Errorf("Expected entry array to be %v, got %v", end - start, output)
    return
  }
  for i := start; i < end; i++ {
    if output := *values[i - start]; output != i {
      t.Errorf("Expected %v, got %v", i, output)
    }
  }
}

func doConsume(
    t *testing.T,
    c functional.Consumer,
    s functional.Stream,
    expectedError error) {
  if err := c.Consume(s); err != expectedError {
    t.Errorf("Expected %v, got %v", expectedError, err)
  }
}

func intCreater() interface{} {
  return new(int)
}

