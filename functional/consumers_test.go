// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package functional

import (
    "fmt"
    "testing"
)

func TestConsumersNormal(t *testing.T) {
  s := Slice(Count(), 0, 5)
  ec := newEvenNumberConsumer()
  oc := newOddNumberConsumer()
  errors := MultiConsume(s, new(int), nil, ec, oc)
  if len(errors) != 2 || errors[0] != nil || errors[1] != nil {
    t.Error("Expected no errors.")
  }
  if output := fmt.Sprintf("%v", ec.results); output != "[0 2 4]" {
    t.Errorf("Expected [0 2 4] got %v", output)
  }
  if output := fmt.Sprintf("%v", oc.results); output != "[1 3]" {
    t.Errorf("Expected [1 3] got %v", output)
  }
}

func TestConsumersEndEarly(t *testing.T) {
  first5 := func(s Stream) Stream {
    return Slice(s, 0, 5)
  }
  ec := newEvenNumberConsumer()
  oc := newOddNumberConsumer()
  nc := &noNextConsumer{}
  errors := MultiConsume(
      Count(),
      new(int),
      nil,
      nc,
      ModifyConsumerStream(ec, first5),
      ModifyConsumerStream(oc, first5))

  if len(errors) != 3 || errors[0] != nil || errors[1] != nil || errors[2] != nil {
    t.Error("Expected no errors.")
  }
  if output := fmt.Sprintf("%v", ec.results); output != "[0 2 4]" {
    t.Errorf("Expected [0 2 4] got %v", output)
  }
  if output := fmt.Sprintf("%v", oc.results); output != "[1 3]" {
    t.Errorf("Expected [1 3] got %v", output)
  }
  if !nc.completed {
    t.Error("MultiConsume returned before child consumers completed.")
  }
}

func TestNoConsumers(t *testing.T) {
  errors := MultiConsume(Count(), new(int), nil)
  if len(errors) != 0 {
    t.Errorf("Expected MultiConsume to return empty slice")
  }
}

func TestReadPastEndConsumer(t *testing.T) {
  s := Slice(Count(), 0, 5)
  rc1 := &readPastEndConsumer{}
  rc2 := &readPastEndConsumer{}
  MultiConsume(s, new(int), nil, rc1, rc2)
  if !rc1.completed || !rc2.completed {
    t.Error("MultiConsume returned before child consumers completed.")
  }
}

func TestModifyConsumerStream(t *testing.T) {
  s := &streamCloseChecker{Count(), &simpleCloseChecker{}}
  var slice *streamCloseChecker
  f := func(s Stream) Stream {
    slice = &streamCloseChecker{s, &simpleCloseChecker{}}
    return slice
  }
  mc := ModifyConsumerStream(newEvenNumberConsumer(), f)
  if err := mc.Consume(s); err != nil {
    t.Errorf("Expected no error, got %v", err)
  }
  verifyCloseCalled(t, slice, true)
  verifyCloseCalled(t, s, false)
}

func TestModifyConsumerStreamError(t *testing.T) {
  s := &streamCloseChecker{Count(), &simpleCloseChecker{}}
  var slice *streamCloseChecker
  f := func(s Stream) Stream {
    slice = &streamCloseChecker{s, &simpleCloseChecker{closeError: closeError}}
    return slice
  }
  mc := ModifyConsumerStream(newEvenNumberConsumer(), f)
  if err := mc.Consume(s); err != closeError {
    t.Errorf("Expected closeError, got %v", err)
  }
  verifyCloseCalled(t, slice, true)
  verifyCloseCalled(t, s, false)
}

type filterConsumer struct {
  f Filterer
  results []int
}

func (fc *filterConsumer) Consume(s Stream) (err error) {
  fc.results, err = toIntArray(Filter(fc.f, s))
  return
}

type readPastEndConsumer struct {
  completed bool
}

func (c *readPastEndConsumer) Consume(s Stream) (err error) {
  toIntArray(s)
  var x int
  for i := 0; i < 10; i++ {
    s.Next(&x)
  }
  c.completed = true
  return
}

type noNextConsumer struct {
  completed bool
}

func (nc *noNextConsumer) Consume(s Stream) (err error) {
  nc.completed = true
  return
}

func newEvenNumberConsumer() *filterConsumer {
  return &filterConsumer{f: NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p % 2 == 0 {
      return nil
    }
    return Skipped
  })}
}

func newOddNumberConsumer() *filterConsumer {
  return &filterConsumer{f: NewFilterer(func(ptr interface{}) error {
    p := ptr.(*int)
    if *p % 2 == 1 {
      return nil
    }
    return Skipped
  })}
}
