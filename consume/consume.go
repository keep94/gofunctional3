// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// Package consume provides useful ways to consume streams.
package consume

import (
  "github.com/keep94/gofunctional3/functional"
  "reflect"
)

// Buffer reads T values from a Stream of T until it either fills up or
// the Stream is exhaused.
type Buffer struct {
  buffer reflect.Value
  handler elementHandler
  idx int
}

// NewBuffer creates a new Buffer. aSlice is a []T used to store values.
func NewBuffer(aSlice interface{}) *Buffer {
  return &Buffer{buffer: sliceValue(aSlice, false), handler: newValueHandler()}
}

// NewPtrBuffer creates a new Buffer. aSlice is a []*T used to store values.
// nil pinters in aSlice are replaced with new(T) on demand.
func NewPtrBuffer(aSlice interface{}) *Buffer {
  return NewPtrBufferWithCreater(aSlice, nil)
}

// NewPtrBufferWithCreater creates a new Buffer.
// aSlice is a []*T used to store values.
// creater allocates memory to store the T values. nil means new(T). 
// nil pinters in aSlice are replaced on demand.
// NewPtrBufferWithCreater is draft API, it may change in incompatible ways.
func NewPtrBufferWithCreater(
    aSlice interface{}, creater functional.Creater) *Buffer {
  aSliceValue := sliceValue(aSlice, true)
  return &Buffer{
      buffer: aSliceValue,
      handler: newPtrHandler(creater, aSliceValue.Type())}
}

// Values returns the values gathered from the last Consume call. The number of
// values gathered will not exceed the length of the original slice passed
// to NewBuffer. Returned value is a []T or []*T depending on whether
// NewBuffer or NewPtrBuffer was used to create this instance. Returned
// value remains valid until the next call to Consume.
func (b *Buffer) Values() interface{} {
  return b.buffer.Slice(0, b.idx).Interface()
}

// Consume fetches the values. s is a Stream of T.
func (b *Buffer) Consume(s functional.Stream) (err error) {
  b.idx, err = readStreamIntoSlice(s, b.buffer, b.handler)
  if err == functional.Done {
    err = nil
  }
  return
}

// GrowingBuffer reads values from a Stream of T until the stream is exausted.
// GrowingBuffer grows as needed to hold all the read values.
type GrowingBuffer struct {
  buffer reflect.Value
  sliceType reflect.Type
  handler elementHandler
  idx int
}

// NewGrowingBuffer creates a new GrowingBuffer that stores the read values
// as a []T. aSlice is a []T. Although the aSlice value is never read,
// GrowingBuffer needs it to create new slices via reflection when growing
// the buffer. initialLength is the initial size of the slice used to store
// the read values and must be greater than 0.
func NewGrowingBuffer(aSlice interface{}, initialLength int) *GrowingBuffer {
  if initialLength <= 0 {
    panic("initialLength must be greater than 0.")
  }
  result := &GrowingBuffer{
      sliceType: sliceType(aSlice, false),
      handler: newValueHandler()}
  result.buffer = result.ensureCapacity(reflect.Value{}, initialLength)
  return result
}

// NewPtrGrowingBuffer creates a new GrowingBuffer that stores the read values
// as a []*T. aSlice is a []*T. Although the aSlice value is never read,
// GrowingBuffer needs it to create new slices via reflection when growing
// the buffer. initialLength is the initial size of the slice used to store
// the read values and must be greater than 0. creater allocates memory to
// store the T values. nil means new(T).
func NewPtrGrowingBuffer(
    aSlice interface{},
    initialLength int,
    creater functional.Creater) *GrowingBuffer {
  if initialLength <= 0 {
    panic("initialLength must be greater than 0.")
  }
  sliceType := sliceType(aSlice, true)
  result := &GrowingBuffer{
      sliceType: sliceType,
      handler: newPtrHandler(creater, sliceType)}
  result.buffer = result.ensureCapacity(reflect.Value{}, initialLength)
  return result
}

// Consume fetches the values. s is a Stream of T.
func (g *GrowingBuffer) Consume(s functional.Stream) (err error) {
  g.idx = 0
  for err == nil {
    bufLen := g.buffer.Len()
    if g.idx == bufLen {
      g.buffer = g.ensureCapacity(g.buffer, 2 * bufLen)
      bufLen = g.buffer.Len()
    }
    var numRead int
    numRead, err = readStreamIntoSlice(s, g.buffer.Slice(g.idx, bufLen), g.handler)
    g.idx += numRead
  }
  if err == functional.Done {
    err = nil
  }
  return
}
  
// Values returns the values gathered from the last Consume call.
// Returned value is a []T or []*T depending on whether
// NewGrowingBuffer or NewPtrGrowingBuffer was used to create this instance.
// Returned value remains valid until the next call to Consume.
func (g *GrowingBuffer) Values() interface{} {
  return g.buffer.Slice(0, g.idx).Interface()
}

func (g *GrowingBuffer) ensureCapacity(
    aSlice reflect.Value, capacity int) reflect.Value {
  var oldLen int
  if aSlice.IsValid() {
    oldLen = aSlice.Len()
  } else {
    oldLen = 0
  }
  if capacity > oldLen {
    result := g.makeSlice(capacity)
    for i := 0; i < oldLen; i++ {
      result.Index(i).Set(aSlice.Index(i))
    }
    return result
  }
  return aSlice
}

func (g *GrowingBuffer) makeSlice(length int) reflect.Value {
  return reflect.MakeSlice(g.sliceType, length, length)
}

// PageBuffer reads a page of T values from a stream of T.
type PageBuffer struct {
  buffer reflect.Value
  handler elementHandler
  desired_page_no int
  pageLen int
  page_no int
  is_end bool
  idx int
}

// NewPageBuffer returns a new PageBuffer instance.
// aSlice is a []T whose length is double that of each page;
// desiredPageNo is the desired 0-based page number. NewPageBuffer panics
// if the length of aSlice is odd.
func NewPageBuffer(aSlice interface{}, desiredPageNo int) *PageBuffer {
  return newPageBuffer(
      sliceValue(aSlice, false),
      desiredPageNo,
      newValueHandler())
}

// NewPtrPageBuffer returns a new PageBuffer instance.
// aSlice is a []*T whose length is double that of each page;
// desiredPageNo is the desired 0-based page number. NewPageBuffer panics
// if the length of aSlice is odd.
// nil pointers in aSlice are replaced with new(T) on demand.
func NewPtrPageBuffer(aSlice interface{}, desiredPageNo int) *PageBuffer {
  return NewPtrPageBufferWithCreater(aSlice, desiredPageNo, nil)
}

// NewPtrPageBufferWithCreater returns a new PageBuffer instance.
// aSlice is a []*T whose length is double that of each page;
// desiredPageNo is the desired 0-based page number;
// creater allocates memory to store the T values. nil means new(T). 
// NewPageBuffer panics if the length of aSlice is odd.
// nil pointers in aSlice are replaced on demand.
// NewPtrPageBufferWithCreater is draft API, it may change in incompatible ways.
func NewPtrPageBufferWithCreater(
    aSlice interface{},
    desiredPageNo int,
    creater functional.Creater) *PageBuffer {
  aSliceValue := sliceValue(aSlice, true)
  return newPageBuffer(
      aSliceValue,
      desiredPageNo,
      newPtrHandler(creater, aSliceValue.Type()))
}

func newPageBuffer(
    aSlice reflect.Value,
    desiredPageNo int,
    handler elementHandler) *PageBuffer {
  l := aSlice.Len()
  if l % 2 == 1 {
    panic("Slice passed to NewPageBuffer must have even length.")
  }
  if l == 0 {
    panic("Slice passed to NewPageBuffer must have non-zero length.")
  }
  return &PageBuffer{
      buffer: aSlice,
      handler: handler,
      desired_page_no: desiredPageNo,
      pageLen: l / 2}
}

// Values returns the values of the fetched page as a []T or a []*T depending
// on whether NewPageBuffer or NewPtrPageBuffer was used to create this
// insstance. Returned slice is valid until next call to consume.
func (pb *PageBuffer) Values() interface{} {
  offset := pb.pageOffset(pb.page_no)
  return pb.buffer.Slice(offset, offset + pb.idx).Interface()
}

// PageNo returns the 0-based page number of fetched page. Note that this
// returned page number may be less than the desired page number if the
// Stream passed to Consume becomes exhaused.
func (pb *PageBuffer) PageNo() int {
  return pb.page_no
}

// End returns true if last page reached.
func (pb *PageBuffer) End() bool {
  return pb.is_end
}

// Consume fetches the values. s is a Stream of T.
func (pb *PageBuffer) Consume(s functional.Stream) (err error) {
  pb.page_no = 0
  pb.idx = 0
  pb.is_end = false
  for err == nil && !pb.isDesiredPageRead() {
    if pb.idx > 0 {
      pb.page_no++
    }
    offset := pb.pageOffset(pb.page_no)
    pb.idx, err = readStreamIntoSlice(
        s, pb.buffer.Slice(offset, offset + pb.pageLen), pb.handler)
  }
  if err == nil {
    anElement := pb.buffer.Index(pb.pageOffset(pb.page_no + 1))
    pb.handler.ensureValid(anElement)
    pb.is_end = s.Next(pb.handler.toInterface(anElement)) == functional.Done
  } else if err == functional.Done {
    pb.is_end = true
    err = nil
    if pb.page_no > 0 && pb.idx == 0 {
      pb.page_no--
      pb.idx = pb.pageLen
    }
  }
  return
}

func (pb *PageBuffer) pageOffset(pageNo int) int {
  return (pageNo % 2) * pb.pageLen
}

func (pb *PageBuffer) isDesiredPageRead() bool {
  if pb.idx == 0 {
    return false
  }
  return pb.page_no >= pb.desired_page_no
}

// FirstOnly reads the first value from stream storing it in ptr.
// FirstOnly returns emptyError if no values were on stream.
func FirstOnly(stream functional.Stream, emptyError error, ptr interface{}) (err error) {
  err = stream.Next(ptr)
  if err == functional.Done {
    err = emptyError
    return
  }
  return
}

type elementHandler interface {
  toInterface(value reflect.Value) interface{}
  ensureValid(value reflect.Value)
}

func newValueHandler() elementHandler {
  return valueHandler{}
}

func newPtrHandler(
    creater functional.Creater, aSliceType reflect.Type) elementHandler {
  if creater == nil {
    ttype := aSliceType.Elem().Elem()
    return ptrHandler(func() reflect.Value {
      return reflect.New(ttype)
    })
  }
  return ptrHandler(func() reflect.Value {
    return reflect.ValueOf(creater())
  })
}

type valueHandler struct {
}

func (v valueHandler) toInterface(value reflect.Value) interface{} {
  return value.Addr().Interface()
}

func (v valueHandler) ensureValid(value reflect.Value) {
}

type ptrHandler func() reflect.Value

func (p ptrHandler) toInterface(value reflect.Value) interface{} {
  return value.Interface()
}

func (p ptrHandler) ensureValid(value reflect.Value) {
  if value.IsNil() {
    value.Set(p())
  }
}

func readStreamIntoSlice(
     s functional.Stream,
     aSlice reflect.Value,
     handler elementHandler) (numRead int, err error) {
  l := aSlice.Len()
  for numRead = 0; numRead < l; numRead++ {
    anElement := aSlice.Index(numRead)
    handler.ensureValid(anElement)
    err = s.Next(handler.toInterface(anElement))
    if err != nil {
      break
    }
  }
  return
}

func sliceValue(aSlice interface{}, sliceOfPtrs bool) reflect.Value {
  result := reflect.ValueOf(aSlice)
  if result.Kind() != reflect.Slice {
    panic("a slice is expected.")
  }
  if sliceOfPtrs && result.Type().Elem().Kind() != reflect.Ptr {
    panic("a slice of pointers is expected.")
  }
  return result
}

func sliceType(aSlice interface{}, sliceOfPtrs bool) reflect.Type {
  result := reflect.TypeOf(aSlice)
  if result.Kind() != reflect.Slice {
    panic("a slice is expected.")
  }
  if sliceOfPtrs && result.Elem().Kind() != reflect.Ptr {
    panic("a slice of pointers is expected.")
  }
  return result
}
