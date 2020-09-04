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
	buffer  reflect.Value
	handler elementHandler
	idx     int
}

// NewBuffer creates a new Buffer. aSlice is a []T used to store values.
func NewBuffer(aSlice interface{}) *Buffer {
	return &Buffer{buffer: sliceValue(aSlice, false), handler: valueHandler{}}
}

// NewPtrBuffer creates a new Buffer. aSlice is a []*T used to store values.
// If a *T value in aSlice is nil, the Consume method will replace it using
// new(T).
func NewPtrBuffer(aSlice interface{}) *Buffer {
	aSliceValue := sliceValue(aSlice, true)
	return &Buffer{
		buffer: aSliceValue,
		handler: overwriteNilPtrHandler{
			creater: newCreaterFunc(nil, aSliceValue.Type())}}
}

// Values returns the values gathered from the last Consume call. The number of
// values gathered will not exceed the length of the original slice passed
// to NewBuffer. Returned value is a []T or []*T depending on whether
// NewBuffer or NewPtrBuffer was used to create this instance. Returned
// slice, and each pointer in slice if a []*T, remains valid until the next
// call to Consume.
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

// AppendTo appends the values from a Stream of T to an existing []T.
// aSlicePointer points to the []T which is updated in place.
func AppendTo(aSlicePointer interface{}) functional.Consumer {
	aSliceValue := sliceValueFromP(aSlicePointer, false)
	return &appendConsumer{buffer: aSliceValue, handler: valueHandler{}}
}

// AppendPtrsTo appends the values from a Stream of T to an existing []*T.
// aSlicePointer points to the []*T which is updated in place.
// creater allocates space to store one T value. nil means new(T).
func AppendPtrsTo(
	aSlicePointer interface{}, creater functional.Creater) functional.Consumer {
	aSliceValue := sliceValueFromP(aSlicePointer, true)
	aSliceType := aSliceValue.Type()
	return &appendConsumer{
		buffer: aSliceValue,
		handler: overwritePtrHandler{
			creater: newCreaterFunc(creater, aSliceType),
		},
	}
}

// DEPRECATED: see AppendTo and AppendPtrsTo.
// GrowingBuffer reads values from a Stream of T until the stream is exausted.
// GrowingBuffer grows as needed to hold all the read values.
type GrowingBuffer struct {
	bufferPtr   reflect.Value
	creater     functional.Creater
	isPtrBuffer bool
}

// DEPRECATED: see AppendTo.
// NewGrowingBuffer creates a new GrowingBuffer that stores the read values
// as a []T. aSlice is a []T and should be nil. GrowingBuffer uses aSlice to
// make slices internally via reflection. length is a hint for how many values
// will be read and must be non negative.
func NewGrowingBuffer(aSlice interface{}, length int) *GrowingBuffer {
	return newGrowingBuffer(
		sliceType(aSlice, false),
		nil,
		false,
		length)
}

// DEPRECATED: see AppendPtrsTo.
// NewPtrGrowingBuffer creates a new GrowingBuffer that stores the read values
// as a []*T. aSlice is a []*T and should be nil. GrowingBuffer uses aSlice to
// make slices internally via reflection. length is a hint for how many values
// will be read and must be non negative. creater allocates space to store
// one T value. nil means new(T).
func NewPtrGrowingBuffer(
	aSlice interface{},
	length int,
	creater functional.Creater) *GrowingBuffer {
	return newGrowingBuffer(
		sliceType(aSlice, false),
		creater,
		true,
		length)
}

func newGrowingBuffer(
	aSliceType reflect.Type,
	creater functional.Creater,
	isPtrBuffer bool,
	length int) *GrowingBuffer {
	if length < 0 {
		panic("length must be non negative")
	}
	aSlicePtr := reflect.New(aSliceType)
	aSlicePtr.Elem().Set(reflect.MakeSlice(aSliceType, 0, length+1))
	result := &GrowingBuffer{
		bufferPtr:   aSlicePtr,
		creater:     creater,
		isPtrBuffer: isPtrBuffer}
	return result
}

// DEPRECATED: see AppendTo and AppendPtrsTo.
// Consume fetches the values. s is a Stream of T.
func (g *GrowingBuffer) Consume(s functional.Stream) error {
	buffer := g.bufferPtr.Elem()
	buffer.Set(buffer.Slice(0, 0))
	if g.isPtrBuffer {
		return AppendPtrsTo(g.bufferPtr.Interface(), g.creater).Consume(s)
	}
	return AppendTo(g.bufferPtr.Interface()).Consume(s)
}

// DEPRECATED: see AppendTo and AppendPtrsTo.
// Values returns the values gathered from the last Consume call.
// Returned value is a []T or []*T depending on whether
// NewGrowingBuffer or NewPtrGrowingBuffer was used to create this instance.
// Returned slice remains valid until the next call to Consume.
func (g *GrowingBuffer) Values() interface{} {
	return g.bufferPtr.Elem().Interface()
}

// PageBuffer reads a page of T values from a stream of T.
type PageBuffer struct {
	buffer          reflect.Value
	handler         elementHandler
	desired_page_no int
	pageLen         int
	page_no         int
	is_end          bool
	idx             int
}

// NewPageBuffer returns a new PageBuffer instance.
// aSlice is a []T whose length is double that of each page;
// desiredPageNo is the desired 0-based page number. NewPageBuffer panics
// if the length of aSlice is odd.
func NewPageBuffer(aSlice interface{}, desiredPageNo int) *PageBuffer {
	return newPageBuffer(
		sliceValue(aSlice, false),
		desiredPageNo,
		valueHandler{})
}

// NewPtrPageBuffer returns a new PageBuffer instance.
// aSlice is a []*T whose length is double that of each page;
// desiredPageNo is the desired 0-based page number. NewPageBuffer panics
// if the length of aSlice is odd. If a *T value in aSlice is nil,
// the Consume method will replace it using new(T).
func NewPtrPageBuffer(aSlice interface{}, desiredPageNo int) *PageBuffer {
	aSliceValue := sliceValue(aSlice, true)
	return newPageBuffer(
		aSliceValue,
		desiredPageNo,
		overwriteNilPtrHandler{
			creater: newCreaterFunc(nil, aSliceValue.Type())})
}

func newPageBuffer(
	aSlice reflect.Value,
	desiredPageNo int,
	handler elementHandler) *PageBuffer {
	l := aSlice.Len()
	if l%2 == 1 {
		panic("Slice passed to NewPageBuffer must have even length.")
	}
	if l == 0 {
		panic("Slice passed to NewPageBuffer must have non-zero length.")
	}
	return &PageBuffer{
		buffer:          aSlice,
		handler:         handler,
		desired_page_no: desiredPageNo,
		pageLen:         l / 2}
}

// Values returns the values of the fetched page as a []T or a []*T depending
// on whether NewPageBuffer or NewPtrPageBuffer was used to create this
// instance. Returned slice, and each pointer in slice if a []*T, is valid
// until next call to Consume.
func (pb *PageBuffer) Values() interface{} {
	offset := pb.pageOffset(pb.page_no)
	return pb.buffer.Slice(offset, offset+pb.idx).Interface()
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
			s, pb.buffer.Slice(offset, offset+pb.pageLen), pb.handler)
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

type appendConsumer struct {
	buffer  reflect.Value
	handler elementHandler
}

func (c *appendConsumer) Consume(s functional.Stream) (err error) {
	bufCap := c.buffer.Cap()
	bufLen := c.buffer.Len()
	for err == nil {
		if bufLen == bufCap {
			bufCap = 2*bufCap + 1
			c.buffer.Set(ensureCapacity(c.buffer, bufCap))
		}
		var numRead int
		numRead, err = readStreamIntoSlice(s, c.buffer.Slice(bufLen, bufCap), c.handler)
		bufLen += numRead
		c.buffer.Set(c.buffer.Slice(0, bufLen))
	}
	if err == functional.Done {
		err = nil
	}
	return
}

func newCreaterFunc(
	creater functional.Creater, aSliceType reflect.Type) func() reflect.Value {
	if creater == nil {
		ttype := aSliceType.Elem().Elem()
		return func() reflect.Value {
			return reflect.New(ttype)
		}
	}
	return func() reflect.Value {
		return reflect.ValueOf(creater())
	}
}

type elementHandler interface {
	toInterface(value reflect.Value) interface{}
	ensureValid(value reflect.Value)
}

type valueHandler struct {
}

func (v valueHandler) toInterface(value reflect.Value) interface{} {
	return value.Addr().Interface()
}

func (v valueHandler) ensureValid(value reflect.Value) {
}

type ptrHandler struct {
}

func (p ptrHandler) toInterface(value reflect.Value) interface{} {
	return value.Interface()
}

type overwriteNilPtrHandler struct {
	ptrHandler
	creater func() reflect.Value
}

func (p overwriteNilPtrHandler) ensureValid(value reflect.Value) {
	if value.IsNil() {
		value.Set(p.creater())
	}
}

type overwritePtrHandler struct {
	ptrHandler
	creater func() reflect.Value
}

func (p overwritePtrHandler) ensureValid(value reflect.Value) {
	value.Set(p.creater())
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
	return checkSliceValue(reflect.ValueOf(aSlice), sliceOfPtrs)
}

func sliceValueFromP(
	aSlicePointer interface{}, sliceOfPtrs bool) reflect.Value {
	resultPtr := reflect.ValueOf(aSlicePointer)
	if resultPtr.Type().Kind() != reflect.Ptr {
		panic("A pointer to a slice is expected.")
	}
	return checkSliceValue(resultPtr.Elem(), sliceOfPtrs)
}

func checkSliceValue(
	result reflect.Value, sliceOfPtrs bool) reflect.Value {
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

func ensureCapacity(
	aSlice reflect.Value, capacity int) reflect.Value {
	result := reflect.MakeSlice(aSlice.Type(), aSlice.Len(), capacity)
	reflect.Copy(result, aSlice)
	return result
}
