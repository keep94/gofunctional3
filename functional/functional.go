// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// Package functional provides functional programming constructs.
package functional

import (
  "bufio"
  "errors"
  "io"
  "reflect"
)

var (
  // Done indicates that the end of a Stream has been reached
  Done = errors.New("functional: End of Stream reached.")
  // Filters return Skipped to indicate that the current value should be
  // skipped.
  Skipped = errors.New("functional: Value skipped.")
)

var (
  nilM = nilMapper{}
  nilPieceL = []compositeMapperPiece{{mapper: nilM}}
  nilS = nilStream{}
  trueF = trueFilterer{}
  falseF = falseFilterer{}
)

// Stream is a sequence emitted values.
// Each call to Next() emits the next value in the stream.
// A Stream that emits values of type T is a Stream of T.
type Stream interface {
  // Next emits the next value in this Stream of T.
  // If Next returns nil, the next value is stored at ptr.
  // If Next returns Done, then the end of the Stream has been reached,
  // and the value ptr points to is unspecified. ptr must be a *T.
  // Once Next returns Done, it should continue to return Done.
  Next(ptr interface{}) error
  // Caller calls Close when it is finished with this Stream. 
  // The result of calling Next after Close is unspecified.
  io.Closer
}

// Tuple represents a tuple of values that ReadRows emits
type Tuple interface {
  // Ptrs returns a pointer to each field in the tuple.
  Ptrs() []interface{}
}

// Filterer of T filters values in a Stream of T.
type Filterer interface {
  // Filter returns nil if value ptr points to should be included or Skipped
  // if value should be skipped. ptr must be a *T.
  Filter(ptr interface{}) error
}

// Mapper maps a type T value to a type U value in a Stream.
type Mapper interface {
  // Map does the mapping storing the mapped value at destPtr.
  // If Mapper returns Skipped, then no mapped value is stored at destPtr.
  // Map may return other errors. srcPtr is a *T; destPtr is a *U
  Map(srcPtr interface{}, destPtr interface{}) error
}

// CompositeMapper represents Mappers composed together e.g f(g(x)).
// Programs using CompositeMapper should typically store and pass them as
// values, not pointers. A CompositeMapper can be used by multiple goroutines
// simultaneously if its underlying Mappers can be used by multiple goroutines
// simultaneously. The zero value for CompositeMapper is a Mapper that maps
// nothing (the Map method always returns Skipped).
type CompositeMapper struct {
  _pieces []compositeMapperPiece
}

func (c CompositeMapper) Map(srcPtr interface{}, destPtr interface{}) error {
  return c.Fast().Map(srcPtr, destPtr)
}

// Fast returns a quicker version of this CompositeMapper that cannot be
// used by multiple goroutines simultaneously as if FastCompose were used.
func (c CompositeMapper) Fast() Mapper {
  pieces := c.pieces()
  fastPieces := make([]fastMapperPiece, len(pieces))
  for i := range fastPieces {
    fastPieces[i].setFromCompositePiece(&pieces[i])
  }
  return fastCompositeMapper{fastPieces}
}

func (c CompositeMapper) pieces() []compositeMapperPiece {
  if len(c._pieces) == 0 {
    return nilPieceL
  }
  return c._pieces
}

// Creater of T creates a new, pre-initialized, T and returns a pointer to it.
type Creater func() interface {}

// Copier of T copies the value at src to the value at dest. This type is
// often needed when values of type T need to be pre-initialized. src and
// dest are of type *T and both point to pre-initialized T.
type Copier func(src, dest interface{})

// Rows represents rows in a database table. Most database API already have
// a type that implements this interface
type Rows interface {
  // Next advances to the next row. Next returns false if there is no next row.
  // Every call to Scan, even the first one, must be preceded by a call to Next.
  Next() bool
  // Reads the values out of the current row. args are pointer types.
  Scan(args ...interface{}) error
}

// NilStream returns a Stream that emits no values. Calling Close on
// returned Stream is a no-op.
func NilStream() Stream {
  return nilS
}

// Map applies f, which maps a type T value to a type U value, to a Stream
// of T producing a new Stream of U. If s is
// (x1, x2, x3, ...), Map returns the Stream (f(x1), f(x2), f(x3), ...).
// If f returns Skipped for a T value, then the corresponding U value is
// left out of the returned stream. ptr is a *T providing storage for
// emitted values from s. If f is a CompositeMapper, Fast() is called on
// it automatically.
// Calling Close on returned Stream closes s.
func Map(f Mapper, s Stream, ptr interface{}) Stream {
  ms, ok := s.(*mapStream)
  if ok {
    return &mapStream{FastCompose(f, ms.mapper, ptr), ms.Stream, ms.ptr}
  }
  cm, ok := f.(CompositeMapper)
  if ok {
    return &mapStream{cm.Fast(), s, ptr}
  }
  return &mapStream{f, s, ptr}
}

// Filter filters values from s, returning a new Stream of T. The returned
// Stream's Next method reports any errors besides Skipped that the Filter
// method of f returns. 
// f is a Filterer of T; s is a Stream of T.
// Calling Close on returned Stream closes s.
func Filter(f Filterer, s Stream) Stream {
  fs, ok := s.(*filterStream)
  if ok {
    return &filterStream{All(fs.filterer, f), fs.Stream}
  }
  return &filterStream{f, s}
}

// Count returns an infinite Stream of int which emits all values beginning
// at 0.
// Calling Close on returned Stream is a no-op.
func Count() Stream {
  return &count{start: 0, step: 1}
}

// CountFrom returns an infinite Stream of int emitting values beginning at
// start and increasing by step.
// Calling Close on returned Stream is a no-op.
func CountFrom(start, step int) Stream {
  return &count{start: start, step: step}
}

// Slice returns a Stream that will emit elements in s starting at index start
// and continuing to but not including index end. Indexes are 0 based. If end
// is negative, it means go to the end of s.
// Calling Close on returned Stream
// closes s.
func Slice(s Stream, start int, end int) Stream {
  return &sliceStream{Stream: s, start: start, end: end}
}

// ReadRows returns the rows in a database table as a Stream of Tuple.
// Calling Close on returned stream does nothing.
func ReadRows(r Rows) Stream {
  return &rowStream{rows: r}
}

// ReadLines returns the lines of text in r separated by either "\n" or "\r\n"
// as a Stream of string. The emitted string types do not contain the
// end of line characters.
// Calling Close on returned Stream does nothing.
func ReadLines(r io.Reader) Stream {
  return &lineStream{bufio: bufio.NewReader(r)}
}

// ReadLinesAndClose works just like ReadLines except that calling Close on
// returned Stream closes r.
func ReadLinesAndClose(r io.ReadCloser) Stream {
  return &closeStream{Stream: ReadLines(r), Closer: r}
}

// NewStreamFromStreamFunc creates a Stream of Streams by repeatedly calling
// f. Calling Close on returned Stream is a no-op.
func NewStreamFromStreamFunc(f func() Stream) Stream {
  return streamFromStreamFunc{f: f}
}

// Deferred(f) is equivalent to Flatten(Slice(NewStreamFromStreamFunc(f), 0, 1))
func Deferred(f func() Stream) Stream {
  return Flatten(Slice(NewStreamFromStreamFunc(f), 0, 1))
}

// Cycle(f) is equivalent to Flatten(NewStreamFromStreamFunc(f))
func Cycle(f func() Stream) Stream {
  return Flatten(NewStreamFromStreamFunc(f))
}

// Concat concatenates multiple Streams into one.
// If x = (x1, x2, ...) and y = (y1, y2, ...) then
// Concat(x, y) = (x1, x2, ..., y1, y2, ...).
// Calling Close on returned Stream closes all underlying streams.
// If caller passes a slice to Concat, no copy is made of it.
func Concat(s ...Stream) Stream {
  if len(s) == 0 {
    return nilS
  }
  if len(s) == 1 {
    return s[0]
  }
  return &concatStream{s: s}
}

// NewStreamFromValues converts a []T into a Stream of T. aSlice is a []T.
// c is a Copier of T. If c is nil, regular assignment is used.
// Calling Close on returned Stream is a no-op.
func NewStreamFromValues(aSlice interface{}, c Copier) Stream {
  sliceValue := getSliceValue(aSlice)
  if sliceValue.Len() == 0 {
    return nilS
  }
  return &plainStream{sliceValue: sliceValue, copyFunc: toSliceValueCopier(c)}
}

// NewStreamFromPtrs converts a []*T into a Stream of T. aSlice is a []*T.
// c is a Copier of T. If c is nil, regular assignment is used.
// Calling Close on returned Stream is a no-op.
func NewStreamFromPtrs(aSlice interface{}, c Copier) Stream {
  sliceValue := getSliceValue(aSlice)
  if sliceValue.Len() == 0 {
    return nilS
  }
  valueCopierFunc := toSliceValueCopier(c)
  copyFunc := func(src reflect.Value, dest interface{}) {
    valueCopierFunc(reflect.Indirect(src), dest)
  }
  return &plainStream{sliceValue: sliceValue, copyFunc: copyFunc}
}

// Flatten converts a Stream of Stream of T into a Stream of T.
// The returned Stream automatically closes each emitted Stream from s
// propagating any error from closing through Next.
// Calling Close on returned Stream closes s and the last emitted Stream
// from s currently being read.
func Flatten(s Stream) Stream {
  return &flattenStream{stream: s, current: nilS}
}

// TakeWhile returns a Stream that emits the values in s until the Filter
// method of f returns Skipped. The returned Stream's Next method reports
// any errors besides Skipped that the Filter method of f returns. 
// Calling Close on returned Stream closes s.
// f is a Filterer of T; s is a Stream of T.
func TakeWhile(f Filterer, s Stream) Stream {
  return &takeStream{Stream: s, f: f}
}

// DropWhile returns a Stream that emits the values in s starting at the
// first value where the Filter method of f returns Skipped. The returned
// Stream's Next method reports any errors that the Filter method of f
// returns until it returns Skipped. 
// f is a Filterer of T; s is a Stream of T.
// Calling Close on returned Stream closes s.
func DropWhile(f Filterer, s Stream) Stream {
  return &dropStream{Stream: s, f: f}
}

// Any returns a Filterer that returns Skipped if all of the fs return
// Skipped. Otherwise it returns nil or the first error not equal to Skipped.
func Any(fs ...Filterer) Filterer {
  if len(fs) == 0 {
    return falseF
  }
  if len(fs) == 1 {
    return fs[0]
  }
  ors := make([][]Filterer, len(fs))
  for i := range fs {
    ors[i] = orList(fs[i])
  }
  return orFilterer(filterFlatten(ors))
}

// All returns a Filterer that returns nil if all of the
// fs return nil. Otherwise it returns the first error encountered.
func All(fs ...Filterer) Filterer {
  if len(fs) == 0 {
    return trueF
  }
  if len(fs) == 1 {
    return fs[0]
  }
  ands := make([][]Filterer, len(fs))
  for i := range fs {
    ands[i] = andList(fs[i])
  }
  return andFilterer(filterFlatten(ands))
}

// Compose composes two Mappers together into one e.g f(g(x)). If g maps
// type T values to type U values, and f maps type U values to type V
// values, then Compose returns a CompositeMapper mapping T values to V values.
// c is a Creater of U. Each time Map is called on returned CompositeMapper,
// it invokes c to create a U value to receive the intermediate result from g.
func Compose(f Mapper, g Mapper, c Creater) CompositeMapper {
  l := mapperLen(f) + mapperLen(g)
  pieces := make([]compositeMapperPiece, l)
  n := appendMapper(pieces, g)
  pieces[n - 1].creater = c
  appendMapper(pieces[n:], f)
  return CompositeMapper{pieces}
}

// FastCompose works like Compose except that it uses a *U value instead of
// a Creater of U to link f ang g. ptr is the *U value. Intermediate results
// from g are stored at ptr. Unlike Compose, the Mapper that FastCompose
// returns cannot be used by multiple goroutines simultaneously since what
// ptr points to changes with each call to Map.
func FastCompose(f Mapper, g Mapper, ptr interface{}) Mapper {
  l := mapperLen(f) + mapperLen(g)
  pieces := make([]fastMapperPiece, l)
  n := appendFastMapper(pieces, g)
  pieces[n - 1].ptr = ptr
  appendFastMapper(pieces[n:], f)
  return fastCompositeMapper{pieces}
}

// NoCloseStream returns a Stream just like s but with a Close method that does
// nothing. This function is useful for preventing a stream from
// closing its underlying stream.
func NoCloseStream(s Stream) Stream {
  return noCloseStream{s}
}

// NewFilterer returns a new Filterer of T. f takes a *T returning nil
// if T value pointed to it should be included or Skipped if it should not
// be included. f can return other errors too.
func NewFilterer(f func(ptr interface{}) error) Filterer {
  return funcFilterer(f)
}

// NewMapper returns a new Mapper mapping T values to U Values. In f,
// srcPtr is a *T and destPtr is a *U pointing to pre-allocated T and U
// values respectively. f returns Skipped if mapped value should be
// skipped. f can also return other errors.
func NewMapper(m func(srcPtr interface{}, destPtr interface{}) error) Mapper {
  return funcMapper(m)
}

type count struct {
  start int
  step int
  closeDoesNothing
}

func (c *count) Next(ptr interface{}) error {
  p := ptr.(*int)
  *p = c.start
  c.start += c.step
  return nil
}

type mapStream struct {
  mapper Mapper
  Stream
  ptr interface{} 
}

func (s *mapStream) Next(ptr interface{}) error {
  err := s.Stream.Next(s.ptr)
  for ; err == nil; err = s.Stream.Next(s.ptr) {
    if err = s.mapper.Map(s.ptr, ptr); err != Skipped {
      return err
    }
  }
  return err
}

type trueFilterer struct {
}

func (t trueFilterer) Filter(ptr interface{}) error {
  return nil
}

type falseFilterer struct {
}

func (f falseFilterer) Filter(ptr interface{}) error {
  return Skipped
}

type nilStream struct {
  closeDoesNothing
}

func (s nilStream) Next(ptr interface{}) error {
  return Done
}

type nilMapper struct {
}

func (m nilMapper) Map(srcPtr, destPtr interface{}) error {
  return Skipped
}

type filterStream struct {
  filterer Filterer
  Stream
}

func (s *filterStream) Next(ptr interface{}) error {
  err := s.Stream.Next(ptr)
  for ; err == nil; err = s.Stream.Next(ptr) {
    if err = s.filterer.Filter(ptr); err != Skipped {
      return err
    }
  }
  return err
}

type sliceStream struct {
  Stream
  start int
  end int
  index int
  done bool
}

func (s *sliceStream) Next(ptr interface{}) error {
  if s.done {
    return Done
  }
  for s.end < 0 || s.index < s.end {
    err := s.Stream.Next(ptr)
    if err == Done {
      s.done = true
      return Done
    }
    if err != nil {
      return err
    }
    s.index++
    if s.index > s.start {
      return nil
    }
  }
  s.done = true
  return Done
}

type rowStream struct {
  rows Rows
  done bool
  closeDoesNothing
}

func (s *rowStream) Next(ptr interface{}) error {
  if s.done {
    return Done
  }
  if !s.rows.Next() {
    s.done = true
    return Done
  }
  ptrs := ptr.(Tuple).Ptrs()
  return s.rows.Scan(ptrs...)
}

type lineStream struct {
  bufio *bufio.Reader
  done bool
  closeDoesNothing
}

func (s *lineStream) Next(ptr interface{}) error {
  if s.done {
    return Done
  }
  p := ptr.(*string)
  line, isPrefix, err := s.bufio.ReadLine()
  if err == io.EOF {
    s.done = true
    return Done
  }
  if err != nil {
    return err
  }
  if !isPrefix {
    *p = string(line)
    return nil
  }
  *p, err = s.readRestOfLine(line)
  return err
}

func (s *lineStream) readRestOfLine(line []byte) (string, error) {
  lines := [][]byte{copyBytes(line)}
  for {
    l, isPrefix, err := s.bufio.ReadLine()
    if err == io.EOF {
      break
    }
    if err != nil {
      return "", err
    }
    lines = append(lines, copyBytes(l))
    if !isPrefix {
      break
    }
  }
  return string(byteFlatten(lines)), nil
}

type concatStream struct {
  s []Stream
  idx int
}

func (c *concatStream) Next(ptr interface{}) error {
  for ;c.idx < len(c.s); c.idx++ {
    err := c.s[c.idx].Next(ptr)
    if err == Done {
      continue
    }
    return err
  }
  return Done
}

func (c *concatStream) Close() error {
  var result error
  for i := range c.s {
    err := c.s[i].Close()
    if result == nil {
      result = err
    }
  }
  return result
}

type plainStream struct {
  sliceValue reflect.Value
  copyFunc func(src reflect.Value, dest interface{})
  index int
  closeDoesNothing
}

func (s *plainStream) Next(ptr interface{}) error {
  if s.index == s.sliceValue.Len() {
    return Done
  }
  s.copyFunc(s.sliceValue.Index(s.index), ptr)
  s.index++
  return nil
}

type streamFromStreamFunc struct {
  f func() Stream
  closeDoesNothing
}

func (s streamFromStreamFunc) Next(ptr interface{}) error {
  p := ptr.(*Stream)
  *p = s.f()
  return nil
}

type flattenStream struct {
  stream Stream
  current Stream
}

func (s *flattenStream) Next(ptr interface{}) error {
  err := s.current.Next(ptr)
  for ; err == Done; err = s.current.Next(ptr) {
    var temp Stream
    if serr := s.stream.Next(&temp); serr != nil {
      return serr
    }
    oldCurrent := s.current
    s.current = temp
    if serr := oldCurrent.Close(); serr != nil {
      return serr
    }
  }
  return err
}

func (s *flattenStream) Close() error {
  result := s.current.Close()
  err := s.stream.Close()
  if result == nil {
    result = err
  }
  return result
}

type takeStream struct {
  Stream
  f Filterer
}

func (s *takeStream) Next(ptr interface{}) error {
  if s.f == nil {
    return Done
  }
  err := s.Stream.Next(ptr)
  if err == Done {
    s.f = nil
    return Done
  }
  if err != nil {
    return err
  }
  if ferr := s.f.Filter(ptr); ferr != Skipped {
    return ferr
  }
  s.f = nil
  return Done
}

type dropStream struct {
  Stream
  f Filterer
}

func (s *dropStream) Next(ptr interface{}) error {
  err := s.Stream.Next(ptr)
  if s.f == nil {
    return err
  }
  for ; err == nil; err = s.Stream.Next(ptr) {
    ferr := s.f.Filter(ptr)
    if ferr == Skipped {
      s.f = nil
      return nil
    }
    if ferr != nil {
      return ferr
    }
  }
  return err
}

type closeDoesNothing struct {
}

func (c closeDoesNothing) Close() error {
  return nil
}
  
type funcFilterer func(ptr interface{}) error

func (f funcFilterer) Filter(ptr interface{}) error {
  return f(ptr)
}

type andFilterer []Filterer

func (f andFilterer) Filter(ptr interface{}) error {
  for i := range f {
    if err := f[i].Filter(ptr); err != nil {
      return err
    }
  }
  return nil
}

type orFilterer []Filterer

func (f orFilterer) Filter(ptr interface{}) error {
  for i := range f {
    if err := f[i].Filter(ptr); err != Skipped {
      return err
    }
  }
  return Skipped
}

type funcMapper func(srcPtr interface{}, destPtr interface{}) error

func (m funcMapper) Map(srcPtr interface{}, destPtr interface{}) error {
  return m(srcPtr, destPtr)
}

type fastCompositeMapper struct {
  pieces []fastMapperPiece
}

func (m fastCompositeMapper) Map(srcPtr interface{}, destPtr interface{}) error {
  sPtr := srcPtr
  var dPtr interface{}
  length := len(m.pieces)
  for i := range m.pieces {
    piece := &m.pieces[i]
    if (i == length - 1) {
      dPtr = destPtr
    } else {
      dPtr = piece.ptr
    }
    if err := piece.mapper.Map(sPtr, dPtr); err != nil {
      return err
    }
    sPtr = dPtr
  }
  return nil
}

type compositeMapperPiece struct {
  mapper Mapper
  creater Creater
}

func (cmp *compositeMapperPiece) setFromFastPiece(fmp *fastMapperPiece) {
  cmp.mapper = fmp.mapper
  if fmp.ptr == nil {
    cmp.creater = nil
  } else {
    cmp.creater = newCreater(fmp.ptr)
  }
}

type fastMapperPiece struct {
  mapper Mapper
  ptr interface{}
}

func (fmp *fastMapperPiece) setFromCompositePiece(cmp *compositeMapperPiece) {
  fmp.mapper = cmp.mapper
  if cmp.creater == nil {
    fmp.ptr = nil
  } else {
    fmp.ptr = cmp.creater()
  }
}

type readerWrapper struct {
  io.Reader
}

type rowsWrapper struct {
  Rows
}

type noCloseStream struct {
  Stream
}

func (s noCloseStream) Close() error {
  return nil
}

type closeStream struct {
  Stream
  io.Closer
}

func (s *closeStream) Close() error {
  return s.Closer.Close()
}

func orList(f Filterer) []Filterer {
  switch i := f.(type) {
    case orFilterer:
      return i
    case falseFilterer:
      return nil
  }
  return []Filterer{f}
}

func andList(f Filterer) []Filterer {
  switch i := f.(type) {
    case andFilterer:
      return i
    case trueFilterer:
      return nil
  }
  return []Filterer{f}
}

func filterFlatten(fs [][]Filterer) []Filterer {
  var l int
  for i := range fs {
    l += len(fs[i])
  }
  result := make([]Filterer, l)
  n := 0
  for i := range fs {
    n += copy(result[n:], fs[i])
  }
  return result
}

func mapperLen(m Mapper) int {
  switch am := m.(type) {
  case CompositeMapper:
    return len(am.pieces())
  case fastCompositeMapper:
    return len(am.pieces)
  }
  return 1
}

func appendMapper(pieces []compositeMapperPiece, m Mapper) int {
  switch am := m.(type) {
  case CompositeMapper:
    return copy(pieces, am.pieces())
  case fastCompositeMapper:
    for i := range am.pieces {
      pieces[i].setFromFastPiece(&am.pieces[i])
    }
    return len(am.pieces)
  default:
    pieces[0] = compositeMapperPiece{mapper: m}
  }
  return 1
}

func appendFastMapper(pieces []fastMapperPiece, m Mapper) int {
  switch am := m.(type) {
  case CompositeMapper:
    ampieces := am.pieces()
    for i := range ampieces {
      pieces[i].setFromCompositePiece(&ampieces[i])
    }
    return len(ampieces)
  case fastCompositeMapper:
    return copy(pieces, am.pieces)
  default:
    pieces[0] = fastMapperPiece{mapper: m}
  }
  return 1
}

func newCreater(ptr interface{}) Creater {
  return func() interface{} {
    return ptr
  }
}

func copyBytes(b []byte) []byte {
  result := make([]byte, len(b))
  copy(result, b)
  return result
}

func byteFlatten(b [][]byte) []byte {
  var l int
  for i := range b {
    l += len(b[i])
  }
  result := make([]byte, l)
  n := 0
  for i := range b {
    n += copy(result[n:], b[i])
  }
  return result
}

func toSliceValueCopier(c Copier) func(src reflect.Value, dest interface{}) {
  if c == nil {
    return assignFromValue
  }
  return func(src reflect.Value, dest interface{}) {
    c(src.Addr().Interface(), dest)
  }
}

func assignCopier(src, dest interface{}) {
  srcP := reflect.ValueOf(src)
  assignFromValue(reflect.Indirect(srcP), dest)
}

func assignFromValue(src reflect.Value, dest interface{}) {
  destP := reflect.ValueOf(dest)
  reflect.Indirect(destP).Set(src)
}

func getSliceValue(aSlice interface{}) reflect.Value {
  sliceValue := reflect.ValueOf(aSlice)
  if sliceValue.Kind() != reflect.Slice {
    panic("Slice argument expected")
  }
  return sliceValue
}

