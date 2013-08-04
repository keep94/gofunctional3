// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package functional

// Emitter allows a function to emit values to an associated Stream.
type Emitter interface {

  // EmitPtr returns the pointer supplied to Next of associated Stream.
  // If Close is called on associated Stream, EmitPtr returns nil and false.
  EmitPtr() (ptr interface{}, streamOpened bool)

  // Return causes Next of associated Stream to return. Return yields control
  // to the caller of Next blocking until Next on associated Stream is called
  // again or Stream is closed. err is the value that Next should return.
  Return(err error)

  // An Emitting function calls Finalize when it is done emitting values.
  // but before it does any final cleanup. Finalize blocks until Close is
  // called on associated Stream.
  Finalize()
}

// NewGenerator creates a Stream that emits the values from emitting
// function f. First, f emits values by calling EmitPtr and Return on the
// Emitter passed to it. When When f is through emitting values or when EmitPtr
// returns false for streamOpened, f calls Finalize() on its Emitter,
// performs any necessary cleanup and finally returns the error that
// Close() on the associated Stream will return. Its very important that f
// calls Finalize() before performing cleanup to ensure that the cleanup is
// done after Close() is called on the associated Stream. Caller must call
// Close() on returned Stream or else the goroutine operating the Stream will
// never exit.
func NewGenerator(f func(e Emitter) error) Stream {
  result := regularGenerator{&emitterStream{ptrCh: make(chan interface{}), errCh: make(chan error)}}
  go func() {
    var err error
    defer func() {
      result.Finalize()
      result.endEmitter(err)
    }()
    result.startEmitter()
    err = f(result)
  }()
  return result
}

// EmitAll emits all of Stream s to Emitter e.
// If the Stream for e becomes closed, EmitAll returns false.
// Otherwise EmitAll returns true.
func EmitAll(s Stream, e Emitter) (opened bool) {
  var ptr interface{}
  if ptr, opened = e.EmitPtr(); !opened {
    return
  }
  for err := s.Next(ptr); err != Done; err = s.Next(ptr) {
    e.Return(err)
    if ptr, opened = e.EmitPtr(); !opened {
      return
    }
  }
  return
}

type regularGenerator struct {
  *emitterStream
}

func (e regularGenerator) Finalize() {
  for _, opened := e.EmitPtr(); opened; _, opened = e.EmitPtr() {
    e.Return(Done)
  }
}

func (s regularGenerator) Return(err error) {
  if (s.ptr != nil) {
    s.emitterStream.Return(err)
  }
}

func (s regularGenerator) Close() error {
  result := s.Next(nil)
  s.close()
  return result
}
