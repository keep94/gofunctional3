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

	// Return causes Next of associated Stream to return err. Return yields
	// execution to the caller of Next blocking until it calls Next again or
	// Close. Finally, Return returns the pointer passed to Next or nil and
	// false if caller called Close.
	Return(err error) (ptr interface{}, streamOpened bool)
}

// NewGenerator creates a Stream that emits the values from emitting
// function f. First, f emits values by calling EmitPtr and Return on the
// Emitter passed to it. When When f is through emitting values or when EmitPtr
// or Return returns false for streamOpened, f calls WaitForClose(),
// performs any necessary cleanup and finally returns the error that
// Close() on the associated Stream will return. Its very important that f
// calls WaitForClose() before performing cleanup to ensure that the cleanup is
// done after Close() is called on the associated Stream. Caller must call
// Close() on returned Stream or else the goroutine operating the Stream will
// never exit. Note that execution of f begins the first time the caller calls
// Next() or Close() on associated Stream.
func NewGenerator(f func(e Emitter) error) Stream {
	result := regularGenerator{&emitterStream{ptrCh: make(chan interface{}), errCh: make(chan error)}}
	go func() {
		var err error
		defer func() {
			WaitForClose(result)
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
		if ptr, opened = e.Return(err); !opened {
			return
		}
	}
	return
}

// Use WaitForClose in emitting functions (See description for NewGenerator).
// WaitForClose yields execution to the caller until caller calls Close on
// associated Stream while returning Done each time caller calls Next.
// An emitting function calls WaitForClose when it is done emitting values.
// but before it does any final cleanup.
func WaitForClose(e Emitter) {
	for _, opened := e.EmitPtr(); opened; _, opened = e.Return(Done) {
	}
}

type regularGenerator struct {
	*emitterStream
}

func (s regularGenerator) EmitPtr() (interface{}, bool) {
	return s.ptr, s.ptr != nil
}

func (s regularGenerator) Return(err error) (interface{}, bool) {
	if s.ptr != nil {
		s.emitterStream.Return(err)
	}
	return s.EmitPtr()
}

func (s regularGenerator) Next(ptr interface{}) error {
	if ptr == nil {
		panic("Got nil pointer in Next.")
	}
	return s.emitterStream.next(ptr)
}

func (s regularGenerator) Close() error {
	result := s.emitterStream.next(nil)
	s.close()
	return result
}
