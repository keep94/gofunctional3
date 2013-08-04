// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package functional

// A Consumer of T consumes the T values from a Stream of T.
type Consumer interface {
  // Consume consumes values from Stream s.
  Consume(s Stream) error
}

// ConsumerFunc is an adapter that allows ordinary functions to be used as
//  Consumers.
type ConsumerFunc func(s Stream) error

func (f ConsumerFunc) Consume(s Stream) error {
  return f(s)
}

// CompositeConsumer returns a Consumer that sends values it consumes to each
// one of consumers. The returned Consumer's Consume method reports an error if 
// the Consume method in any of consumers reports an error.
// ptr is a *T where T values being consumed are temporarily held;
// copier knows how to copy the values of type T being consumed
// (can be nil if simple assignment should be used). If caller passes a slice
// for consumers, no copy is made of it.
func CompositeConsumer(
    ptr interface{},
    copier Copier,
    consumers ...Consumer) Consumer {
  return ConsumerFunc(func(s Stream) error {
    errors := MultiConsume(s, ptr, copier, consumers...)
    for _, e := range errors {
      if e != nil {
        return e
      }
    }
    return nil
  })
}

// FilterConsumer creates a new Consumer whose Consume method applies f to the
// Stream before passing it onto c.
func FilterConsumer(c Consumer, f Filterer) Consumer {
  return ModifyConsumer(
      c,
      func(s Stream) Stream {
        return Filter(f, s)
      })
}

// ModifyConsumer returns a new Consumer
// that applies f to its Stream and then gives the resulting Stream to c.
// If c is a Consumer of T and f takes a Stream of U and returns a Stream of T,
// then ModifyConsumer returns a Consumer of U.
// The Consume method of the returned Consumer will close the Stream that f
// returns but not the original Stream. It does this by wrapping the
// original Stream with NoCloseStream.
func ModifyConsumer(
    c Consumer,
    f func(s Stream) Stream) Consumer {
  return ConsumerFunc(func(s Stream) (err error) {
    newS := f(NoCloseStream(s))
    defer func() {
      ce := newS.Close()
      if err == nil {
        err = ce
      }
    }()
    err = c.Consume(newS)
    return
  })
}

// MultiConsume consumes the values of s, a Stream of T, sending those T
// values to each Consumer in consumers. MultiConsume consumes values from s
// until no Consumer in consumers is accepting values.
// ptr is a *T that receives the values from s. copier is a Copier
// of T used to copy T values to the Streams sent to each Consumer in
// consumers. Passing null for copier means use simple assignment.
// MultiConsume returns all the errors from the individual Consume methods.
// The order of the returned errors matches the order of the consumers.
func MultiConsume(s Stream, ptr interface{}, copier Copier, consumers ...Consumer) (closeErrors []error) {
  if len(consumers) == 0 {
    return
  }
  if copier == nil {
    copier = assignCopier
  }
  streams := make([]splitStream, len(consumers))
  closeErrors = make([]error, len(consumers))
  for i := range streams {
    streams[i] = splitStream{emitterStream{ptrCh: make(chan interface{}), errCh: make(chan error)}}
    go func(s *splitStream, c Consumer, e *error) {
      defer s.endStream()
      s.startStream()
      *e = c.Consume(s)
    }(&streams[i], consumers[i], &closeErrors[i])
  }
  var err error
  for asyncReturn(streams, err) {
    err = s.Next(ptr)
    for i := range streams {
      if !streams[i].isClosed() {
        p, _ := streams[i].EmitPtr()
        copier(ptr, p)
      }
    }
  }
  return
}

type splitStream struct {
  emitterStream
}

func (s *splitStream) Next(ptr interface{}) error {
  if ptr == nil {
    panic("Got nil pointer in Next.")
  }
  return s.emitterStream.Next(ptr)
}

func (s *splitStream) Close() error {
  return nil
}

func asyncReturn(streams []splitStream, err error) bool {
  for i := range streams {
    if !streams[i].isClosed() {
      streams[i].errCh <- err
    }
  }
  result := false
  for i := range streams {
    if !streams[i].isClosed() {
      streams[i].ptr = <-streams[i].ptrCh
      if streams[i].ptr == nil {
        streams[i].close()
      } else {
        result = true
      }
    }
  }
  return result
}
