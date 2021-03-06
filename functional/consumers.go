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
	switch len(consumers) {
	case 0:
		return nilConsumer{}
	case 1:
		return consumers[0]
	default:
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
}

// FilterConsumer creates a new Consumer whose Consume method applies f to the
// Stream before passing it onto c.
func FilterConsumer(c Consumer, f Filterer) Consumer {
	if nested, ok := c.(*filterConsumer); ok {
		return &filterConsumer{
			consumer: nested.consumer,
			filter:   All(f, nested.filter),
		}
	}
	return &filterConsumer{consumer: c, filter: f}
}

// MapConsumer creates a new Consumer whose Consume method applies m to the
// Stream before passing it onto c. c cnsumes U values; m maps T values to
// U Values; ptr is *T to temporarily hold T values, and this function
// returns a consumer of T values.
func MapConsumer(c Consumer, m Mapper, ptr interface{}) Consumer {
	if nested, ok := c.(*mapConsumer); ok {
		return &mapConsumer{
			consumer: nested.consumer,
			mapper:   FastCompose(nested.mapper, m, nested.ptr),
			ptr:      ptr,
		}
	}
	return &mapConsumer{consumer: c, mapper: m, ptr: ptr}
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
func MultiConsume(s Stream, ptr interface{}, copier Copier, consumers ...Consumer) (errors []error) {
	consumerLen := len(consumers)
	if consumerLen == 0 {
		return
	}
	errors = make([]error, consumerLen)
	if consumerLen == 1 {
		errors[0] = consumers[0].Consume(s)
		return
	}

	if copier == nil {
		copier = assignCopier
	}
	streams := make([]splitStream, consumerLen)
	for i := range streams {
		streams[i] = splitStream{&emitterStream{ptrCh: make(chan interface{}), errCh: make(chan error)}}
		go func(s splitStream, c Consumer, e *error) {
			defer s.endStream()
			s.startStream()
			*e = c.Consume(s)
		}(streams[i], consumers[i], &errors[i])
	}
	var err error
	for asyncReturn(streams, err) {
		err = s.Next(ptr)
		for i := range streams {
			if !streams[i].isClosed() {
				p := streams[i].ptr
				copier(ptr, p)
			}
		}
	}
	return
}

// NilConsumer returns a consumer that consumes no values.
func NilConsumer() Consumer {
	return nilConsumer{}
}

type splitStream struct {
	*emitterStream
}

func (s splitStream) Next(ptr interface{}) error {
	if ptr == nil {
		panic("Got nil pointer in Next.")
	}
	return s.emitterStream.next(ptr)
}

func (s splitStream) Close() error {
	return nil
}

type nilConsumer struct {
}

func (n nilConsumer) Consume(s Stream) error {
	return nil
}

type filterConsumer struct {
	consumer Consumer
	filter   Filterer
}

func (f *filterConsumer) Consume(s Stream) error {
	return f.consumer.Consume(Filter(f.filter, s))
}

type mapConsumer struct {
	consumer Consumer
	mapper   Mapper
	ptr      interface{}
}

func (m *mapConsumer) Consume(s Stream) error {
	return m.consumer.Consume(Map(m.mapper, s, m.ptr))
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
