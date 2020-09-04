// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package functional_test

import (
	"fmt"
	"github.com/keep94/gofunctional3/functional"
	"time"
)

// DateCount represents a count of some occurrence by date
type DateCount struct {
	Date  time.Time
	Count int64
}

// YearCount represents a count of some occurrence by year
type YearCount struct {
	Year  int
	Count int64
}

// ByYear takes a Stream of DateCount instances and returns a Stream of
// YearCount instances by totaling the counts in the DateCount instances
// by year. The DateCount instances must be ordered by Date. Caller must Close
// the returned Stream. Calling Close on returned Stream also closes s.
func ByYear(s functional.Stream) functional.Stream {
	return functional.NewGenerator(func(e functional.Emitter) error {
		var ptr interface{}
		var opened bool
		var incoming DateCount

		// As soon as caller calls Close() we quit without pulling unnecessary
		// values from underlying stream.
		if ptr, opened = e.EmitPtr(); !opened {
			return s.Close()
		}
		var err error

		// We get the first date while propagating any errors to the caller.
		// Note that we stay in this loop until either we get a first date or
		// caller calls Close()
		for err = s.Next(&incoming); err != nil; err = s.Next(&incoming) {
			// Propagate error and yield execution to caller. Then get the next
			// pointer caller pases to Next().
			if ptr, opened = e.Return(err); !opened {
				return s.Close()
			}
		}
		currentYear := incoming.Date.Year()

		// Running total for current year
		sum := incoming.Count

		// When Done marker is reached, we have to emit the count of the final year.
		for err = s.Next(&incoming); err != functional.Done; err = s.Next(&incoming) {

			// Propagate any errors to caller.
			if err != nil {
				if ptr, opened = e.Return(err); !opened {
					return s.Close()
				}
				continue
			}
			year := incoming.Date.Year()

			// If year changed, emit the count for the current year.
			// Then change currentYear and and make sum be the running total for
			// that year.
			if year != currentYear {
				*ptr.(*YearCount) = YearCount{Year: currentYear, Count: sum}
				if ptr, opened = e.Return(nil); !opened {
					return s.Close()
				}
				sum = incoming.Count
				currentYear = year
			} else {
				sum += incoming.Count
			}
		}
		// Emit the final year.
		*ptr.(*YearCount) = YearCount{Year: currentYear, Count: sum}

		// Note that we return nil, not functional.Done, since we are emitting the
		// count for the final year. The call to WaitForClose() takes
		// care of returning functional.Done to the caller until the caller calls
		// Close()
		e.Return(nil)
		functional.WaitForClose(e)
		return s.Close()
	})
}

func YMD(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func ExampleNewGenerator() {
	s := functional.NewStreamFromPtrs(
		[]*DateCount{
			{YMD(2013, 5, 24), 13},
			{YMD(2013, 4, 1), 5},
			{YMD(2013, 1, 1), 8},
			{YMD(2012, 12, 31), 24},
			{YMD(2012, 5, 26), 10}},
		nil)
	s = ByYear(s)
	defer s.Close()
	var yc YearCount
	for err := s.Next(&yc); err == nil; err = s.Next(&yc) {
		fmt.Printf("%d: %d\n", yc.Year, yc.Count)
	}
	// Output:
	// 2013: 26
	// 2012: 34
}
