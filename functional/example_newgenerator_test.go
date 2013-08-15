package functional_test

import (
  "fmt"
  "github.com/keep94/gofunctional3/functional"
  "time"
)

// DateCount represents a count of some occurrence by date
type DateCount struct {
  Date time.Time
  Count int64
}

// YearCount represents a count of some occurrence by year
type YearCount struct {
  Year int
  Count int64
}

func emitByYear(s functional.Stream, e functional.Emitter) {
  var ptr interface{}
  var opened bool
  var incoming DateCount

  // As soon as caller calls Close() we quit without pulling unnecessary
  // values from underlying stream.
  if ptr, opened = e.EmitPtr(); !opened {
    return
  }
  var err error

  // We get the first date while propagating any errors to the caller.
  // Note that we stay in this loop until either we get a first date or
  // caller calls Close()
  for err = s.Next(&incoming); err != nil; err = s.Next(&incoming) {
    // Propagate error and yield execution to caller.
    e.Return(err)

    // After each call to Return, we must get the pointer caller supplied to
    // Next.
    if ptr, opened = e.EmitPtr(); !opened {
      return
    }
  }
  currentYear := incoming.Date.Year()

  // Running total for current year
  sum := incoming.Count

  // When Done marker is reached, we have to emit the count of the final year.
  for err = s.Next(&incoming); err != functional.Done; err = s.Next(&incoming) {

    // Propagate any errors to caller.
    if err != nil {
      e.Return(err)
      if ptr, opened = e.EmitPtr(); !opened {
        return
      }
      continue
    }
    year := incoming.Date.Year()

    // If year changed, emit the count for the current year.
    // Then change currentYear and and make sum be the running total for
    // that year.
    if year != currentYear {
      *ptr.(*YearCount) = YearCount{Year: currentYear, Count: sum}
      e.Return(nil)
      if ptr, opened = e.EmitPtr(); !opened {
        return
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
  // count for the final year. The call to Finalize() in ByYear() takes care
  // of returning functional.Done to the caller.
  e.Return(nil)
}

// ByYear takes a Stream of DateCount instances and returns a Stream of
// YearCount instances by totaling the counts in the DateCount instances
// by year. The DateCount instances must be ordered by Date. Caller must Close
// the returned Stream.
func ByYear(s functional.Stream) functional.Stream {
  return functional.NewGenerator(func(e functional.Emitter) error {
    // Emit counts by year
    emitByYear(s, e)

    // Wait until caller calls Close() while returning functional.Done each time
    // caller calls Next().
    e.Finalize()

    // Do clean up that is visible to caller
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
