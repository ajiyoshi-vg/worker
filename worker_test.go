package worker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func ReturnN(i int, out io.Writer) Producer {
	return func() (interface{}, error) {
		fmt.Fprintf(out, "produce:%v\n", i)
		return i, nil
	}
}

func SleepAndReturnN(i int, msec int, out io.Writer) Producer {
	return func() (interface{}, error) {
		time.Sleep(time.Duration(msec) * time.Millisecond)
		fmt.Fprintf(out, "produce:%v (after sleep %v msec)\n", i, msec)
		return i, nil
	}
}

func SleepAndFail(msec int, out io.Writer) Producer {
	return func() (interface{}, error) {
		time.Sleep(time.Duration(msec) * time.Millisecond)
		fmt.Fprintf(out, "produce error (after sleep %v msec)\n", msec)
		return nil, errors.New("error")
	}
}

func TestConsumeAllTheProduced(t *testing.T) {
	out := new(bytes.Buffer)
	ps := ConsProducers(
		SleepAndReturnN(0, 50, out),
		SleepAndReturnN(1, 30, out),
		ReturnN(2, out),
	)
	stat := ps.Consume(func(i interface{}) {
		fmt.Fprintf(out, "consume:%v\n", i)
	})
	fmt.Println(stat)
	expected := `produce:2
consume:2
produce:1 (after sleep 30 msec)
consume:1
produce:0 (after sleep 50 msec)
consume:0
`
	if !strings.HasPrefix(out.String(), expected) {
		t.Fatalf("bad order of result\nexpected(%v) but got(%v)", out.String(), expected)
	}
	if stat.Success != 3 {
		t.Fatalf("task should success(%v)", stat)
	}
	if stat.TotalTime > 80*time.Duration(time.Millisecond) {
		t.Fatalf("sleep 50msec and 30msec should be concurrent(%v)", stat)
	}
}

func TestInTheCaseSomeProducersFailed(t *testing.T) {
	out := new(bytes.Buffer)
	ps := ConsProducers(
		SleepAndFail(50, out),
		SleepAndReturnN(1, 30, out),
		ReturnN(2, out),
	)
	stat := ps.Consume(func(i interface{}) {
		fmt.Fprintf(out, "consume:%v\n", i)
	})
	fmt.Println(stat)
	expected := `produce:2
consume:2
produce:1 (after sleep 30 msec)
consume:1
produce error (after sleep 50 msec)
`
	if !strings.HasPrefix(out.String(), expected) {
		t.Fail()
	}
	if stat.Success != 2 {
		t.Fatalf("one task should fail(%v)", stat)
	}
	if stat.TotalTime > 80*time.Duration(time.Millisecond) {
		t.Fatalf("sleep 50msec and 30msec should be concurrent(%v)", stat)
	}
}
