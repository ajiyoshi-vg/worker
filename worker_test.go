package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func ReturnN(i int) Producer {
	return func() (interface{}, error) {
		fmt.Printf("produce:%v\n", i)
		return i, nil
	}
}

func SleepAndReturnN(i int, msec int) Producer {
	return func() (interface{}, error) {
		time.Sleep(time.Duration(msec) * time.Millisecond)
		fmt.Printf("produce:%v (after sleep %v msec)\n", i, msec)
		return i, nil
	}
}

func SleepAndFail(msec int) Producer {
	return func() (interface{}, error) {
		time.Sleep(time.Duration(msec) * time.Millisecond)
		fmt.Printf("produce error (after sleep %v msec)\n", msec)
		return nil, errors.New("error")
	}
}

func TestConsumeAllTheProduced(t *testing.T) {
	ps := ConsProducers(
		SleepAndReturnN(0, 50),
		SleepAndReturnN(1, 30),
		ReturnN(2),
	)
	stat := ps.Consume(func(i interface{}) {
		fmt.Printf("consume:%v\n", i)
	})
	fmt.Println(stat)
}

func TestInTheCaseSomeProducersFailed(t *testing.T) {
	ps := ConsProducers(
		SleepAndFail(50),
		SleepAndReturnN(1, 30),
		ReturnN(2),
	)
	stat := ps.Consume(func(i interface{}) {
		fmt.Printf("consume:%v\n", i)
	})
	fmt.Println(stat)
}
