package worker

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type Producers []Producer
type Producer func() (interface{}, error)
type Consumer func(interface{})
type ErrorHandler func(error)
type Item struct {
	val interface{}
	err error
}
type Result struct {
	success     int
	failure     int
	produceTime []time.Duration
	consumeTime []time.Duration
	blockTime   []time.Duration
	totalTime   time.Duration
}

var LogError ErrorHandler = func(err error) {
	log.Println(err)
}

func ConsProducers(ps ...Producer) Producers {
	return ps
}

func (ps Producers) Consume(c Consumer) *Result {
	return ConsumeAll(ps, c, LogError)
}

func ConsumeAll(ps []Producer, c Consumer, eh ErrorHandler) *Result {
	num := len(ps)
	ch := make(chan Item, num)
	result := NewResult(num)
	start := time.Now()

	for i, producer := range ps {
		go func(p Producer, cost *time.Duration) {
			from := time.Now()
			val, err := p()
			ch <- Item{val, err}
			*cost = time.Now().Sub(from)
		}(producer, &result.produceTime[i])
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < num; i++ {
			blockFrom := time.Now()
			v := <-ch
			result.blockTime[i] = time.Now().Sub(blockFrom)

			from := time.Now()
			if v.err != nil {
				result.failure++
				eh(v.err)
			} else {
				result.success++
				c(v.val)
			}
			result.consumeTime[i] = time.Now().Sub(from)
		}
		wg.Done()
	}()

	wg.Wait()
	result.totalTime = time.Now().Sub(start)
	return result
}

func NewResult(n int) *Result {
	return &Result{
		produceTime: make([]time.Duration, n),
		consumeTime: make([]time.Duration, n),
		blockTime:   make([]time.Duration, n),
	}
}

func (stat Result) String() string {
	format := `{
	"success":%v,
	"failure":%v,
	"totalTime":"%v",
	"blockTimes":[%v],
	"produceTimes":[%v],
	"consumeTimes":[%v]
}`
	return fmt.Sprintf(format,
		stat.success, stat.failure,
		stat.totalTime,
		jsonTimes(stat.blockTime),
		jsonTimes(stat.produceTime),
		jsonTimes(stat.consumeTime),
	)
}

func jsonTimes(ts []time.Duration) string {
	array := make([]string, 0, len(ts))
	for _, t := range ts {
		array = append(array, fmt.Sprintf("\"%v\"", t))
	}
	return strings.Join(array, ", ")
}
