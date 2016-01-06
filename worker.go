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
	Success      int
	Failure      int
	ProduceTime  []time.Duration
	ConsumeTime  []time.Duration
	BlockTime    []time.Duration
	TotalTime    time.Duration
	TotalProduce time.Duration
	TotalConsume time.Duration
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
		go func(p Producer, cost *time.Duration, total *time.Duration) {
			from := time.Now()
			val, err := p()
			ch <- Item{val, err}
			*cost = time.Now().Sub(from)
			*total = time.Now().Sub(start)
		}(producer, &result.ProduceTime[i], &result.TotalProduce)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		consumeStart := time.Now()
		for i := 0; i < num; i++ {
			blockFrom := time.Now()
			v := <-ch
			result.BlockTime[i] = time.Now().Sub(blockFrom)

			from := time.Now()
			if v.err != nil {
				result.Failure++
				eh(v.err)
			} else {
				result.Success++
				c(v.val)
			}
			result.ConsumeTime[i] = time.Now().Sub(from)
			result.TotalConsume = time.Now().Sub(consumeStart)
		}
		wg.Done()
	}()

	wg.Wait()
	result.TotalTime = time.Now().Sub(start)
	return result
}

func NewResult(n int) *Result {
	return &Result{
		ProduceTime: make([]time.Duration, n),
		ConsumeTime: make([]time.Duration, n),
		BlockTime:   make([]time.Duration, n),
	}
}

func (stat Result) String() string {
	format := `{
	"success":%v,
	"failure":%v,
	"totalTime":"%v",
	"totalProduce":"%v",
	"totalConsume":"%v",
	"blockTimes":[%v],
	"produceTimes":[%v],
	"consumeTimes":[%v]
}`
	return fmt.Sprintf(format,
		stat.Success, stat.Failure,
		stat.TotalTime,
		stat.TotalProduce,
		stat.TotalConsume,
		jsonTimes(stat.BlockTime),
		jsonTimes(stat.ProduceTime),
		jsonTimes(stat.ConsumeTime),
	)
}

func jsonTimes(ts []time.Duration) string {
	array := make([]string, 0, len(ts))
	for _, t := range ts {
		array = append(array, fmt.Sprintf("\"%v\"", t))
	}
	return strings.Join(array, ", ")
}
