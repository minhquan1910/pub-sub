package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type (
	subscriber chan interface{}
	topicFunc  func(v interface{}) bool
)

type PubInterface interface {
	// Subscribe tất cả các topic
	Subscribe() chan interface{}
	// Subscribe một topic nhất định
	SubscribeTopic(topic topicFunc) chan interface{}
	// Xóa 1 subscriber
	Evict(sub chan interface{})
	// Publish ra 1 topic
	Publish(v interface{})
	// Xóa tất cả các sub của pub
	Close()
	// gửi topic
	sendTopic(sub subscriber, topic topicFunc, v interface{}, wg *sync.WaitGroup)
}

type Publisher struct {

	//Mutex
	mutex sync.RWMutex

	//time out cho viec publishing
	timeout time.Duration

	//Kich thuoc hang doi
	buffer int

	// topic cua subscriber
	subscribers map[subscriber]topicFunc
}

func NewPublisher(duration time.Duration, buffer int) PubInterface {
	return &Publisher{
		timeout:     duration,
		buffer:      buffer,
		subscribers: make(map[subscriber]topicFunc),
	}
}

func (p *Publisher) Subscribe() chan interface{} {
	return p.SubscribeTopic(nil)
}

func (p *Publisher) SubscribeTopic(topic topicFunc) chan interface{} {

	ch := make(chan interface{}, p.buffer)
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.subscribers[ch] = topic

	return ch
}

func (p *Publisher) Evict(sub chan interface{}) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	delete(p.subscribers, sub)
	close(sub)
}

func (p *Publisher) Publish(v interface{}) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	var wg sync.WaitGroup
	for sub, topic := range p.subscribers {
		wg.Add(1)
		p.sendTopic(sub, topic, v, &wg)
	}

	wg.Wait()
}

func (p *Publisher) Close() {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	for sub := range p.subscribers {
		delete(p.subscribers, sub)
		close(sub)
	}
}

func (p *Publisher) sendTopic(sub subscriber, topic topicFunc, v interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	if topic != nil && !topic(v) {
		return
	}

	select {
	case sub <- v:
	case <-time.After(p.timeout):
	}
}

func main() {
	p := NewPublisher(100*time.Millisecond, 10)

	defer p.Close()

	all := p.Subscribe()
	golang := p.SubscribeTopic(func(v interface{}) bool {
		if s, ok := v.(string); ok {
			return strings.Contains(s, "golang")
		}
		return false
	})

	p.Publish("hello golang")
	p.Publish("hello c++")

	go func() {
		for msg := range all {
			fmt.Println("all: ", msg)
		}
	}()

	go func() {
		for msg := range golang {
			fmt.Println("all: ", msg)
		}
	}()

	time.Sleep(time.Second * 3)
}
