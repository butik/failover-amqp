package failover_amqp

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type RMQAtomicConn struct {
	value atomic.Value
}

type RMQGracefulReconnectionCallback func(restoredConnection *RMQAtomicConn)

func (a *RMQAtomicConn) Get() (*amqp.Connection, error) {
	conn := a.value.Load()
	if conn == nil {
		return nil, errors.New("connection is not initialized")
	}

	return conn.(*amqp.Connection), nil
}

func (a *RMQAtomicConn) Set(conn *amqp.Connection) {
	a.value.Store(conn)
}

type RMQFailoverConnector struct {
	URLList                   []string
	rmqAtomicConn             *RMQAtomicConn
	rmqErrChan                chan *amqp.Error
	rmqGracefulReconnectionCb RMQGracefulReconnectionCallback
	reconnectionCount         *uint64
}

func NewRMQFailoverConnector(urlList []string) *RMQFailoverConnector {
	return &RMQFailoverConnector{
		URLList:                   urlList,
		rmqAtomicConn:             &RMQAtomicConn{},
		rmqGracefulReconnectionCb: nil,
		reconnectionCount:         new(uint64),
	}
}

func (c *RMQFailoverConnector) getNextURL() string {
	currentTrying := atomic.LoadUint64(c.reconnectionCount)
	return c.URLList[int(currentTrying)%len(c.URLList)]
}

func (c *RMQFailoverConnector) rmqConnect(nextUrl string) error {
	atomic.AddUint64(c.reconnectionCount, 1)
	conn, err := amqp.Dial(nextUrl)
	if err != nil {
		return err
	}

	c.rmqErrChan = make(chan *amqp.Error)

	conn.NotifyClose(c.rmqErrChan)

	c.rmqAtomicConn.Set(conn)

	return nil
}

func (c *RMQFailoverConnector) rmqCloseListener() {
	var rmqErr *amqp.Error
	for {
		if rmqErr = <-c.rmqErrChan; rmqErr != nil {
			log.Printf("RMQ connection error: %s\n", rmqErr.Error())

			ebf := backoff.NewExponentialBackOff()
			ebf.InitialInterval = 500 * time.Millisecond
			ebf.MaxInterval = 5 * time.Second
			ebf.MaxElapsedTime = 0

			for {
				time.Sleep(ebf.NextBackOff())
				if err := c.rmqConnect(c.getNextURL()); err != nil {
					log.Println("RMQ reconnection failure: ", err.Error())
				} else {
					log.Println("RMQ reconnected")
					break
				}
			}

			if c.rmqGracefulReconnectionCb != nil {
				c.rmqGracefulReconnectionCb(c.rmqAtomicConn)
			}
		} else {
			break
		}
	}
}

func (c *RMQFailoverConnector) GetConnection() (*RMQAtomicConn, error) {
	if err := c.rmqConnect(c.getNextURL()); err != nil {
		return nil, err
	}

	go c.rmqCloseListener()

	return c.rmqAtomicConn, nil
}

func (c *RMQFailoverConnector) SetGracefulReconnectionCallback(cb RMQGracefulReconnectionCallback) {
	c.rmqGracefulReconnectionCb = cb
}
