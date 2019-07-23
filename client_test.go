package failover_amqp

import (
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
)

func TestRMQFailoverConnection(t *testing.T) {
	rmqServers := []string{
		"amqp://guest:guest@127.0.0.1:5673/",
		"amqp://guest:guest@127.0.0.1:5672/",
	}

	connection := NewRMQFailoverConnector(rmqServers)
	connection.SetGracefulReconnectionCallback(func(restoredConnection *RMQAtomicConn) {
		conn, err := restoredConnection.Get()
		require.NoError(t, err)

		_, err = conn.Channel()
		require.NoError(t, err)
	})
	atomicConn, err := connection.GetConnection()
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		time.Sleep(1*time.Second)

		var conn *amqp.Connection
		conn, err = atomicConn.Get()
		require.NoError(t, err)

		var channel *amqp.Channel
		channel, err = conn.Channel()
		require.NoError(t, err)

		err = channel.Publish("test-failover", fmt.Sprintf("action-%d", i), false, false, amqp.Publishing{
			ContentType: "application/json",
			Body: []byte(`{"key":"val"}`),
		})
		require.NoError(t, err)

		connection.rmqErrChan <- amqp.ErrClosed
	}
}

