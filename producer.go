package picodata

import (
	"context"
	"fmt"
	"time"

	"git.picodata.io/core/picodata-go/logger"
)

const (
	pollPeriod      = 500 * time.Millisecond
	connsStateQuery = `
		SELECT ppa.address,
		       pi.current_state
		FROM   _pico_peer_address AS ppa
		       JOIN _pico_instance AS pi
		         ON ppa.raft_id = pi.raft_id
		WHERE  connection_type = 'pgproto';
	`
)

type connState struct {
	address      string
	currentState string
}

type stateProducer struct {
	provider *connectionProvider
}

func newStateProducer(provider *connectionProvider) *stateProducer {
	return &stateProducer{
		provider: provider,
	}
}

func (p *stateProducer) runProducing(dataChan chan<- event, stopChan chan struct{}) {
	ticker := time.NewTicker(pollPeriod)

	for {
		select {
		case <-stopChan:
			close(dataChan)
			ticker.Stop()
			return
		case <-ticker.C:
			// noop
		}

		connsState, err := p.getConnsState()
		if err != nil {
			logger.Log(logger.LevelError, "producer: runProducing: error receiving connections state: %v", err)
			continue
		}

		for _, state := range connsState {
			dataChan <- event{address: state.address, state: eventState(state.currentState)}
		}
	}
}

func (p *stateProducer) getConnsState() ([]connState, error) {
	connsState := make([]connState, 0, 1)

	// NOTE: maybe we need a separate balancer for producer?
	// In that case, we wiil need to track pool length in two places.
	conn := p.provider.nextConnection()
	rows, err := conn.Query(context.Background(), connsStateQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var connAddr string
		var connStatus []any

		if err := rows.Scan(&connAddr, &connStatus); err != nil {
			return nil, err
		}

		connStatusState, ok := connStatus[0].(string)
		if !ok {
			return nil, fmt.Errorf("producer: getConnsState: %s state must be a string, but has type %s", connAddr, fmt.Sprintf("%T", connStatus[0]))
		}

		connsState = append(connsState, connState{address: connAddr, currentState: connStatusState})
	}

	return connsState, nil
}
