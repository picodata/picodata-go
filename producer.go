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
	filter   *stateFilter
}

func newStateProducer(provider *connectionProvider) *stateProducer {
	initConnConfig := provider.config()
	initConnAddr := fmt.Sprintf("%s:%d", initConnConfig.ConnConfig.Host, initConnConfig.ConnConfig.Port)

	return &stateProducer{
		provider: provider,
		filter:   newStateFilter(initConnAddr, stateOnline),
	}
}

func (p *stateProducer) runProducing(eventChan chan<- event, stopChan chan struct{}) {
	const op = "producer: runProducing"

	ticker := time.NewTicker(pollPeriod)

	for {
		select {
		case <-stopChan:
			close(eventChan)
			ticker.Stop()
			return
		case <-ticker.C:
			// noop
		}

		connStates, err := p.getConnStates()
		if err != nil {
			logger.Log(logger.LevelError, "%s: %v", op, err)
			continue
		}

		filteredConnStates := p.filter.filterNewOrUpdated(connStates)

		for _, state := range filteredConnStates {
			eventChan <- event{address: state.address, state: state.currentState}
		}
	}
}

func (p *stateProducer) getConnStates() ([]connState, error) {
	const op = "producer: getConnStates"

	connsState := make([]connState, 0, 1)

	// TODO: Maybe we need a separate balancer for producer?
	// In that case, we will also need to track pool length in two places.
	conn := p.provider.nextConnection()
	rows, err := conn.Query(context.Background(), connsStateQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var connAddr string
		var connFetchedState []any // contains [string, int]

		if err := rows.Scan(&connAddr, &connFetchedState); err != nil {
			return nil, err
		}

		connStateStr, ok := connFetchedState[0].(string)
		if !ok {
			return nil, fmt.Errorf("%s: %s state must be a string, but has type %s", op, connAddr, fmt.Sprintf("%T", connFetchedState[0]))
		}

		connsState = append(connsState, connState{address: connAddr, currentState: connStateStr})
	}

	return connsState, nil
}

// stateFilter keeps track of known states and filters new/updated ones
type stateFilter struct {
	knownConns map[string]string // map[address]state
}

func newStateFilter(initConnAddr, initConnState string) *stateFilter {
	knownConns := make(map[string]string)
	knownConns[initConnAddr] = initConnState

	return &stateFilter{
		knownConns: knownConns,
	}
}

// filterNewOrUpdated filters the input slice and returns a new slice with only new or updated states
func (sf *stateFilter) filterNewOrUpdated(newConnStates []connState) []connState {
	result := make([]connState, 0, len(newConnStates))

	for _, s := range newConnStates {
		if currentState, exists := sf.knownConns[s.address]; !exists || currentState != s.currentState {
			// This is either a new address or the state has changed
			result = append(result, s)
			sf.knownConns[s.address] = s.currentState
		}
	}

	return result
}
