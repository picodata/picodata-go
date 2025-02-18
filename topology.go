package picodata

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type poller interface {
	StartPolling(ctx context.Context, period time.Duration)
}

const (
	online  = "Online"
	offline = "Offline"
)

type topologyManagerImpl struct {
	balancer  *balancerImpl
	connState map[string]*stateMachine
}

func newManager(baseConfig *pgxpool.Config, balancer *balancerImpl) *topologyManagerImpl {
	connStatusMap := make(map[string]*stateMachine, len(balancer.Conns()))
	for connAddress := range balancer.ConnMap() {
		connStatusMap[connAddress] = newStateMachine(online, newTransitionMap(connAddress, baseConfig, balancer))
	}

	return &topologyManagerImpl{
		balancer:  balancer,
		connState: connStatusMap,
	}
}

func (m *topologyManagerImpl) StartPolling(ctx context.Context, period time.Duration) {
	const (
		instanceStateQuery = `
		SELECT ppa.address,
		       pi.current_state
		FROM   _pico_peer_address AS ppa
		       JOIN _pico_instance AS pi
		         ON ppa.raft_id = pi.raft_id
		WHERE  connection_type = 'pgproto';
	`
	)
	type instanceStatus struct {
		address       string
		currentStatus struct {
			state string
			id    float64
		}
	}

	ticker := time.NewTicker(period)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// noop
			}

			var conn *pgxpool.Pool
			for name, state := range m.connState {
				if state.currentState == online {
					conn = m.balancer.ConnMap()[name]
				}
			}

			if conn == nil {
				log.Println("No connections are online, continue listening...")
				continue
			}

			rows, err := conn.Query(context.Background(), instanceStateQuery)
			if err != nil {
				log.Println(err)
				continue
			}

			for rows.Next() {
				var addr string
				var rawStatus []any
				if err := rows.Scan(&addr, &rawStatus); err != nil {
					log.Println(err)
					continue
				}

				status := instanceStatus{
					address: addr,
					currentStatus: struct {
						state string
						id    float64
					}{
						state: rawStatus[0].(string),
						id:    rawStatus[1].(float64),
					},
				}

				_, err := m.connState[status.address].Transition(event(status.currentStatus.state))
				if err != nil {
					log.Printf("Error while state transition: %s", err.Error())
				}
			}
			rows.Close()
		}

	}()
}

func newTransitionMap(connAddress string, config *pgxpool.Config, balancer *balancerImpl) map[state]transitionMap {
	cfg := config.Copy()
	addr := strings.Split(connAddress, ":")
	cfg.ConnConfig.Host = addr[0]
	port, _ := strconv.Atoi(addr[1])
	cfg.ConnConfig.Port = uint16(port)

	return map[state]transitionMap{
		online: {
			online: func() state {
				return state(online)
			},
			offline: func() state {
				balancer.RemoveConn(connAddress)
				log.Println("Removed connection: ", connAddress)
				return state(offline)
			},
		},
		offline: {
			online: func() state {
				newPool, _ := pgxpool.NewWithConfig(context.Background(), cfg)
				balancer.AddConn(connAddress, newPool)
				log.Println("Add connection: ", connAddress)
				return state(online)
			},
			offline: func() state {
				return state(offline)
			},
		},
	}
}
