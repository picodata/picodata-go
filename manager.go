package picodata

import "github.com/picodata/picodata-go/logger"

type topologyManager struct {
	provider *connectionProvider
}

func newTopologyManager(provider *connectionProvider) *topologyManager {
	return &topologyManager{
		provider: provider,
	}
}

func (m *topologyManager) runProcessing(eventsChan <-chan event) {
	const op = "manager: runProcessing"

	for event := range eventsChan {
		switch event.state {
		case stateOnline:
			if err := m.provider.addConn(event.address); err != nil {
				logger.Log(logger.LevelError, "%s: %v", op, err)
			}
		case stateOffline:
			m.provider.removeConn(event.address)
		default:
			logger.Log(logger.LevelWarn, "%s: undefined state %q for %q", op, event.state, event.address)
		}
	}
}
