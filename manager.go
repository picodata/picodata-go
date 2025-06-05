package picodata

import "git.picodata.io/core/picodata-go/logger"

type topologyManager struct {
	provider *connectionProvider
}

func newTopologyManager(provider *connectionProvider) *topologyManager {
	return &topologyManager{
		provider: provider,
	}
}

func (m *topologyManager) runProcessing(eventsChan <-chan event) {
	for event := range eventsChan {
		switch event.state {
		case stateOnline:
			if err := m.provider.addConn(event.address); err != nil {
				logger.Log(logger.LevelError, "manager: runProcessing: error adding new connection: %v", err)
			}
		case stateOffline:
			m.provider.removeConn(event.address)
		default:
			logger.Log(logger.LevelWarn, "manager: runProcessing: undefined state %q for addr %q", event.state, event.address)
		}
	}
}
