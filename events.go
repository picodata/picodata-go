package picodata

type eventState string

const (
	stateOnline  eventState = "Online"
	stateOffline eventState = "Offline"
)

type event struct {
	address string
	state   eventState
}
