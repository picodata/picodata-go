package picodata

const (
	stateOnline  = "Online"
	stateOffline = "Offline"
)

type event struct {
	address string
	state   string
}
