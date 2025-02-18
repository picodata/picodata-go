package picodata

import (
	"errors"
)

type state string
type event string
type transitionMap map[event]func() state

// StateMachine represents the state machine with current state and transition logic.
type stateMachine struct {
	currentState state
	transitions  map[state]transitionMap // map[state][event] = nextState
}

func newStateMachine(initialState state, transitions map[state]transitionMap) *stateMachine {
	return &stateMachine{
		currentState: initialState,
		transitions:  transitions,
	}
}

// Transition triggers a state change based on the given event.
func (sm *stateMachine) Transition(event event) (state, error) {
	// Get the next state for the current state and event
	nextState, exists := sm.transitions[sm.currentState][event]
	if !exists {
		return "", errors.New("event doesn't exits") // No valid transition for this event
	}

	// Update the current state
	sm.currentState = nextState()
	return sm.currentState, nil
}

// GetCurrentState returns the current state of the state machine.
func (sm *stateMachine) CurrentState() state {
	return sm.currentState
}
