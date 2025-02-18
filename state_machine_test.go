package picodata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateMachine(t *testing.T) {
	t.Run("TestInitialState", func(t *testing.T) {
		sm := newStateMachine("initial", nil)
		assert.Equal(t, sm.CurrentState(), state("initial"))
	})

	t.Run("TestTransitionValid", func(t *testing.T) {
		transitions := map[state]transitionMap{
			"off": {
				"on": func() state { return "on" },
			},
		}
		sm := newStateMachine("off", transitions)

		newState, err := sm.Transition("on")

		assert.NoError(t, err)
		assert.Equal(t, newState, state("on"))
		assert.Equal(t, sm.CurrentState(), state("on"))
	})

	t.Run("TestTransitionInvalid", func(t *testing.T) {
		transitions := map[state]transitionMap{
			"off": {
				"on": func() state { return "on" },
			},
		}
		sm := newStateMachine("off", transitions)

		_, err := sm.Transition("invalid_event")

		assert.Error(t, err)
		expectedErrMsg := "event doesn't exits"
		assert.Equal(t, expectedErrMsg, err.Error())
		assert.Equal(t, sm.CurrentState(), state("off"))
	})

	t.Run("TestTransitionMulti", func(t *testing.T) {
		transitions := map[state]transitionMap{
			"off": {
				"on": func() state { return "on" },
			},
			"on": {
				"off": func() state { return "off" },
			},
		}
		sm := newStateMachine("off", transitions)

		newState, _ := sm.Transition("on")
		assert.Equal(t, newState, state("on"))
		newState, _ = sm.Transition("off")
		assert.Equal(t, newState, state("off"))
	})
}
