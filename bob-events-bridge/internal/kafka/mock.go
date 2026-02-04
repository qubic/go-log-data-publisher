package kafka

import (
	"context"
	"fmt"
	"sync"
)

// MockPublisher is a thread-safe mock implementation of Publisher for testing.
type MockPublisher struct {
	mu       sync.Mutex
	messages []*EventMessage
	failNext bool
	failErr  error
}

// NewMockPublisher creates a new MockPublisher.
func NewMockPublisher() *MockPublisher {
	return &MockPublisher{}
}

// PublishEvent captures the message for later inspection.
func (m *MockPublisher) PublishEvent(_ context.Context, msg *EventMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failNext {
		m.failNext = false
		if m.failErr != nil {
			return m.failErr
		}
		return fmt.Errorf("mock publish failure")
	}

	m.messages = append(m.messages, msg)
	return nil
}

// Close is a no-op for the mock.
func (m *MockPublisher) Close() error {
	return nil
}

// Messages returns all captured messages.
func (m *MockPublisher) Messages() []*EventMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*EventMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

// SetFailNext makes the next PublishEvent call return an error.
func (m *MockPublisher) SetFailNext(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNext = true
	m.failErr = err
}

// Reset clears all captured messages and error state.
func (m *MockPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = nil
	m.failNext = false
	m.failErr = nil
}
