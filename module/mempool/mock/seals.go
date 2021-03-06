// Code generated by mockery v1.0.0. DO NOT EDIT.

package mempool

import (
	flow "github.com/onflow/flow-go/model/flow"

	mock "github.com/stretchr/testify/mock"
)

// Seals is an autogenerated mock type for the Seals type
type Seals struct {
	mock.Mock
}

// Add provides a mock function with given fields: seal
func (_m *Seals) Add(seal *flow.Seal) bool {
	ret := _m.Called(seal)

	var r0 bool
	if rf, ok := ret.Get(0).(func(*flow.Seal) bool); ok {
		r0 = rf(seal)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// All provides a mock function with given fields:
func (_m *Seals) All() []*flow.Seal {
	ret := _m.Called()

	var r0 []*flow.Seal
	if rf, ok := ret.Get(0).(func() []*flow.Seal); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*flow.Seal)
		}
	}

	return r0
}

// ByID provides a mock function with given fields: sealID
func (_m *Seals) ByID(sealID flow.Identifier) (*flow.Seal, bool) {
	ret := _m.Called(sealID)

	var r0 *flow.Seal
	if rf, ok := ret.Get(0).(func(flow.Identifier) *flow.Seal); ok {
		r0 = rf(sealID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*flow.Seal)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(flow.Identifier) bool); ok {
		r1 = rf(sealID)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// Has provides a mock function with given fields: sealID
func (_m *Seals) Has(sealID flow.Identifier) bool {
	ret := _m.Called(sealID)

	var r0 bool
	if rf, ok := ret.Get(0).(func(flow.Identifier) bool); ok {
		r0 = rf(sealID)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// Hash provides a mock function with given fields:
func (_m *Seals) Hash() flow.Identifier {
	ret := _m.Called()

	var r0 flow.Identifier
	if rf, ok := ret.Get(0).(func() flow.Identifier); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(flow.Identifier)
		}
	}

	return r0
}

// Limit provides a mock function with given fields:
func (_m *Seals) Limit() uint {
	ret := _m.Called()

	var r0 uint
	if rf, ok := ret.Get(0).(func() uint); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint)
	}

	return r0
}

// Rem provides a mock function with given fields: sealID
func (_m *Seals) Rem(sealID flow.Identifier) bool {
	ret := _m.Called(sealID)

	var r0 bool
	if rf, ok := ret.Get(0).(func(flow.Identifier) bool); ok {
		r0 = rf(sealID)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// Size provides a mock function with given fields:
func (_m *Seals) Size() uint {
	ret := _m.Called()

	var r0 uint
	if rf, ok := ret.Get(0).(func() uint); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint)
	}

	return r0
}
