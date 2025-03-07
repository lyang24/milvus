// Code generated by mockery v2.32.4. DO NOT EDIT.

package datacoord

import mock "github.com/stretchr/testify/mock"

// MockRWChannelStore is an autogenerated mock type for the RWChannelStore type
type MockRWChannelStore struct {
	mock.Mock
}

type MockRWChannelStore_Expecter struct {
	mock *mock.Mock
}

func (_m *MockRWChannelStore) EXPECT() *MockRWChannelStore_Expecter {
	return &MockRWChannelStore_Expecter{mock: &_m.Mock}
}

// Add provides a mock function with given fields: nodeID
func (_m *MockRWChannelStore) Add(nodeID int64) {
	_m.Called(nodeID)
}

// MockRWChannelStore_Add_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Add'
type MockRWChannelStore_Add_Call struct {
	*mock.Call
}

// Add is a helper method to define mock.On call
//   - nodeID int64
func (_e *MockRWChannelStore_Expecter) Add(nodeID interface{}) *MockRWChannelStore_Add_Call {
	return &MockRWChannelStore_Add_Call{Call: _e.mock.On("Add", nodeID)}
}

func (_c *MockRWChannelStore_Add_Call) Run(run func(nodeID int64)) *MockRWChannelStore_Add_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int64))
	})
	return _c
}

func (_c *MockRWChannelStore_Add_Call) Return() *MockRWChannelStore_Add_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockRWChannelStore_Add_Call) RunAndReturn(run func(int64)) *MockRWChannelStore_Add_Call {
	_c.Call.Return(run)
	return _c
}

// Delete provides a mock function with given fields: nodeID
func (_m *MockRWChannelStore) Delete(nodeID int64) ([]*channel, error) {
	ret := _m.Called(nodeID)

	var r0 []*channel
	var r1 error
	if rf, ok := ret.Get(0).(func(int64) ([]*channel, error)); ok {
		return rf(nodeID)
	}
	if rf, ok := ret.Get(0).(func(int64) []*channel); ok {
		r0 = rf(nodeID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*channel)
		}
	}

	if rf, ok := ret.Get(1).(func(int64) error); ok {
		r1 = rf(nodeID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockRWChannelStore_Delete_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Delete'
type MockRWChannelStore_Delete_Call struct {
	*mock.Call
}

// Delete is a helper method to define mock.On call
//   - nodeID int64
func (_e *MockRWChannelStore_Expecter) Delete(nodeID interface{}) *MockRWChannelStore_Delete_Call {
	return &MockRWChannelStore_Delete_Call{Call: _e.mock.On("Delete", nodeID)}
}

func (_c *MockRWChannelStore_Delete_Call) Run(run func(nodeID int64)) *MockRWChannelStore_Delete_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int64))
	})
	return _c
}

func (_c *MockRWChannelStore_Delete_Call) Return(_a0 []*channel, _a1 error) *MockRWChannelStore_Delete_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockRWChannelStore_Delete_Call) RunAndReturn(run func(int64) ([]*channel, error)) *MockRWChannelStore_Delete_Call {
	_c.Call.Return(run)
	return _c
}

// GetBufferChannelInfo provides a mock function with given fields:
func (_m *MockRWChannelStore) GetBufferChannelInfo() *NodeChannelInfo {
	ret := _m.Called()

	var r0 *NodeChannelInfo
	if rf, ok := ret.Get(0).(func() *NodeChannelInfo); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*NodeChannelInfo)
		}
	}

	return r0
}

// MockRWChannelStore_GetBufferChannelInfo_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetBufferChannelInfo'
type MockRWChannelStore_GetBufferChannelInfo_Call struct {
	*mock.Call
}

// GetBufferChannelInfo is a helper method to define mock.On call
func (_e *MockRWChannelStore_Expecter) GetBufferChannelInfo() *MockRWChannelStore_GetBufferChannelInfo_Call {
	return &MockRWChannelStore_GetBufferChannelInfo_Call{Call: _e.mock.On("GetBufferChannelInfo")}
}

func (_c *MockRWChannelStore_GetBufferChannelInfo_Call) Run(run func()) *MockRWChannelStore_GetBufferChannelInfo_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockRWChannelStore_GetBufferChannelInfo_Call) Return(_a0 *NodeChannelInfo) *MockRWChannelStore_GetBufferChannelInfo_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRWChannelStore_GetBufferChannelInfo_Call) RunAndReturn(run func() *NodeChannelInfo) *MockRWChannelStore_GetBufferChannelInfo_Call {
	_c.Call.Return(run)
	return _c
}

// GetChannels provides a mock function with given fields:
func (_m *MockRWChannelStore) GetChannels() []*NodeChannelInfo {
	ret := _m.Called()

	var r0 []*NodeChannelInfo
	if rf, ok := ret.Get(0).(func() []*NodeChannelInfo); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*NodeChannelInfo)
		}
	}

	return r0
}

// MockRWChannelStore_GetChannels_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetChannels'
type MockRWChannelStore_GetChannels_Call struct {
	*mock.Call
}

// GetChannels is a helper method to define mock.On call
func (_e *MockRWChannelStore_Expecter) GetChannels() *MockRWChannelStore_GetChannels_Call {
	return &MockRWChannelStore_GetChannels_Call{Call: _e.mock.On("GetChannels")}
}

func (_c *MockRWChannelStore_GetChannels_Call) Run(run func()) *MockRWChannelStore_GetChannels_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockRWChannelStore_GetChannels_Call) Return(_a0 []*NodeChannelInfo) *MockRWChannelStore_GetChannels_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRWChannelStore_GetChannels_Call) RunAndReturn(run func() []*NodeChannelInfo) *MockRWChannelStore_GetChannels_Call {
	_c.Call.Return(run)
	return _c
}

// GetNode provides a mock function with given fields: nodeID
func (_m *MockRWChannelStore) GetNode(nodeID int64) *NodeChannelInfo {
	ret := _m.Called(nodeID)

	var r0 *NodeChannelInfo
	if rf, ok := ret.Get(0).(func(int64) *NodeChannelInfo); ok {
		r0 = rf(nodeID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*NodeChannelInfo)
		}
	}

	return r0
}

// MockRWChannelStore_GetNode_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetNode'
type MockRWChannelStore_GetNode_Call struct {
	*mock.Call
}

// GetNode is a helper method to define mock.On call
//   - nodeID int64
func (_e *MockRWChannelStore_Expecter) GetNode(nodeID interface{}) *MockRWChannelStore_GetNode_Call {
	return &MockRWChannelStore_GetNode_Call{Call: _e.mock.On("GetNode", nodeID)}
}

func (_c *MockRWChannelStore_GetNode_Call) Run(run func(nodeID int64)) *MockRWChannelStore_GetNode_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int64))
	})
	return _c
}

func (_c *MockRWChannelStore_GetNode_Call) Return(_a0 *NodeChannelInfo) *MockRWChannelStore_GetNode_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRWChannelStore_GetNode_Call) RunAndReturn(run func(int64) *NodeChannelInfo) *MockRWChannelStore_GetNode_Call {
	_c.Call.Return(run)
	return _c
}

// GetNodeChannelCount provides a mock function with given fields: nodeID
func (_m *MockRWChannelStore) GetNodeChannelCount(nodeID int64) int {
	ret := _m.Called(nodeID)

	var r0 int
	if rf, ok := ret.Get(0).(func(int64) int); ok {
		r0 = rf(nodeID)
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// MockRWChannelStore_GetNodeChannelCount_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetNodeChannelCount'
type MockRWChannelStore_GetNodeChannelCount_Call struct {
	*mock.Call
}

// GetNodeChannelCount is a helper method to define mock.On call
//   - nodeID int64
func (_e *MockRWChannelStore_Expecter) GetNodeChannelCount(nodeID interface{}) *MockRWChannelStore_GetNodeChannelCount_Call {
	return &MockRWChannelStore_GetNodeChannelCount_Call{Call: _e.mock.On("GetNodeChannelCount", nodeID)}
}

func (_c *MockRWChannelStore_GetNodeChannelCount_Call) Run(run func(nodeID int64)) *MockRWChannelStore_GetNodeChannelCount_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int64))
	})
	return _c
}

func (_c *MockRWChannelStore_GetNodeChannelCount_Call) Return(_a0 int) *MockRWChannelStore_GetNodeChannelCount_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRWChannelStore_GetNodeChannelCount_Call) RunAndReturn(run func(int64) int) *MockRWChannelStore_GetNodeChannelCount_Call {
	_c.Call.Return(run)
	return _c
}

// GetNodes provides a mock function with given fields:
func (_m *MockRWChannelStore) GetNodes() []int64 {
	ret := _m.Called()

	var r0 []int64
	if rf, ok := ret.Get(0).(func() []int64); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]int64)
		}
	}

	return r0
}

// MockRWChannelStore_GetNodes_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetNodes'
type MockRWChannelStore_GetNodes_Call struct {
	*mock.Call
}

// GetNodes is a helper method to define mock.On call
func (_e *MockRWChannelStore_Expecter) GetNodes() *MockRWChannelStore_GetNodes_Call {
	return &MockRWChannelStore_GetNodes_Call{Call: _e.mock.On("GetNodes")}
}

func (_c *MockRWChannelStore_GetNodes_Call) Run(run func()) *MockRWChannelStore_GetNodes_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockRWChannelStore_GetNodes_Call) Return(_a0 []int64) *MockRWChannelStore_GetNodes_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRWChannelStore_GetNodes_Call) RunAndReturn(run func() []int64) *MockRWChannelStore_GetNodes_Call {
	_c.Call.Return(run)
	return _c
}

// GetNodesChannels provides a mock function with given fields:
func (_m *MockRWChannelStore) GetNodesChannels() []*NodeChannelInfo {
	ret := _m.Called()

	var r0 []*NodeChannelInfo
	if rf, ok := ret.Get(0).(func() []*NodeChannelInfo); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*NodeChannelInfo)
		}
	}

	return r0
}

// MockRWChannelStore_GetNodesChannels_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetNodesChannels'
type MockRWChannelStore_GetNodesChannels_Call struct {
	*mock.Call
}

// GetNodesChannels is a helper method to define mock.On call
func (_e *MockRWChannelStore_Expecter) GetNodesChannels() *MockRWChannelStore_GetNodesChannels_Call {
	return &MockRWChannelStore_GetNodesChannels_Call{Call: _e.mock.On("GetNodesChannels")}
}

func (_c *MockRWChannelStore_GetNodesChannels_Call) Run(run func()) *MockRWChannelStore_GetNodesChannels_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockRWChannelStore_GetNodesChannels_Call) Return(_a0 []*NodeChannelInfo) *MockRWChannelStore_GetNodesChannels_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRWChannelStore_GetNodesChannels_Call) RunAndReturn(run func() []*NodeChannelInfo) *MockRWChannelStore_GetNodesChannels_Call {
	_c.Call.Return(run)
	return _c
}

// Reload provides a mock function with given fields:
func (_m *MockRWChannelStore) Reload() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockRWChannelStore_Reload_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Reload'
type MockRWChannelStore_Reload_Call struct {
	*mock.Call
}

// Reload is a helper method to define mock.On call
func (_e *MockRWChannelStore_Expecter) Reload() *MockRWChannelStore_Reload_Call {
	return &MockRWChannelStore_Reload_Call{Call: _e.mock.On("Reload")}
}

func (_c *MockRWChannelStore_Reload_Call) Run(run func()) *MockRWChannelStore_Reload_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockRWChannelStore_Reload_Call) Return(_a0 error) *MockRWChannelStore_Reload_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRWChannelStore_Reload_Call) RunAndReturn(run func() error) *MockRWChannelStore_Reload_Call {
	_c.Call.Return(run)
	return _c
}

// Update provides a mock function with given fields: op
func (_m *MockRWChannelStore) Update(op ChannelOpSet) error {
	ret := _m.Called(op)

	var r0 error
	if rf, ok := ret.Get(0).(func(ChannelOpSet) error); ok {
		r0 = rf(op)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockRWChannelStore_Update_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Update'
type MockRWChannelStore_Update_Call struct {
	*mock.Call
}

// Update is a helper method to define mock.On call
//   - op ChannelOpSet
func (_e *MockRWChannelStore_Expecter) Update(op interface{}) *MockRWChannelStore_Update_Call {
	return &MockRWChannelStore_Update_Call{Call: _e.mock.On("Update", op)}
}

func (_c *MockRWChannelStore_Update_Call) Run(run func(op ChannelOpSet)) *MockRWChannelStore_Update_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(ChannelOpSet))
	})
	return _c
}

func (_c *MockRWChannelStore_Update_Call) Return(_a0 error) *MockRWChannelStore_Update_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRWChannelStore_Update_Call) RunAndReturn(run func(ChannelOpSet) error) *MockRWChannelStore_Update_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockRWChannelStore creates a new instance of MockRWChannelStore. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockRWChannelStore(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockRWChannelStore {
	mock := &MockRWChannelStore{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
