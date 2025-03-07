package writebuffer

import (
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	// DeletePolicyBFPKOracle is the const config value for using bf pk oracle as delete policy
	DeletePolicyBFPkOracle = `bloom_filter_pkoracle`

	// DeletePolicyL0Delta is the const config value for using L0 delta as deleta policy.
	DeletePolicyL0Delta = `l0_delta`
)

type WriteBufferOption func(opt *writeBufferOption)

type writeBufferOption struct {
	deletePolicy string
	idAllocator  allocator.Interface
	syncPolicies []SyncPolicy

	pkStatsFactory metacache.PkStatsFactory
	broker         broker.Broker
}

func defaultWBOption() *writeBufferOption {
	return &writeBufferOption{
		// TODO use l0 delta as default after implementation.
		deletePolicy: paramtable.Get().DataNodeCfg.DeltaPolicy.GetValue(),
		syncPolicies: []SyncPolicy{
			SyncFullBuffer,
			GetSyncStaleBufferPolicy(paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)),
		},
	}
}

func WithDeletePolicy(policy string) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.deletePolicy = policy
	}
}

func WithIDAllocator(allocator allocator.Interface) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.idAllocator = allocator
	}
}

func WithPKStatsFactory(factory metacache.PkStatsFactory) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.pkStatsFactory = factory
	}
}

func WithBroker(broker broker.Broker) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.broker = broker
	}
}

func WithSyncPolicy(policy SyncPolicy) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.syncPolicies = append(opt.syncPolicies, policy)
	}
}
