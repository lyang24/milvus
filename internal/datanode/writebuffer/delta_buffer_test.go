package writebuffer

import (
	"fmt"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type DeltaBufferSuite struct {
	suite.Suite
}

func (s *DeltaBufferSuite) TestBuffer() {
	s.Run("int64_pk", func() {
		deltaBuffer := NewDeltaBuffer()

		tss := lo.RepeatBy(100, func(idx int) uint64 { return tsoutil.ComposeTSByTime(time.Now(), int64(idx)) })
		pks := lo.Map(tss, func(ts uint64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(int64(ts)) })

		memSize := deltaBuffer.Buffer(pks, tss, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		s.EqualValues(100*8*2, memSize)
	})

	s.Run("string_pk", func() {
		deltaBuffer := NewDeltaBuffer()

		tss := lo.RepeatBy(100, func(idx int) uint64 { return tsoutil.ComposeTSByTime(time.Now(), int64(idx)) })
		pks := lo.Map(tss, func(ts uint64, idx int) storage.PrimaryKey {
			return storage.NewVarCharPrimaryKey(fmt.Sprintf("%03d", idx))
		})

		memSize := deltaBuffer.Buffer(pks, tss, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		s.EqualValues(100*8+100*3, memSize)
	})
}

func (s *DeltaBufferSuite) TestRenew() {
	deltaBuffer := NewDeltaBuffer()

	result := deltaBuffer.Renew()
	s.Nil(result)
	s.True(deltaBuffer.IsEmpty())

	tss := lo.RepeatBy(100, func(idx int) uint64 { return tsoutil.ComposeTSByTime(time.Now(), int64(idx)) })
	pks := lo.Map(tss, func(ts uint64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(int64(ts)) })

	deltaBuffer.Buffer(pks, tss, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})

	result = deltaBuffer.Renew()
	s.NotNil(result)
	s.True(deltaBuffer.IsEmpty())

	s.ElementsMatch(tss, result.Tss)
	s.ElementsMatch(pks, result.Pks)
}

func TestDeltaBuffer(t *testing.T) {
	suite.Run(t, new(DeltaBufferSuite))
}
