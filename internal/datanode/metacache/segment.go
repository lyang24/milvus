// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metacache

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type SegmentInfo struct {
	segmentID        int64
	partitionID      int64
	state            commonpb.SegmentState
	startPosition    *msgpb.MsgPosition
	checkpoint       *msgpb.MsgPosition
	startPosRecorded bool
	numOfRows        int64
	bfs              *BloomFilterSet
	compactTo        int64
}

func (s *SegmentInfo) SegmentID() int64 {
	return s.segmentID
}

func (s *SegmentInfo) PartitionID() int64 {
	return s.partitionID
}

func (s *SegmentInfo) State() commonpb.SegmentState {
	return s.state
}

func (s *SegmentInfo) NumOfRows() int64 {
	return s.numOfRows
}

func (s *SegmentInfo) StartPosition() *msgpb.MsgPosition {
	return s.startPosition
}

func (s *SegmentInfo) Checkpoint() *msgpb.MsgPosition {
	return s.checkpoint
}

func (s *SegmentInfo) GetHistory() []*storage.PkStatistics {
	return s.bfs.GetHistory()
}

func (s *SegmentInfo) CompactTo() int64 {
	return s.compactTo
}

func (s *SegmentInfo) GetBloomFilterSet() *BloomFilterSet {
	return s.bfs
}

func (s *SegmentInfo) Clone() *SegmentInfo {
	return &SegmentInfo{
		segmentID:        s.segmentID,
		partitionID:      s.partitionID,
		state:            s.state,
		startPosition:    s.startPosition,
		checkpoint:       s.checkpoint,
		startPosRecorded: s.startPosRecorded,
		numOfRows:        s.numOfRows,
		bfs:              s.bfs,
		compactTo:        s.compactTo,
	}
}

func NewSegmentInfo(info *datapb.SegmentInfo, bfs *BloomFilterSet) *SegmentInfo {
	return &SegmentInfo{
		segmentID:        info.GetID(),
		partitionID:      info.GetPartitionID(),
		state:            info.GetState(),
		numOfRows:        info.GetNumOfRows(),
		startPosition:    info.GetStartPosition(),
		checkpoint:       info.GetDmlPosition(),
		startPosRecorded: true,
		bfs:              bfs,
	}
}
