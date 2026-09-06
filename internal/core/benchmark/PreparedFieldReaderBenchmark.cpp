// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <memory>
#include <vector>

#include <benchmark/benchmark.h>

#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

namespace milvus::segcore {
namespace {

constexpr int64_t kRowCount = 1 << 20;

class PreparedFieldReaderBenchmark : public benchmark::Fixture {
 public:
    void
    SetUp(const benchmark::State& state) override {
        schema_ = std::make_shared<Schema>();
        auto pk = schema_->AddDebugField("pk", DataType::INT64);
        schema_->set_primary_field_id(pk);
        value_field_ = schema_->AddDebugField("value", DataType::INT64, true);

        auto raw_data = DataGen(schema_,
                                kRowCount,
                                /*seed=*/42,
                                /*ts_offset=*/0,
                                /*repeat_count=*/1,
                                /*array_len=*/10,
                                /*group_count=*/1,
                                /*random_pk=*/false,
                                /*random_val=*/true,
                                /*random_valid=*/true);
        segment_ = CreateGrowingSegment(schema_, empty_index_meta);
        segment_->PreInsert(kRowCount);
        segment_->Insert(0,
                         kRowCount,
                         raw_data.row_ids_.data(),
                         raw_data.timestamps_.data(),
                         raw_data.raw_);

        legacy_reader_ = std::make_unique<SegmentChunkReader>(
            nullptr, segment_.get(), kRowCount);
        prepared_reader_ =
            std::make_unique<PreparedFieldReader<int64_t>>(nullptr,
                                                           segment_.get(),
                                                           kRowCount,
                                                           value_field_,
                                                           PinnedIndexView{});

        const auto batch_size = static_cast<size_t>(state.range(0));
        offsets_.resize(batch_size);
        values_.resize(batch_size);
        validity_.resize(batch_size);
        for (size_t i = 0; i < batch_size; ++i) {
            offsets_[i] = static_cast<int64_t>(
                (i * 2654435761ULL + 1013904223ULL) % kRowCount);
        }
    }

 protected:
    SchemaPtr schema_;
    FieldId value_field_{0};
    SegmentGrowingPtr segment_;
    std::unique_ptr<SegmentChunkReader> legacy_reader_;
    std::unique_ptr<PreparedFieldReader<int64_t>> prepared_reader_;
    std::vector<int64_t> offsets_;
    std::vector<int64_t> values_;
    TargetBitmap validity_;
};

BENCHMARK_DEFINE_F(PreparedFieldReaderBenchmark, LegacyVariantAccess)
(benchmark::State& state) {
    const auto size_per_chunk = legacy_reader_->SizePerChunk();
    for (auto _ : state) {
        int64_t cached_chunk_id = -1;
        ChunkDataAccessor accessor;
        validity_.set();
        for (size_t i = 0; i < offsets_.size(); ++i) {
            const auto offset = offsets_[i];
            const auto chunk_id = offset / size_per_chunk;
            const auto chunk_offset = offset % size_per_chunk;
            if (chunk_id != cached_chunk_id) {
                accessor = legacy_reader_->GetChunkDataAccessor(
                    DataType::INT64, value_field_, chunk_id, {});
                cached_chunk_id = chunk_id;
            }
            auto value = accessor(chunk_offset);
            if (value.has_value()) {
                values_[i] = get_from_variant<int64_t>(value);
            } else {
                validity_.reset(i);
            }
        }
        benchmark::DoNotOptimize(values_.data());
        benchmark::DoNotOptimize(validity_.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * offsets_.size());
}

BENCHMARK_DEFINE_F(PreparedFieldReaderBenchmark, PreparedBatchAccess)
(benchmark::State& state) {
    for (auto _ : state) {
        prepared_reader_->Gather(
            offsets_.data(), offsets_.size(), values_.data(), validity_);
        benchmark::DoNotOptimize(values_.data());
        benchmark::DoNotOptimize(validity_.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * offsets_.size());
}

BENCHMARK_REGISTER_F(PreparedFieldReaderBenchmark, LegacyVariantAccess)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024);
BENCHMARK_REGISTER_F(PreparedFieldReaderBenchmark, PreparedBatchAccess)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024);

}  // namespace
}  // namespace milvus::segcore

BENCHMARK_MAIN();
