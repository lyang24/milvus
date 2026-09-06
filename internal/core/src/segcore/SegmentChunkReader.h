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
#pragma once

#include <stdint.h>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include <boost/core/span.hpp>
#include "boost/variant/variant.hpp"
#include "cachinglayer/CacheSlot.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "index/Index.h"
#include "index/ScalarIndex.h"
#include "segcore/SegmentInterface.h"

namespace milvus::segcore {

using data_access_type = std::optional<boost::variant<bool,
                                                      int8_t,
                                                      int16_t,
                                                      int32_t,
                                                      int64_t,
                                                      float,
                                                      double,
                                                      std::string,
                                                      std::string_view>>;

using ChunkDataAccessor = std::function<const data_access_type(int)>;
using MultipleChunkDataAccessor = std::function<const data_access_type()>;
using PinnedIndexView = boost::span<const PinWrapper<const index::IndexBase*>>;

// A query-lifetime, fixed-width field reader. Source selection and scalar-index
// casts happen once when the reader is prepared; each gather then writes typed
// values and validity directly into caller-owned buffers. The PinWrappers
// backing pinned_index must outlive this reader.
template <typename T>
class PreparedFieldReader {
    static_assert(std::is_fundamental_v<T>);

 public:
    PreparedFieldReader(milvus::OpContext* op_ctx,
                        const SegmentInternalInterface* segment,
                        int64_t active_count,
                        FieldId field_id,
                        PinnedIndexView pinned_index)
        : op_ctx_(op_ctx),
          segment_(segment),
          active_count_(active_count),
          field_id_(field_id),
          size_per_chunk_(segment->size_per_chunk()) {
        scalar_indexes_.reserve(pinned_index.size());
        for (const auto& pinned : pinned_index) {
            auto scalar_index =
                dynamic_cast<const index::ScalarIndex<T>*>(pinned.get());
            AssertInfo(scalar_index != nullptr,
                       "field {} prepared reader index type mismatch",
                       field_id_.get());
            if (!scalar_index->HasRawData()) {
                scalar_indexes_.clear();
                break;
            }
            scalar_indexes_.push_back(scalar_index);
        }
        if (scalar_indexes_.empty()) {
            auto source = segment_->GetPreparedFieldDataSource(field_id_);
            sealed_column_ = std::move(source.sealed_column);
            growing_validity_ = std::move(source.growing_validity);
            if (source.growing_data != nullptr) {
                growing_data_ = dynamic_cast<const ConcurrentVector<T>*>(
                    source.growing_data);
                AssertInfo(growing_data_ != nullptr,
                           "field {} prepared growing data type mismatch",
                           field_id_.get());
            }
        }
    }

    T*
    ScratchValues(int64_t count) {
        AssertInfo(count >= 0, "prepared reader count must be non-negative");
        const auto requested = static_cast<size_t>(count);
        if (requested > scratch_capacity_) {
            scratch_values_ = std::make_unique<T[]>(requested);
            scratch_capacity_ = requested;
        }
        return scratch_values_.get();
    }

    TargetBitmap&
    ScratchValidity(int64_t count) {
        AssertInfo(count >= 0, "prepared reader count must be non-negative");
        scratch_validity_.resize(count);
        scratch_validity_.set();
        return scratch_validity_;
    }

    template <typename Bitmap>
    void
    Gather(const int64_t* offsets, int64_t count, T* values, Bitmap& validity) {
        AssertInfo(count >= 0, "prepared reader count must be non-negative");
        AssertInfo(static_cast<int64_t>(validity.size()) == count,
                   "prepared reader validity size {} differs from count {}",
                   validity.size(),
                   count);
        if (count == 0) {
            return;
        }

        if (growing_data_ != nullptr) {
            validity.set();
            if (growing_validity_ != nullptr) {
                const auto requested = static_cast<size_t>(count);
                if (requested > validity_values_capacity_) {
                    validity_values_ = std::make_unique<bool[]>(requested);
                    validity_values_capacity_ = requested;
                }
                growing_validity_->bulk_is_valid(
                    offsets, count, validity_values_.get());
                for (int64_t i = 0; i < count; ++i) {
                    validity.set(i, validity_values_[i]);
                }
            }
            auto chunks = growing_data_->acquire_chunks();
            for (int64_t i = 0; i < count; ++i) {
                AssertInfo(offsets[i] >= 0 && offsets[i] < active_count_,
                           "field {} offset {} out of range [0, {})",
                           field_id_.get(),
                           offsets[i],
                           active_count_);
                values[i] =
                    *growing_data_->get_physical_element(chunks, offsets[i]);
            }
            return;
        }

        if (sealed_column_ != nullptr) {
            if (sealed_column_->IsNullable()) {
                sealed_column_->BulkIsValid(
                    op_ctx_,
                    [&validity](bool valid, size_t i) {
                        validity.set(i, valid);
                    },
                    offsets,
                    count);
            } else {
                validity.set();
            }
            sealed_column_->BulkPrimitiveValueAt(op_ctx_,
                                                 values,
                                                 offsets,
                                                 count,
                                                 /*small_int_raw_type=*/true);
            return;
        }

        AssertInfo(!scalar_indexes_.empty(),
                   "field {} has no prepared raw data source",
                   field_id_.get());

        validity.set();
        for (int64_t i = 0; i < count; ++i) {
            auto [index_id, index_offset] = ResolveIndexOffset(offsets[i]);
            AssertInfo(
                index_id < scalar_indexes_.size(),
                "field {} index chunk {} exceeds prepared index count {}",
                field_id_.get(),
                index_id,
                scalar_indexes_.size());
            auto value =
                scalar_indexes_[index_id]->Reverse_Lookup(index_offset);
            if (value.has_value()) {
                values[i] = value.value();
            } else {
                validity.reset(i);
            }
        }
    }

    template <typename Bitmap>
    void
    GatherRange(int64_t start, int64_t count, T* values, Bitmap& validity) {
        AssertInfo(count >= 0, "prepared reader count must be non-negative");
        offsets_.resize(count);
        for (int64_t i = 0; i < count; ++i) {
            offsets_[i] = start + i;
        }
        Gather(offsets_.data(), count, values, validity);
    }

    template <typename Bitmap>
    void
    Gather(const int32_t* offsets, int64_t count, T* values, Bitmap& validity) {
        AssertInfo(count >= 0, "prepared reader count must be non-negative");
        offsets_.resize(count);
        for (int64_t i = 0; i < count; ++i) {
            offsets_[i] = offsets[i];
        }
        Gather(offsets_.data(), count, values, validity);
    }

 private:
    std::pair<size_t, int64_t>
    ResolveIndexOffset(int64_t offset) const {
        AssertInfo(offset >= 0 && offset < active_count_,
                   "field {} offset {} out of range [0, {})",
                   field_id_.get(),
                   offset,
                   active_count_);
        if (scalar_indexes_.size() == 1) {
            return {0, offset};
        }
        if (segment_->type() == SegmentType::Growing ||
            !segment_->is_chunked()) {
            return {static_cast<size_t>(offset / size_per_chunk_),
                    offset % size_per_chunk_};
        }
        auto [chunk_id, chunk_offset] =
            segment_->get_chunk_by_offset(field_id_, offset);
        return {static_cast<size_t>(chunk_id), chunk_offset};
    }

    milvus::OpContext* op_ctx_;
    const SegmentInternalInterface* segment_;
    int64_t active_count_;
    FieldId field_id_;
    int64_t size_per_chunk_;
    std::vector<const index::ScalarIndex<T>*> scalar_indexes_;
    const ConcurrentVector<T>* growing_data_{nullptr};
    ThreadSafeValidDataPtr growing_validity_;
    std::shared_ptr<ChunkedColumnInterface> sealed_column_;
    FixedVector<int64_t> offsets_;
    std::unique_ptr<T[]> scratch_values_;
    size_t scratch_capacity_{0};
    TargetBitmap scratch_validity_;
    std::unique_ptr<bool[]> validity_values_;
    size_t validity_values_capacity_{0};
};

using PreparedFieldReaderVariant = std::variant<std::monostate,
                                                PreparedFieldReader<bool>,
                                                PreparedFieldReader<int8_t>,
                                                PreparedFieldReader<int16_t>,
                                                PreparedFieldReader<int32_t>,
                                                PreparedFieldReader<int64_t>,
                                                PreparedFieldReader<float>,
                                                PreparedFieldReader<double>>;

inline PreparedFieldReaderVariant
PrepareFieldReader(milvus::OpContext* op_ctx,
                   const SegmentInternalInterface* segment,
                   int64_t active_count,
                   DataType data_type,
                   FieldId field_id,
                   PinnedIndexView pinned_index) {
#define PREPARE_FIELD_READER_CASE(DATA_TYPE, CPP_TYPE)         \
    case DATA_TYPE:                                            \
        return PreparedFieldReaderVariant(                     \
            std::in_place_type<PreparedFieldReader<CPP_TYPE>>, \
            op_ctx,                                            \
            segment,                                           \
            active_count,                                      \
            field_id,                                          \
            pinned_index)

    switch (data_type) {
        PREPARE_FIELD_READER_CASE(DataType::BOOL, bool);
        PREPARE_FIELD_READER_CASE(DataType::INT8, int8_t);
        PREPARE_FIELD_READER_CASE(DataType::INT16, int16_t);
        PREPARE_FIELD_READER_CASE(DataType::INT32, int32_t);
        PREPARE_FIELD_READER_CASE(DataType::INT64, int64_t);
        PREPARE_FIELD_READER_CASE(DataType::TIMESTAMPTZ, int64_t);
        PREPARE_FIELD_READER_CASE(DataType::FLOAT, float);
        PREPARE_FIELD_READER_CASE(DataType::DOUBLE, double);
        default:
            return std::monostate{};
    }

#undef PREPARE_FIELD_READER_CASE
}

// Helper to extract a value of type T from data_access_type.
// For std::string, handles both std::string and std::string_view in the variant.
// Uses boost::apply_visitor to avoid ADL conflicts between boost::variant::get
// and boost::array::get.
namespace detail {
template <typename T>
struct ValueExtractor : public boost::static_visitor<T> {
    ValueExtractor() = default;

    T
    operator()(const T& val) const {
        return val;
    }
    template <typename U>
    T
    operator()(const U&) const {
        // Both types are selected from internal schema/accessor state, so a
        // mismatch is an internal contract violation rather than bad input.
        ThrowInfo(UnexpectedError, "unexpected type in data_access_type");
    }
};

template <>
struct ValueExtractor<std::string> : public boost::static_visitor<std::string> {
    ValueExtractor() = default;

    std::string
    operator()(const std::string& s) const {
        return s;
    }
    std::string
    operator()(std::string_view sv) const {
        return std::string(sv);
    }
    template <typename U>
    std::string
    operator()(const U&) const {
        // Both types are selected from internal schema/accessor state, so a
        // mismatch is an internal contract violation rather than bad input.
        ThrowInfo(UnexpectedError, "unexpected type in data_access_type");
    }
};
}  // namespace detail

template <typename T>
T
get_from_variant(const data_access_type& opt) {
    return boost::apply_visitor(detail::ValueExtractor<T>{}, opt.value());
}

class SegmentChunkReader {
 public:
    SegmentChunkReader(milvus::OpContext* op_ctx,
                       const segcore::SegmentInternalInterface* segment,
                       int64_t active_count)
        : segment_(segment),
          active_count_(active_count),
          size_per_chunk_(segment->size_per_chunk()),
          op_ctx_(op_ctx) {
    }

    MultipleChunkDataAccessor
    GetMultipleChunkDataAccessor(DataType data_type,
                                 FieldId field_id,
                                 int64_t& current_chunk_id,
                                 int64_t& current_chunk_pos,
                                 PinnedIndexView pinned_index) const;

    ChunkDataAccessor
    GetChunkDataAccessor(DataType data_type,
                         FieldId field_id,
                         int chunk_id,
                         PinnedIndexView pinned_index) const;

    void
    MoveCursorForMultipleChunk(int64_t& current_chunk_id,
                               int64_t& current_chunk_pos,
                               const FieldId field_id,
                               const int64_t num_chunk,
                               const int64_t batch_size) const {
        int64_t segment_row_count = segment_->get_row_count();
        int64_t current_offset =
            segment_->num_rows_until_chunk(field_id, current_chunk_id) +
            current_chunk_pos;
        int64_t target_offset = current_offset + batch_size;

        if (target_offset >= segment_row_count) {
            current_chunk_id = num_chunk - 1;
            current_chunk_pos =
                segment_row_count -
                segment_->num_rows_until_chunk(field_id, current_chunk_id);
            return;
        }
        auto [chunk_id, chunk_pos] =
            segment_->get_chunk_by_offset(field_id, target_offset);
        current_chunk_id = chunk_id;
        current_chunk_pos = chunk_pos;
    }

    void
    MoveCursorForSingleChunk(int64_t& current_chunk_id,
                             int64_t& current_chunk_pos,
                             const int64_t num_chunk,
                             const int64_t batch_size) const {
        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id; chunk_id < num_chunk;
             ++chunk_id) {
            auto chunk_size = chunk_id == num_chunk - 1
                                  ? active_count_ - chunk_id * SizePerChunk()
                                  : SizePerChunk();

            for (int64_t i = chunk_id == current_chunk_id ? current_chunk_pos
                                                          : 0;
                 i < chunk_size;
                 ++i) {
                if (++processed_rows >= batch_size) {
                    current_chunk_id = chunk_id;
                    current_chunk_pos = i + 1;
                    return;
                }
            }
        }
    }

    int64_t
    SizePerChunk() const {
        return size_per_chunk_;
    }

    const int64_t active_count_;
    const segcore::SegmentInternalInterface* segment_;

 private:
    template <typename T>
    MultipleChunkDataAccessor
    GetMultipleChunkDataAccessor(FieldId field_id,
                                 int64_t& current_chunk_id,
                                 int64_t& current_chunk_pos,
                                 PinnedIndexView pinned_index) const;

    template <typename T>
    ChunkDataAccessor
    GetChunkDataAccessor(FieldId field_id,
                         int chunk_id,
                         PinnedIndexView pinned_index) const;

    const int64_t size_per_chunk_;
    milvus::OpContext* op_ctx_;
};

}  // namespace milvus::segcore
