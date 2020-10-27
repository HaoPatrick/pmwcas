// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <gtest/gtest.h>
#include <stdlib.h>

#include <atomic>

#include "common/allocator_internal.h"
#include "environment/environment_linux.h"
#include "include/allocator.h"
#include "include/environment.h"
#include "include/pmwcas.h"
#include "include/status.h"
#include "mwcas/mwcas.h"
#include "util/auto_ptr.h"
#include "util/random_number_generator.h"

namespace pmwcas {

const uint32_t kDescriptorPoolSize = 0x400;
const uint32_t kTestArraySize = 0x80;
const uint32_t kWordsToUpdate = 4;

struct AllocTestLinkedListNode {
  uint64_t key;
  nv_ptr<AllocTestLinkedListNode> next;
};

#ifdef PMDK
struct RootObj {
  PMEMoid pool;
  PMEMoid array;
};

class PMwCASMemorySafetyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto thread_count = Environment::Get()->GetCoreCount();
    allocator_ = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    auto root_obj = (RootObj *)allocator_->GetRoot(sizeof(RootObj));

    pmemobj_zalloc(allocator_->GetPool(), &root_obj->pool,
                   sizeof(pmwcas::DescriptorPool), TOID_TYPE_NUM(char));
    new (pmemobj_direct(root_obj->pool))
        pmwcas::DescriptorPool(kDescriptorPoolSize, thread_count);

    pmemobj_zalloc(allocator_->GetPool(), &root_obj->array,
                   sizeof(nv_ptr<AllocTestLinkedListNode>) * kTestArraySize * 2,
                   TOID_TYPE_NUM(char));

    pool_ = (pmwcas::DescriptorPool *)pmemobj_direct(root_obj->pool);
    array1_ =
        (nv_ptr<AllocTestLinkedListNode> *)pmemobj_direct(root_obj->array);
    array2_ = &array1_[kTestArraySize];

    {
      PMEMobjpool *pop = allocator_->GetPool();
      TOID(char) iter;
      POBJ_FOREACH_TYPE(pop, iter) {
        void *addr = pmemobj_direct(iter.oid);
        base_allocations_.insert(addr);
      }
    }

    // prefill array2_ with some nodes
    for (uint32_t i = 0; i < kTestArraySize; ++i) {
      auto ptr = reinterpret_cast<uint64_t *>(&array2_[i]);
      allocator_->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode), false);
      new (array2_[i]) AllocTestLinkedListNode{i, nullptr};
    }
    // leave array1_ empty (i.e. with nullptr)

    {
      PMEMobjpool *pop = allocator_->GetPool();
      TOID(char) iter;
      POBJ_FOREACH_TYPE(pop, iter) {
        void *addr = pmemobj_direct(iter.oid);
        if (!contains(base_allocations_, addr)) {
          user_allocations_.insert(addr);
        }
      }
    }
  }

  PMDKAllocator *allocator_;
  pmwcas::DescriptorPool *pool_;
  nv_ptr<AllocTestLinkedListNode> *array1_;
  nv_ptr<AllocTestLinkedListNode> *array2_;

  std::set<void *> base_allocations_;
  std::set<void *> user_allocations_;

  bool contains(std::set<void *> &allocations, void *addr) {
    return allocations.find(addr) != allocations.end();
  }
};

TEST_F(PMwCASMemorySafetyTest, SingleThreadAllocationSuccess) {
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);

  nv_ptr<AllocTestLinkedListNode> *addresses[kWordsToUpdate];
  nv_ptr<AllocTestLinkedListNode> values[kWordsToUpdate];

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    addresses[i] = nullptr;
    values[i] = nullptr;
  }

  nv_ptr<AllocTestLinkedListNode> *array = array1_;

  pool_->GetEpoch()->Protect();

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
  retry:
    uint64_t idx = rng.Generate();
    for (uint32_t j = 0; j < i; ++j) {
      if (addresses[j] == &array[idx]) {
        goto retry;
      }
    }

    addresses[i] = &array[idx];
    values[i] =
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(&array[idx])
            ->GetValueProtected();
    EXPECT_EQ(values[i], nullptr);
  }

  auto descriptor = pool_->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor.GetRaw());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto idx = descriptor.ReserveAndAddEntry((uint64_t *)addresses[i],
                                             (uint64_t)values[i],
                                             Descriptor::kRecycleNewOnFailure);
    uint64_t *ptr = descriptor.GetNewValuePtr(idx);
    allocator_->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode));
  }

  EXPECT_TRUE(descriptor.MwCAS());

  std::set<void *> allocated;
  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto addr = static_cast<nv_ptr<AllocTestLinkedListNode>>(
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(addresses[i])
            ->GetValueProtected());
    EXPECT_FALSE(contains(allocated, addr));
    allocated.insert(addr);
  }

  pool_->GetEpoch()->Unprotect();

  std::set<void *> new_allocations;
  {
    PMEMobjpool *pop = allocator_->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations_, addr) &&
          !contains(user_allocations_, addr)) {
        new_allocations.insert(addr);
      }
    }
  }

  ASSERT_EQ(allocated.size(), new_allocations.size());
  for (auto i = allocated.begin(), j = new_allocations.begin();
       i != allocated.end() && j != new_allocations.end(); ++i, ++j) {
    EXPECT_EQ(*i, *j);
  }

  // have to clear the EpochManager entry
  Thread::ClearRegistry(true);
}

TEST_F(PMwCASMemorySafetyTest, SingleThreadAllocationFailure) {
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);

  nv_ptr<AllocTestLinkedListNode> *addresses[kWordsToUpdate];
  nv_ptr<AllocTestLinkedListNode> values[kWordsToUpdate];

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    addresses[i] = nullptr;
    values[i] = nullptr;
  }

  nv_ptr<AllocTestLinkedListNode> *array = array1_;

  pool_->GetEpoch()->Protect();

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
  retry:
    uint64_t idx = rng.Generate();
    for (uint32_t j = 0; j < i; ++j) {
      if (addresses[j] == &array[idx]) {
        goto retry;
      }
    }

    addresses[i] = &array[idx];
    values[i] =
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(&array[idx])
            ->GetValueProtected();
    EXPECT_EQ(values[i], nullptr);
  }

  auto descriptor = pool_->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor.GetRaw());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto idx = descriptor.ReserveAndAddEntry((uint64_t *)addresses[i],
                                             (uint64_t)values[i],
                                             Descriptor::kRecycleNewOnFailure);
    uint64_t *ptr = descriptor.GetNewValuePtr(idx);
    allocator_->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode));
  }

  EXPECT_TRUE(descriptor.Abort().ok());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto addr = static_cast<nv_ptr<AllocTestLinkedListNode>>(
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(addresses[i])
            ->GetValueProtected());
    EXPECT_EQ(addr, nullptr);
  }

  pool_->GetEpoch()->Unprotect();

  // Loop over the entire descriptor pool to ensure that
  // the previous descriptor is recycled
  for (uint32_t i = 0; i < kDescriptorPoolSize; ++i) {
    pool_->GetEpoch()->Protect();
    auto desc = pool_->AllocateDescriptor();
    desc.Abort();
    pool_->GetEpoch()->Unprotect();
  }

  std::set<void *> new_allocations;
  {
    PMEMobjpool *pop = allocator_->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations_, addr) &&
          !contains(user_allocations_, addr)) {
        new_allocations.insert(addr);
      }
    }
  }

  ASSERT_EQ(new_allocations.size(), 0);

  // have to clear the EpochManager entry
  Thread::ClearRegistry(true);
}

TEST_F(PMwCASMemorySafetyTest, SingleThreadAllocationLeak) {
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);

  nv_ptr<AllocTestLinkedListNode> *addresses[kWordsToUpdate];
  nv_ptr<AllocTestLinkedListNode> values[kWordsToUpdate];

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    addresses[i] = nullptr;
    values[i] = nullptr;
  }

  nv_ptr<AllocTestLinkedListNode> *array = array1_;

  pool_->GetEpoch()->Protect();

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
  retry:
    uint64_t idx = rng.Generate();
    for (uint32_t j = 0; j < i; ++j) {
      if (addresses[j] == &array[idx]) {
        goto retry;
      }
    }

    addresses[i] = &array[idx];
    values[i] =
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(&array[idx])
            ->GetValueProtected();
    EXPECT_EQ(values[i], nullptr);
  }

  auto descriptor = pool_->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor.GetRaw());

  std::set<void *> allocated;
  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto idx = descriptor.ReserveAndAddEntry((uint64_t *)addresses[i],
                                             (uint64_t)values[i],
                                             Descriptor::kRecycleNewOnFailure);
    uint64_t *ptr = descriptor.GetNewValuePtr(idx);
    // Incorrect use of AllocateOffset(). This will leak memory persistently.
    allocator_->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode), false);
    allocated.insert(nv_ptr<AllocTestLinkedListNode>(*ptr));
  }

  EXPECT_EQ(allocated.size(), kWordsToUpdate);

  EXPECT_TRUE(descriptor.Abort().ok());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto addr = static_cast<nv_ptr<AllocTestLinkedListNode>>(
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(addresses[i])
            ->GetValueProtected());
    EXPECT_EQ(addr, nullptr);
  }

  pool_->GetEpoch()->Unprotect();

  // Loop over the entire descriptor pool to ensure that
  // the previous descriptor is recycled
  for (uint32_t i = 0; i < kDescriptorPoolSize; ++i) {
    pool_->GetEpoch()->Protect();
    auto desc = pool_->AllocateDescriptor();
    desc.Abort();
    pool_->GetEpoch()->Unprotect();
  }

  std::set<void *> new_allocations;
  {
    PMEMobjpool *pop = allocator_->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations_, addr) &&
          !contains(user_allocations_, addr)) {
        new_allocations.insert(addr);
      }
    }
  }

  ASSERT_EQ(allocated.size(), new_allocations.size());
  for (auto i = allocated.begin(), j = new_allocations.begin();
       i != allocated.end() && j != new_allocations.end(); ++i, ++j) {
    EXPECT_EQ(*i, *j);
  }

  // have to clear the EpochManager entry
  Thread::ClearRegistry(true);
}

#endif
}  // namespace pmwcas

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_alsologtostderr = 1;

#ifdef PMDK
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          "mwcas_mem_safety_test_pool", "mwcas_alloc_layout",
                          static_cast<uint64_t>(1024) * 1024 * 1204 * 1),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
#else
  pmwcas::InitLibrary(
      pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
      pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
#endif
  return RUN_ALL_TESTS();
}
