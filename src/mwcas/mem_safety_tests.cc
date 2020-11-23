// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <fcntl.h>
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
const uint32_t kNumThreads = 4;
const char *kSemaphoreName = "/mwcas-mem-safety-recovery-test-sem";

static bool contains(const std::set<void *> &allocations, void *addr) {
  return allocations.find(addr) != allocations.end();
}

static void insert(std::set<void *> &allocations, void *addr) {
  EXPECT_TRUE(!contains(allocations, addr));
  allocations.insert(addr);
}

struct AllocTestLinkedListNode {
  nv_ptr<AllocTestLinkedListNode> next;
  uint64_t key;
  uint64_t payload;
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
        insert(base_allocations_, addr);
      }
    }

    // prefill array2_ with some nodes
    for (uint32_t i = 0; i < kTestArraySize; ++i) {
      auto ptr = reinterpret_cast<uint64_t *>(&array2_[i]);
      allocator_->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode), false);
      new (array2_[i]) AllocTestLinkedListNode{nullptr, i, 0};
    }
    // leave array1_ empty (i.e. with nullptr)

    {
      PMEMobjpool *pop = allocator_->GetPool();
      TOID(char) iter;
      POBJ_FOREACH_TYPE(pop, iter) {
        void *addr = pmemobj_direct(iter.oid);
        if (!contains(base_allocations_, addr)) {
          insert(user_allocations_, addr);
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
    insert(allocated, addr);
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
        insert(new_allocations, addr);
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
        insert(new_allocations, addr);
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
    insert(allocated, nv_ptr<AllocTestLinkedListNode>(*ptr));
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
        insert(new_allocations, addr);
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

TEST_F(PMwCASMemorySafetyTest, SingleThreadDeallocationSuccess) {
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);

  nv_ptr<AllocTestLinkedListNode> *addresses[kWordsToUpdate];
  nv_ptr<AllocTestLinkedListNode> values[kWordsToUpdate];

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    addresses[i] = nullptr;
    values[i] = nullptr;
  }

  nv_ptr<AllocTestLinkedListNode> *array = array2_;

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
    ASSERT_NE(values[i], nullptr);
    EXPECT_EQ(values[i]->key, idx);
  }

  auto descriptor = pool_->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor.GetRaw());

  std::set<void *> deallocated;
  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto idx =
        descriptor.AddEntry((uint64_t *)addresses[i], (uint64_t)values[i], 0ull,
                            Descriptor::kRecycleOldOnSuccess);
    insert(deallocated, (pmwcas::AllocTestLinkedListNode *)values[i]);
  }

  EXPECT_EQ(deallocated.size(), kWordsToUpdate);

  EXPECT_TRUE(descriptor.MwCAS());

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

  std::set<void *> new_user_allocations;
  {
    PMEMobjpool *pop = allocator_->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations_, addr)) {
        insert(new_user_allocations, addr);
      }
    }
  }

  ASSERT_EQ(deallocated.size() + new_user_allocations.size(),
            user_allocations_.size());
  for (auto &addr : deallocated) {
    EXPECT_TRUE(contains(user_allocations_, addr));
    EXPECT_FALSE(contains(new_user_allocations, addr));
  }
  for (auto &addr : new_user_allocations) {
    EXPECT_TRUE(contains(user_allocations_, addr));
    EXPECT_FALSE(contains(deallocated, addr));
  }

  // have to clear the EpochManager entry
  Thread::ClearRegistry(true);
}

TEST_F(PMwCASMemorySafetyTest, SingleThreadDeallocationFailure) {
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);

  nv_ptr<AllocTestLinkedListNode> *addresses[kWordsToUpdate];
  nv_ptr<AllocTestLinkedListNode> values[kWordsToUpdate];

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    addresses[i] = nullptr;
    values[i] = nullptr;
  }

  nv_ptr<AllocTestLinkedListNode> *array = array2_;

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
    ASSERT_NE(values[i], nullptr);
    EXPECT_EQ(values[i]->key, idx);
  }

  auto descriptor = pool_->AllocateDescriptor();
  EXPECT_NE(nullptr, descriptor.GetRaw());

  std::set<void *> deallocated;
  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto idx =
        descriptor.AddEntry((uint64_t *)addresses[i], (uint64_t)values[i], 0ull,
                            Descriptor::kRecycleOldOnSuccess);
    insert(deallocated, (pmwcas::AllocTestLinkedListNode *)values[i]);
  }

  EXPECT_EQ(deallocated.size(), kWordsToUpdate);

  EXPECT_TRUE(descriptor.Abort().ok());

  for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
    auto addr = static_cast<nv_ptr<AllocTestLinkedListNode>>(
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(addresses[i])
            ->GetValueProtected());
    ASSERT_NE(addr, nullptr);
    EXPECT_EQ(values[i]->key, addresses[i] - array);
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

  std::set<void *> new_user_allocations;
  {
    PMEMobjpool *pop = allocator_->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations_, addr)) {
        insert(new_user_allocations, addr);
      }
    }
  }

  ASSERT_EQ(new_user_allocations.size(), user_allocations_.size());
  for (auto i = new_user_allocations.begin(), j = user_allocations_.begin();
       i != new_user_allocations.end() && j != user_allocations_.end();
       ++i, ++j) {
    EXPECT_EQ(*i, *j);
  }

  for (auto &addr : deallocated) {
    EXPECT_TRUE(contains(new_user_allocations, addr));
  }

  // have to clear the EpochManager entry
  Thread::ClearRegistry(true);
}

TEST_F(PMwCASMemorySafetyTest, SingleThreadSwapNoAlloc) {
  const uint32_t kSwapsToPerform = kTestArraySize / 2;
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);

  nv_ptr<AllocTestLinkedListNode> *addresses[2];
  nv_ptr<AllocTestLinkedListNode> values[2];

  for (uint32_t i = 0; i < 2; ++i) {
    addresses[i] = nullptr;
    values[i] = nullptr;
  }

  nv_ptr<AllocTestLinkedListNode> *array[2] = {array1_, array2_};

  for (uint32_t i = 0; i < kSwapsToPerform; ++i) {
    pool_->GetEpoch()->Protect();

    auto descriptor = pool_->AllocateDescriptor();
    EXPECT_NE(nullptr, descriptor.GetRaw());

    uint64_t idx = rng.Generate();

    for (uint32_t j = 0; j < 2; ++j) {
      addresses[j] = &array[j][idx];
      values[j] =
          reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(&array[j][idx])
              ->GetValueProtected();
    }

    ASSERT_TRUE(values[0] == nullptr or values[1] == nullptr);
    for (uint32_t j = 0; j < 2; ++j) {
      if (values[j]) {
        EXPECT_EQ(values[j]->key, idx);
      }
    }

    for (uint32_t j = 0; j < 2; ++j) {
      auto idx = descriptor.AddEntry(
          (uint64_t *)addresses[j], (uint64_t)values[j],
          (uint64_t)values[1 - j], Descriptor::kRecycleNever);
    }

    EXPECT_TRUE(descriptor.MwCAS());

    pool_->GetEpoch()->Unprotect();
  }

  // Loop over the entire descriptor pool to ensure that
  // the previous descriptors are recycled
  for (uint32_t i = 0; i < kDescriptorPoolSize; ++i) {
    pool_->GetEpoch()->Protect();
    auto desc = pool_->AllocateDescriptor();
    desc.Abort();
    pool_->GetEpoch()->Unprotect();
  }

  std::set<void *> new_user_allocations;
  {
    PMEMobjpool *pop = allocator_->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations_, addr)) {
        insert(new_user_allocations, addr);
      }
    }
  }

  // user allocations should stay the same
  ASSERT_EQ(new_user_allocations.size(), user_allocations_.size());
  for (auto i = new_user_allocations.begin(), j = user_allocations_.begin();
       i != new_user_allocations.end() && j != user_allocations_.end();
       ++i, ++j) {
    EXPECT_EQ(*i, *j);
  }

  for (uint32_t i = 0; i < kTestArraySize; ++i) {
    ASSERT_TRUE(array[0][i] == nullptr or array[1][i] == nullptr);
    for (uint32_t j = 0; j < 2; ++j) {
      if (array[j][i]) {
        EXPECT_EQ(array[j][i]->key, i);
      }
    }
  }

  // have to clear the EpochManager entry
  Thread::ClearRegistry(true);
}

TEST_F(PMwCASMemorySafetyTest, SingleThreadRealloc) {
  const uint32_t kSwapsToPerform = kTestArraySize / 2;
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);

  nv_ptr<AllocTestLinkedListNode> *array = array2_;

  for (uint32_t i = 0; i < kSwapsToPerform; ++i) {
    pool_->GetEpoch()->Protect();

    auto descriptor = pool_->AllocateDescriptor();
    EXPECT_NE(nullptr, descriptor.GetRaw());

    uint64_t idx = rng.Generate();

    nv_ptr<AllocTestLinkedListNode> *addresses = &array[idx];
    nv_ptr<AllocTestLinkedListNode> values =
        reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(&array[idx])
            ->GetValueProtected();

    EXPECT_EQ(values->key, idx);

    auto entry = descriptor.ReserveAndAddEntry(
        (uint64_t *)addresses, (uint64_t)values, Descriptor::kRecycleAlways);

    uint64_t *ptr = descriptor.GetNewValuePtr(entry);
    allocator_->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode));
    AllocTestLinkedListNode *node =
        nv_ptr<AllocTestLinkedListNode>(descriptor.GetNewValue(entry));
    new (node)
        AllocTestLinkedListNode{values->next, values->key, values->payload};

    EXPECT_TRUE(descriptor.MwCAS());

    pool_->GetEpoch()->Unprotect();
  }

  // Loop over the entire descriptor pool to ensure that
  // the previous descriptors are recycled
  for (uint32_t i = 0; i < kDescriptorPoolSize; ++i) {
    pool_->GetEpoch()->Protect();
    auto desc = pool_->AllocateDescriptor();
    desc.Abort();
    pool_->GetEpoch()->Unprotect();
  }

  std::set<void *> new_user_allocations;
  {
    PMEMobjpool *pop = allocator_->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations_, addr)) {
        insert(new_user_allocations, addr);
      }
    }
  }

  // user allocations should stay the same size
  ASSERT_EQ(new_user_allocations.size(), user_allocations_.size());

  for (uint32_t i = 0; i < kTestArraySize; ++i) {
    for (uint32_t j = 0; j < 2; ++j) {
      ASSERT_TRUE(array[i]);
      EXPECT_EQ(array[i]->key, i);
    }
  }

  // have to clear the EpochManager entry
  Thread::ClearRegistry(true);
}

TEST_F(PMwCASMemorySafetyTest, SingleThreadSwapRealloc) {
  const uint32_t kSwapsToPerform = kTestArraySize / 2;
  RandomNumberGenerator rng(rand(), 0, kTestArraySize);

  nv_ptr<AllocTestLinkedListNode> *addresses[2];
  nv_ptr<AllocTestLinkedListNode> values[2];

  for (uint32_t i = 0; i < 2; ++i) {
    addresses[i] = nullptr;
    values[i] = nullptr;
  }

  nv_ptr<AllocTestLinkedListNode> *array[2] = {array1_, array2_};

  for (uint32_t i = 0; i < kSwapsToPerform; ++i) {
    pool_->GetEpoch()->Protect();

    auto descriptor = pool_->AllocateDescriptor();
    EXPECT_NE(nullptr, descriptor.GetRaw());

    uint64_t idx = rng.Generate();

    for (uint32_t j = 0; j < 2; ++j) {
      addresses[j] = &array[j][idx];
      values[j] =
          reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(&array[j][idx])
              ->GetValueProtected();
    }

    ASSERT_TRUE(values[0] == nullptr or values[1] == nullptr);

    uint32_t j = values[0] ? 0 : 1;

    EXPECT_EQ(values[j]->key, idx);

    descriptor.AddEntry((uint64_t *)addresses[j], (uint64_t)values[j], 0ull,
                        Descriptor::kRecycleOldOnSuccess);

    auto entry = descriptor.ReserveAndAddEntry(
        (uint64_t *)addresses[1 - j], (uint64_t)values[1 - j],
        Descriptor::kRecycleNewOnFailure);

    uint64_t *ptr = descriptor.GetNewValuePtr(entry);
    allocator_->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode));
    AllocTestLinkedListNode *node =
        nv_ptr<AllocTestLinkedListNode>(descriptor.GetNewValue(entry));
    new (node) AllocTestLinkedListNode{values[j]->next, values[j]->key,
                                       values[j]->payload};

    EXPECT_TRUE(descriptor.MwCAS());

    pool_->GetEpoch()->Unprotect();
  }

  // Loop over the entire descriptor pool to ensure that
  // the previous descriptors are recycled
  for (uint32_t i = 0; i < kDescriptorPoolSize; ++i) {
    pool_->GetEpoch()->Protect();
    auto desc = pool_->AllocateDescriptor();
    desc.Abort();
    pool_->GetEpoch()->Unprotect();
  }

  std::set<void *> new_user_allocations;
  {
    PMEMobjpool *pop = allocator_->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations_, addr)) {
        insert(new_user_allocations, addr);
      }
    }
  }

  // user allocations should stay the same size
  ASSERT_EQ(new_user_allocations.size(), user_allocations_.size());

  for (uint32_t i = 0; i < kTestArraySize; ++i) {
    ASSERT_TRUE(array[0][i] == nullptr or array[1][i] == nullptr);
    for (uint32_t j = 0; j < 2; ++j) {
      if (array[j][i]) {
        EXPECT_EQ(array[j][i]->key, i);
      }
    }
  }

  // have to clear the EpochManager entry
  Thread::ClearRegistry(true);
}

void child_process_work(sem_t *sem, std::set<void *> &base_allocations) {
  static const size_t kOperationPerProtect = 100;
  static const size_t kInsertPercentage = 75;

  auto thread_count = Environment::Get()->GetCoreCount();
  auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
  auto root_obj = (RootObj *)allocator->GetRoot(sizeof(RootObj));

  pmemobj_zalloc(allocator->GetPool(), &root_obj->pool,
                 sizeof(pmwcas::DescriptorPool), TOID_TYPE_NUM(char));
  new (pmemobj_direct(root_obj->pool))
      pmwcas::DescriptorPool(kDescriptorPoolSize, thread_count);

  pmemobj_zalloc(allocator->GetPool(), &root_obj->array,
                 sizeof(nv_ptr<AllocTestLinkedListNode>) * kTestArraySize,
                 TOID_TYPE_NUM(char));

  auto pool = (pmwcas::DescriptorPool *)pmemobj_direct(root_obj->pool);
  auto array =
      (nv_ptr<AllocTestLinkedListNode> *)pmemobj_direct(root_obj->array);

  // prefill array with some nodes
  for (uint32_t i = 0; i < kTestArraySize; ++i) {
    auto ptr = reinterpret_cast<uint64_t *>(&array[i]);
    allocator->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode), false);
    new (array[i]) AllocTestLinkedListNode{nullptr, i, 0};
    ASSERT_EQ(array[i]->next, nullptr);
    ASSERT_EQ(array[i]->key, i);
    ASSERT_EQ(array[i]->payload, 0);
  }

  pmwcas::NVRAM::Flush(sizeof(RootObj), root_obj);
  pmwcas::NVRAM::Flush(sizeof(DescriptorPool), pool);
  pmwcas::NVRAM::Flush(sizeof(nv_ptr<AllocTestLinkedListNode>) * kTestArraySize,
                       array);
  LOG(INFO) << "data flushed" << std::endl;

  std::set<void *> user_allocations;
  {
    PMEMobjpool *pop = allocator->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations, addr)) {
        insert(user_allocations, addr);
      }
    }
  }

  // We have 3 more allocations than just the nodes:
  // * The DescriptorPool object
  // * The actual descriptor pool (array)
  // * The indirection array
  ASSERT_EQ(user_allocations.size(), kTestArraySize + 3);

  sem_post(sem);

  std::vector<std::thread> threads;

  Barrier barrier(kNumThreads);

  for (uint32_t t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&]() {
      nv_ptr<AllocTestLinkedListNode> *addresses[kWordsToUpdate];
      nv_ptr<AllocTestLinkedListNode> values[kWordsToUpdate];

      RandomNumberGenerator rng(rand(), 0, kTestArraySize);
      RandomNumberGenerator rng_op(rand(), 0, 100);
      barrier.CountAndWait();

      while (true) {
        pool->GetEpoch()->Protect();

        for (size_t cnt = 0; cnt < kOperationPerProtect; ++cnt) {
          for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
            addresses[i] = nullptr;
            values[i] = nullptr;
          }

          for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
          retry:
            uint64_t idx = rng.Generate();
            for (uint32_t j = 0; j < i; ++j) {
              if (addresses[j] == &array[idx]) {
                goto retry;
              }
            }

            addresses[i] = &array[idx];
            values[i] = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
                            &array[idx])
                            ->GetValueProtected();
          }

          auto descriptor = pool->AllocateDescriptor();
          EXPECT_NE(nullptr, descriptor.GetRaw());

          for (uint32_t i = 0; i < kWordsToUpdate; ++i) {
            if (rng_op.Generate() < kInsertPercentage) {
              // Insert a new node
              auto idx = descriptor.ReserveAndAddEntry(
                  (uint64_t *)addresses[i], (uint64_t)values[i],
                  Descriptor::kRecycleNewOnFailure);
              uint64_t *ptr = descriptor.GetNewValuePtr(idx);
              allocator->AllocateOffset(ptr, sizeof(AllocTestLinkedListNode));
              AllocTestLinkedListNode *node =
                  nv_ptr<AllocTestLinkedListNode>(descriptor.GetNewValue(idx));

              new (node) AllocTestLinkedListNode{values[i], values[i]->key,
                                                 values[i]->payload + 1};
            } else {
              // Remove an existing node
              if (!values[i]->next) {
                // Already the last node (sentinel); skip the deletion.
                continue;
              }
              descriptor.AddEntry((uint64_t *)addresses[i], (uint64_t)values[i],
                                  (uint64_t)values[i]->next,
                                  Descriptor::kRecycleOldOnSuccess);
            }
          }

          descriptor.MwCAS();
        }

        pool->GetEpoch()->Unprotect();
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }
  LOG(INFO) << "Child process finished, this should not happen" << std::endl;
}

void ArrayPreScan(uint64_t *array) {
  uint64_t dirty_cnt{0}, concas_cnt{0}, mwcas_cnt{0};
  for (uint32_t i = 0; i < kTestArraySize; ++i) {
    uint64_t val = array[i];
    if ((val & pmwcas::Descriptor::kDirtyFlag) != 0) {
      dirty_cnt += 1;
      val = val & ~pmwcas::Descriptor::kDirtyFlag;
    }
    if ((val & pmwcas::Descriptor::kCondCASFlag) != 0) {
      concas_cnt += 1;
      val = val & ~pmwcas::Descriptor::kCondCASFlag;
    }
    if ((val & pmwcas::Descriptor::kMwCASFlag) != 0) {
      mwcas_cnt += 1;
      val = val & ~pmwcas::Descriptor::kMwCASFlag;
    }
  }
  LOG(INFO) << "=======================\nDirty count: " << dirty_cnt
            << "\tCondition CAS count: " << concas_cnt
            << "\tMwCAS count: " << mwcas_cnt << std::endl;
}

std::set<void *> ArrayScan(nv_ptr<AllocTestLinkedListNode> *array) {
  static const size_t kMaxLengthOutput = 10;

  std::set<void *> allocations;

  uint64_t length_cnt[kMaxLengthOutput + 1] = {};

  auto getLength = [](nv_ptr<AllocTestLinkedListNode> head) {
    return head ? std::min(head->payload + 1, kMaxLengthOutput) : 0;
  };

  for (uint32_t i = 0; i < kTestArraySize; ++i) {
    nv_ptr<AllocTestLinkedListNode> head = array[i];

    length_cnt[getLength(head)]++;

    for (auto node = head; node; node = node->next) {
      insert(allocations, (AllocTestLinkedListNode *)node);
      if (node->next) {
        EXPECT_EQ(node->key, node->key);
        EXPECT_EQ(node->payload, node->next->payload + 1);
      } else {
        break;
      }
    }
  }

  EXPECT_EQ(length_cnt[0], 0);
  LOG(INFO) << "=======================";
  LOG(INFO) << "\tlength\tcount";
  for (size_t i = 1; i <= kMaxLengthOutput; i++) {
    LOG(INFO) << "\t" << i << "\t" << length_cnt[i];
  }

  return allocations;
}

GTEST_TEST(PMwCASMemorySafetyRecoveryTest, MultiThreadRecovery) {
  auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());

  std::set<void *> base_allocations;
  {
    PMEMobjpool *pop = allocator->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      insert(base_allocations, addr);
    }
  }

  sem_t *sem = sem_open(kSemaphoreName, O_CREAT, 0666, 0);
  if (sem == SEM_FAILED) {
    LOG(FATAL) << "Semaphore creation failure, errno: " << errno;
  }
  sem_unlink(kSemaphoreName);
  pid_t pid = fork();
  if (pid == 0) {
    child_process_work(sem, base_allocations);
    return;
  } else if (pid > 0) {
    sem_wait(sem);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    kill(pid, SIGKILL);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  } else {
    LOG(FATAL) << "fork failed" << std::endl;
  }
  LOG(INFO) << "Recovery starts here.";

  auto root_obj = (RootObj *)allocator->GetRoot(sizeof(RootObj));
  auto pool = (pmwcas::DescriptorPool *)pmemobj_direct(root_obj->pool);
  auto array =
      (nv_ptr<AllocTestLinkedListNode> *)pmemobj_direct(root_obj->array);

  ArrayPreScan((uint64_t *)array);

  pool->Recovery(0, false);

  // We have 3 more allocations than just the nodes:
  // * The DescriptorPool object
  // * The actual descriptor pool (array)
  // * The indirection array
  std::set<void *> nodes = ArrayScan(array);
  insert(nodes, pool);
  insert(nodes, pool->GetDescriptor());
  insert(nodes, array);

  std::set<void *> user_allocations;
  {
    PMEMobjpool *pop = allocator->GetPool();
    TOID(char) iter;
    POBJ_FOREACH_TYPE(pop, iter) {
      void *addr = pmemobj_direct(iter.oid);
      if (!contains(base_allocations, addr)) {
        insert(user_allocations, addr);
      }
    }
  }

  ASSERT_EQ(nodes.size(), user_allocations.size());
  for (auto i = nodes.begin(), j = user_allocations.begin();
       i != nodes.end() && j != user_allocations.end(); ++i, ++j) {
    EXPECT_EQ(*i, *j);
  }
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
