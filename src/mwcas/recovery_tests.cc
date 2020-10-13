// Copyright (c) Xiangpeng Hao (haoxiangpeng@hotmail.com). All rights reserved.
// Licensed under the MIT license.

#include <gtest/gtest.h>
#include <stdlib.h>

#include <chrono>
#include <random>
#include <thread>

#include "common/allocator_internal.h"
#include "include/allocator.h"
#include "include/environment.h"
#include "include/pmwcas.h"
#include "include/status.h"
#include "mwcas/mwcas.h"
#include "util/auto_ptr.h"
#include "util/random_number_generator.h"
#include "environment/environment_linux.h"

static const uint64_t ARRAY_SIZE = 1024;
static const uint8_t ARRAY_INIT_VALUE = 0;
static const uint32_t UPDATE_ROUND = 1024 * 1024;
static const uint32_t WORKLOAD_THREAD_CNT = 4;

void ArrayPreScan(uint64_t* array) {
  uint64_t dirty_cnt{0}, concas_cnt{0}, mwcas_cnt{0};
  for (uint32_t i = 0; i < ARRAY_SIZE; i += 1) {
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

void ArraySanityCheck(uint64_t* array) {
  /// This invariant should be unconditionally kept: the sum of the array should
  /// be the multiple of DESC_CAP
  uint64_t sum{0};
  for (uint32_t i = 0; i < ARRAY_SIZE; i += 1) {
    sum += array[i];
  }
  ASSERT_EQ(sum % DESC_CAP, 0);
  LOG(INFO) << "Recovery sanity check passed!" << std::endl;
}

struct RootObj {
  // pmwcas::DescriptorPool* pool_addr;
  PMEMoid pool_addr;
  // uint64_t* array;
  PMEMoid array;
};

void thread_workload(pmwcas::DescriptorPool* descriptor_pool, uint64_t* array,
                     uint64_t time_in_milliseconds) {
  std::random_device rd;
  std::mt19937 eng(rd());
  std::uniform_int_distribution<> distr(0, ARRAY_SIZE - 1);

  auto begin = std::chrono::steady_clock::now();
  uint64_t elapsed = 0;
  while (elapsed < time_in_milliseconds) {
    pmwcas::EpochGuard guard(descriptor_pool->GetEpoch());
    auto desc = descriptor_pool->AllocateDescriptor();

    /// generate unique positions
    std::set<uint32_t> positions;
    while (positions.size() < DESC_CAP) {
      positions.insert(distr(eng));
    }

    /// randomly select array items to perform MwCAS
    for (const auto& it : positions) {
      auto item = array + it;
#if 0
      auto old_val = *item;
      while (!pmwcas::Descriptor::IsCleanPtr(old_val)) {
        old_val = __atomic_load_n(item, __ATOMIC_SEQ_CST);
      }
#else
      auto old_val =
          ((pmwcas::MwcTargetField<uint64_t>*)item)->GetValueProtected();
#endif
      desc.AddEntry(item, old_val, old_val + 1);
    }

    desc.MwCAS();

    auto end = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count();
  }

  LOG(INFO) << "finished all jobs" << std::endl;
}

namespace pmwcas {
void child_process_work() {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          "mwcas_recovery_test_pool", "mwcas_linked_layout",
                          static_cast<uint64_t>(1024) * 1024 * 1204 * 1),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto* allocator_ = (pmwcas::PMDKAllocator*)pmwcas::Allocator::Get();

  auto* root_obj = (RootObj*)allocator_->GetRoot(sizeof(RootObj));

  pmemobj_zalloc(allocator_->GetPool(), &root_obj->pool_addr,
    sizeof(pmwcas::DescriptorPool), TOID_TYPE_NUM(char));
  new (pmemobj_direct(root_obj->pool_addr))
      pmwcas::DescriptorPool(10000, WORKLOAD_THREAD_CNT + 1, false);

  pmemobj_zalloc(allocator_->GetPool(), &root_obj->array,
    sizeof(uint64_t) * ARRAY_SIZE, TOID_TYPE_NUM(char));

  auto descriptor_pool =
    (pmwcas::DescriptorPool*)pmemobj_direct(root_obj->pool_addr);
  uint64_t* array =
    (uint64_t*)pmemobj_direct(root_obj->array);
  memset(array, ARRAY_INIT_VALUE, sizeof(uint64_t) * ARRAY_SIZE);
  pmwcas::NVRAM::Flush(sizeof(RootObj), root_obj);
  pmwcas::NVRAM::Flush(sizeof(DescriptorPool), descriptor_pool);
  pmwcas::NVRAM::Flush(sizeof(uint64_t) * ARRAY_SIZE, array);
  LOG(INFO) << "data flushed" << std::endl;

  /// Step 1: start the workload on multiple threads;
  std::thread workers[WORKLOAD_THREAD_CNT];
  for (uint32_t t = 0; t < WORKLOAD_THREAD_CNT; t += 1) {
    workers[t] = std::thread(thread_workload, descriptor_pool, array, 300);
  }
  for (uint32_t t = 0; t < WORKLOAD_THREAD_CNT; t += 1) {
    workers[t].join();
  }
  LOG(INFO) << "Child process finished, this should not happen" << std::endl;
}

GTEST_TEST(PMwCASTest, RecoverySingleThreaded) {
  pid_t pid = fork();
  if (pid > 0) {
    /// Step 2: wait for some time;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    /// Step 3: force kill all running threads without noticing them
    kill(pid, SIGKILL);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  } else if (pid == 0) {
    child_process_work();
    return;
  } else {
    LOG(FATAL) << "fork failed" << std::endl;
  }
  LOG(INFO) << "Recovery starts here.";
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          "mwcas_recovery_test_pool", "mwcas_linked_layout",
                          static_cast<uint64_t>(1024) * 1024 * 1204 * 1),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto* allocator_ = (PMDKAllocator*)Allocator::Get();
  auto* root_obj = (RootObj*)allocator_->GetRoot(sizeof(RootObj));
  auto descriptor_pool =
    (pmwcas::DescriptorPool*)pmemobj_direct(root_obj->pool_addr);
  uint64_t* array =
    (uint64_t*)pmemobj_direct(root_obj->array);

  ArrayPreScan(array);

  /// Step 4: perform the recovery
  descriptor_pool->Recovery(false);

  ArraySanityCheck(array);

  /// Step 5: check every item in the array
  std::map<uint64_t, uint32_t> histogram;
  for (uint32_t i = 0; i < ARRAY_SIZE; i += 1) {
    auto value = array[i];

    if (!pmwcas::Descriptor::IsCleanPtr(value)) {
      LOG(INFO) << "Invalid value 0x" << std::hex << value
                << " at " << &array[i] << std::endl;
    }
    // ASSERT_TRUE(pmwcas::Descriptor::IsCleanPtr(value));

    if (histogram.find(value) == histogram.end()) {
      histogram[value] = 1;
    } else {
      histogram[value] += 1;
    }
  }
  LOG(INFO) << "=============================\n";
  LOG(INFO) << "Array histogram\n";
  LOG(INFO) << "value\tcount\n";
  uint32_t total_cnt = 0;
  for (const auto& item : histogram) {
    auto format =
        (pmwcas::Descriptor::IsCleanPtr(item.first)) ? std::dec : std::hex;
    LOG(INFO) << format << item.first << std::dec << "\t" << item.second
              << std::endl;
    total_cnt += item.second;
  }
  LOG(INFO) << "Total count: " << total_cnt << std::endl;

  /// Step 6: perform random work over the pool again, there should not be any
  /// error.
  std::thread workers[WORKLOAD_THREAD_CNT];
  for (uint32_t t = 0; t < WORKLOAD_THREAD_CNT; t += 1) {
    workers[t] = std::thread(thread_workload, descriptor_pool, array, 100);
  }
  for (uint32_t t = 0; t < WORKLOAD_THREAD_CNT; t += 1) {
    workers[t].join();
  }

  ArraySanityCheck(array);
}
}  // namespace pmwcas

int main(int argc, char** argv) {
  // ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);

#ifndef PMDK
  static_assert(false, "PMDK is currently required for recovery");
#endif

  return RUN_ALL_TESTS();
}
