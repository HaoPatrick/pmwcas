// Licensed under the MIT license.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include <chrono>
#include <random>
#include <thread>

#include "common/allocator_internal.h"
#include "environment/environment_linux.h"
#include "include/allocator.h"
#include "include/environment.h"
#include "include/pmwcas.h"
#include "include/status.h"
#include "mwcas/mwcas.h"
#include "util/auto_ptr.h"
#include "util/random_number_generator.h"

static const uint32_t POOL_SIZE = 16;
static const uint32_t WORKLOAD_THREAD_CNT = 1;
static const char* SEMAPHORE_NAME = "/mwcas-callback-test-sem";
static const char* POOL_NAME = "mwcas_callback_test_pool";

bool free_callback1 = false;
bool free_callback2 = false;

void FreeCallback1(uint64_t* mem) {
  LOG(INFO) << "Free Callback 1";
  free_callback1 = true;
}

void FreeCallback2(uint64_t* mem) {
  LOG(INFO) << "Free Callback 2";
  free_callback2 = true;
}

struct RootObj {
  // pmwcas::DescriptorPool* pool_addr;
  PMEMoid pool_addr;
  uint64_t array[4];
};

namespace pmwcas {
void child_process_work(sem_t* sem) {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          "mwcas_callback_test_pool", "mwcas_linked_layout",
                          static_cast<uint64_t>(1024) * 1024 * 1204 * 1),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto* allocator_ = (pmwcas::PMDKAllocator*)pmwcas::Allocator::Get();

  auto* root_obj = (RootObj*)allocator_->GetRoot(sizeof(RootObj));

  pmemobj_zalloc(allocator_->GetPool(), &root_obj->pool_addr,
                 sizeof(pmwcas::DescriptorPool), TOID_TYPE_NUM(char));
  new (pmemobj_direct(root_obj->pool_addr))
      pmwcas::DescriptorPool(POOL_SIZE, WORKLOAD_THREAD_CNT, false);

  auto descriptor_pool =
      (pmwcas::DescriptorPool*)pmemobj_direct(root_obj->pool_addr);
  auto fc1 = descriptor_pool->RegisterFreeCallback(&FreeCallback1);
  auto fc2 = descriptor_pool->RegisterFreeCallback(&FreeCallback2);

  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(root_obj->array[i], 0);
  }

  pmwcas::NVRAM::Flush(sizeof(RootObj), root_obj);
  pmwcas::NVRAM::Flush(sizeof(DescriptorPool), descriptor_pool);

  LOG(INFO) << "data flushed" << std::endl;

  sem_post(sem);

  pmwcas::EpochGuard guard(descriptor_pool->GetEpoch());

  auto desc1 = descriptor_pool->AllocateDescriptor(fc1);
  desc1.ReserveAndAddEntry(&root_obj->array[0], 0,
                           pmwcas::Descriptor::kRecycleNewOnFailure);

  auto desc2 = descriptor_pool->AllocateDescriptor(fc2);
  desc2.ReserveAndAddEntry(&root_obj->array[1], 0,
                           pmwcas::Descriptor::kRecycleAlways);

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  LOG(FATAL) << "Child process finished, this should not happen" << std::endl;
}

GTEST_TEST(PMwCASTest, RecoveryCallback) {
  sem_t* sem = sem_open(SEMAPHORE_NAME, O_CREAT, 0600, 0);
  if (sem == SEM_FAILED) {
    LOG(FATAL) << "Semaphore creation failure, errno: " << errno;
  }
  sem_unlink(SEMAPHORE_NAME);
  pid_t pid = fork();
  if (pid > 0) {
    /// Step 2: wait for some time; make sure child process has already
    /// persisted the root object and started actual work
    sem_wait(sem);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    /// Step 3: force kill all running threads without noticing them
    kill(pid, SIGKILL);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  } else if (pid == 0) {
    child_process_work(sem);
    return;
  } else {
    LOG(FATAL) << "fork failed" << std::endl;
  }
  LOG(INFO) << "Recovery starts here.";
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          "mwcas_callback_test_pool", "mwcas_linked_layout",
                          static_cast<uint64_t>(1024) * 1024 * 1204 * 1),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto* allocator_ = (PMDKAllocator*)Allocator::Get();
  auto* root_obj = (RootObj*)allocator_->GetRoot(sizeof(RootObj));
  auto descriptor_pool =
      (pmwcas::DescriptorPool*)pmemobj_direct(root_obj->pool_addr);

  descriptor_pool->ClearFreeCallbackArray();
  descriptor_pool->RegisterFreeCallback(&FreeCallback1);
  descriptor_pool->RegisterFreeCallback(&FreeCallback2);

  /// Step 4: perform the recovery
  auto descs = descriptor_pool->GetDescriptor();
  descriptor_pool->Recovery(0, false, false);

  ASSERT_TRUE(free_callback1);
  ASSERT_TRUE(free_callback2);

  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(root_obj->array[i], 0);
  }

  sem_close(sem);
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
