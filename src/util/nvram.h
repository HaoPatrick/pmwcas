// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <unistd.h>

#ifdef PMDK
#include <libpmemobj.h>
#endif

#include "include/environment.h"

namespace pmwcas {

struct NVRAM {
#ifdef PMEM
  static inline void Flush(uint64_t bytes, const void* data) {
#ifdef EADR
    // eADR platforms have their CPU caches in the persistence domain.
    // We only need to maintain correct ordering for stores.
    _mm_sfence();
#else
#ifdef PMDK
    auto pmdk_allocator = reinterpret_cast<PMDKAllocator*>(Allocator::Get());
    pmdk_allocator->PersistPtr(data, bytes);
#else
    if(use_clflush) {
      RAW_CHECK(data, "null data");
      uint64_t ncachelines = (bytes + kCacheLineSize - 1) / kCacheLineSize;
      // Flush each cacheline in the given [data, data + bytes) range
      for(uint64_t i = 0; i < ncachelines; ++i) {
        _mm_clflush(&((char*)data)[i * ncachelines]);
      }
    } else {
    }
#endif  // PMDK
#endif
  }
#endif  // PMEM
};

}  // namespace pmwcas
