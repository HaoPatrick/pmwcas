#pragma once

#include <include/pmwcas.h>

namespace pmwcas {

#ifdef PMEM
template <typename T>
struct nv_ptr {
  nv_ptr() = default;
  nv_ptr(std::nullptr_t) {}
  nv_ptr(uint64_t offset) : offset_(offset) {}
  nv_ptr(T *ptr) : offset_(fromPointer(ptr)) {}
  T &operator*() { return *toPointer(offset_); }
  T *operator->() { return toPointer(offset_); }
  friend bool operator==(nv_ptr<T> l, nv_ptr<T> r) {
    return l.offset_ == r.offset_;
  }
  friend bool operator!=(nv_ptr<T> l, nv_ptr<T> r) { return !(l == r); }
  friend bool operator==(nv_ptr<T> l, std::nullptr_t) { return l.offset_ == 0; }
  friend bool operator!=(nv_ptr<T> l, std::nullptr_t) { return l.offset_ != 0; }
  friend bool operator<(nv_ptr<T> l, nv_ptr<T> r) {
    return l.offset_ < r.offset_;
  }

  operator uint64_t() const { return offset_; }
  operator T *() const { return toPointer(offset_); }

 private:
  static uint64_t fromPointer(T *ptr) {
    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    return allocator->GetOffset<T>(ptr);
  }

  static T *toPointer(uint64_t offset) {
    auto allocator = reinterpret_cast<PMDKAllocator *>(Allocator::Get());
    return allocator->GetDirect<T>(offset);
  }

  template <typename U>
  friend nv_ptr<U> CompareExchange64(nv_ptr<U> *destination,
                                     nv_ptr<U> new_value, nv_ptr<U> comparand);

  uint64_t offset_{0};
};

template <typename T>
nv_ptr<T> CompareExchange64(nv_ptr<T> *destination, nv_ptr<T> new_value,
                            nv_ptr<T> comparand) {
  static_assert(sizeof(nv_ptr<T>) == 8,
                "CompareExchange64 only works on 64 bit values");
  ::__atomic_compare_exchange_n(&destination->offset_, &comparand.offset_,
                                new_value.offset_, false, __ATOMIC_SEQ_CST,
                                __ATOMIC_SEQ_CST);
  return comparand;
}

#else
template <typename T>
using nv_ptr = T *;

#endif
}  // namespace pmwcas