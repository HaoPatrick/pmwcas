set(MWCAS_HEADERS
  metrics.h
  mwcas.h
)

set(MWCAS_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/mwcas.cc
)

set_property(GLOBAL APPEND PROPERTY PMWCAS_SRC ${MWCAS_SOURCES})

ADD_PMWCAS_TEST(mwcas_tests)

if(${PMEM_BACKEND} STREQUAL "PMDK")
  ADD_PMWCAS_TEST(recovery_tests)
endif()

ADD_PMWCAS_TEST(mem_safety_tests)
