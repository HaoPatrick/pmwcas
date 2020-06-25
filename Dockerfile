FROM haoxiangpeng/latest-cpp:latest

ARG PMEM_BACKEND=PMDK
COPY . /usr/src/pmwcas
WORKDIR /usr/src/pmwcas
ENV PMEM_IS_PMEM_FORCE 1
RUN mkdir build_tmp \
    &&  cd build_tmp \
    &&  cmake -DCMAKE_CXX_COMPILER=/usr/bin/clang++ --DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_BUILD_TYPE=Release -DPMEM_BACKEND=${PMEM_BACKEND} -DGOOGLE_FRAMEWORK=1 .. \
    &&  make -j4 

ENTRYPOINT make -C build_tmp test ARGS="-E \"(logging)|(Recovery)\" -T Test"
