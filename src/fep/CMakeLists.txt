include_directories(
	${SOLUTION_INCLUDE_DIRS}
	/usr/local/include/cppkafka/
)
link_directories(
    /usr/local/lib
    /usr/local/lib64
)

add_executable(ff_fep
    ff_fep.cpp
)

target_link_libraries(ff_fep pthread cppkafka common curl jsoncpp)
