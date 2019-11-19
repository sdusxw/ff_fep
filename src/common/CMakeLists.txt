
add_library(common
	common.h
	common.cpp
	concurrent_queue.h
	concurrent_queue.cpp
	push_json.h
	push_json.cpp
)

target_link_libraries(common pthread boost_system boost_thread curl)

