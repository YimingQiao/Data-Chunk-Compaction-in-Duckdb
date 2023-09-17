add_library_unity(
        duckdb_optimizer_bushy_join_order
        OBJECT
        bushy_order_optimizer.cpp)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer_bushy_join_order>
        PARENT_SCOPE)