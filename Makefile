all:test

.PHONY:c/target/debug/libcpijul.a
c/target/debug/libcpijul.a:
	cd c;cargo build

test:c/target/debug/libcpijul.a test_c_api.c c/pijul.h
	gcc -o test test_c_api.c -Ic c/target/debug/libcpijul.a -llmdb -lc -lm -ldl -lpthread -lgcc_s -lrt && ./test
