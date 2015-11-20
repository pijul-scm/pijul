all:Pijul.class Main.class libscalapijul.so
	scala -Djava.library.path=".:/usr/lib/x86_64-linux-gnu/:../../c/target/debug" Main

Main.class:main.scala
	scalac main.scala

Pijul.class:pijul.scala
	scalac pijul.scala

libscalapijul.so:scalapijul.c
	gcc -shared -fPIC -o $@ $< -I/usr/lib/jvm/java-7-openjdk-amd64/include -I../../c ../../c/target/debug/libcpijul.a -llmdb -lc -lm -ldl -lpthread -lgcc_s -lrt
