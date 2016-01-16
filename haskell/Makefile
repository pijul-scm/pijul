.PHONY:pijul

LD_LIBRARY_PATH=../c/target/debug:$LD_LIBRARY_PATH

pijul:libpijul.hs
	ghc -L../c/target/debug -llmdb -lc -lm -ldl -lpthread -lgcc_s -lrt -lm -lcpijul -o $@ $<

libpijul.hs:libpijul.hsc
	hsc2hs $<

clean:
	rm -f *.o *.hi pijul libpijul.hs
