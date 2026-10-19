.PHONY: all test clean

all: test

test:
	morloc make -o test-stream test.loc
	./test-stream test | tee test.out
	tail -n 1 test.out | grep -qx true

clean:
	rm -rf test-stream test-stream-build test.out
