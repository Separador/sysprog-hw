CC=gcc
CCFLAGS=-Wall -g
all_test_files = $(wildcard test_data/*.txt)
timeslice = 100 #msecs
sort: sort.c
	$(CC) $(CCFLAGS) $< -o $@
clean:
	rm -f sort output.txt
clean_output:
	rm -f output.txt
run: sort clean_output
	./sort $(timeslice) $(all_test_files)
err: sort clean_output
	./sort 100  test2.txt test1.txt
check:
	python checker.py -f output.txt
.PHONY = clean clean_output run

