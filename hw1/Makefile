CC=gcc
CCFLAGS=-Wall -g
all_test_files = $(wildcard test_data/*.txt)
sort: sort.c
	$(CC) $(CCFLAGS) $< -o $@
clean:
	rm -f sort output.txt
clean_output:
	rm -f output.txt
run: sort clean_output
	./sort $(all_test_files)
check:
	python checker.py -f output.txt
.PHONY = clean clean_output run

