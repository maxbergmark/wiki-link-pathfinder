#include <boost/circular_buffer.hpp>
#include "wiki_searcher.h"
#include <iostream>
#include <vector>
#include <algorithm>
#include <time.h>
#include <sys/time.h>

double get_wall_time() {
	struct timeval time;
	if (gettimeofday(&time,NULL)){
		return 0;
	}
	return (double)time.tv_sec + (double)time.tv_usec * 1e-6;
}

int main(int argc, char **argv) {

	WikiSearcher* wiki_searcher = new WikiSearcher();
	wiki_searcher->read_files();

	// searches.push_back({1095706, 9239}); // jesus -> europe (2)
	// searches.push_back({9239, 27703020}); // europe -> sean banan (3)
	// searches.push_back({27703020, 22149654}); // sean banan -> sean whyte (5)	
	// searches.push_back({22461, 2146249}); // osteoporosis -> danger zone (5)

	// if (argc == 3) {
		// searches.push_back({atoi(argv[1]), atoi(argv[2])});
	// }


	double t0, t1, elapsed;
	double min_t = 1e3;
	double max_t = 0;
	double avg_t = 0;
	long checksum = 0;
	int tot = 10000;
	// for (search_pair p : searches) {
	for (int i = 0; i < tot;) {
		search_pair p = {rand() % 61197143, rand() % 61197143};
		t0 = get_wall_time();
		std::string s = wiki_searcher->perform_search(p.start, p.end);
		t1 = get_wall_time();
		if (wiki_searcher->last_path_length != -1) {
			printf("\r%7d / %7d", i++, tot);
			fflush(stdout);
		}
		checksum += s.size() + s.size() * s.size();
		elapsed = t1 - t0;
		// std::cout << s << std::endl;
		// float t = wiki_searcher->perform_search(p.start, p.end);
		max_t = std::max(max_t, elapsed);
		min_t = std::min(min_t, elapsed);
		avg_t += elapsed / tot;
	}

	printf("\raverage: %.3fms\nmaximum: %.3fms\nminimum: %.3fms\n", 
		avg_t * 1e3, max_t * 1e3, min_t * 1e3);
	printf("\rchecksum: %ld\n", checksum);

	delete wiki_searcher;
	return 1;
}