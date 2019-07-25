#include <boost/circular_buffer.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <algorithm>
#include <time.h>
#include <omp.h>
#include <sys/time.h>

int reverse_bytes(int i) {
	int j;
	unsigned char *dst = (unsigned char *)&j;
	unsigned char *src = (unsigned char *)&i;

	dst[0] = src[3];
	dst[1] = src[2];
	dst[2] = src[1];
	dst[3] = src[0];

	return j;
}

void print(long id, std::vector<int> const &input) {
	printf("\r%4ld: ", id);
	for (unsigned long i = 0; i < input.size(); i++) {
		printf("%8d%s", input[i], (i+1 < input.size()) ? " -> " : "\n");
	}
}

bool verify(std::vector<int> &buffer, int level, 
	const std::vector<std::vector<int>> &links) {
	
	for (int i = 0; i < level - 1; i++) {
		bool found = false;
		int target = buffer[i+1];
		for (int j : links[buffer[i]]) {
			if (j == target) {
				found = true;	
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

void find_cached_paths(int start, int goal, std::vector<int> &buffer, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links, long &count,
	std::vector<std::set<int>> &cache) {

	buffer[level] = start;

}

void search_data_recursive(int start, int goal, std::vector<int> &buffer, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links, long &count,
	std::vector<std::set<int>> &cache) {
	buffer[level] = start;
	search_mask[start] = std::min(search_mask[start], (unsigned char)level);
	int tmp_count = 0;
	for (int i : links[start]) {
		if (level + 1 == max_level && i == goal) {
			#pragma omp critical
			{
				buffer[max_level] = goal;
				// count++;
				// print(count, buffer);
				for (int j = 0; j < (int) buffer.size() - 1; j++) {
					cache[buffer[j]].insert(buffer[j+1]);
				}
			}
		} else if (level + 1 < max_level && level + 1 < search_mask[i]) {
			search_data_recursive(i, goal, buffer, level+1, max_level,
				search_mask, links, count, cache);
		} else if (level + 1 == search_mask[i] && cache[i].size() > 0) {
			#pragma omp critical
			{
				for (int j = 0; j < level; j++) {
					cache[buffer[j]].insert(buffer[j+1]);
				}
				cache[start].insert(i);
			}
		} else {
			tmp_count++;
		}
	}
	#pragma omp atomic
		count += tmp_count;
}

void start_parallel_tasks(int start, int current, int goal, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links, long &count,
	std::vector<std::set<int>> &cache) {

	for (unsigned long int k = 0; k < links[current].size(); k++) {
		int i = links[current][k];
		#pragma omp task default(none) \
			firstprivate(k, i, start, current, goal, level, max_level) \
			shared(search_mask, links, count, cache, stdout)
		{
			if (level == 0) {
				printf("\r%lu / %lu\t", k, links[current].size());
				fflush(stdout);
			}
			std::vector<int> buffer(max_level + 1);
			buffer[0] = start;
			buffer[level] = current;

			if (level + 1 < max_level && level + 1 < search_mask[i]) {
				search_data_recursive(i, goal, buffer, 
					level+1, max_level, search_mask, links, count, cache);
			} else if (level + 1 == search_mask[i] && cache[i].size() > 0) {
				#pragma omp critical
				{
					for (int j = 0; j < level; j++) {
						cache[buffer[j]].insert(buffer[j+1]);
					}
					cache[current].insert(i);
				}
			}
		}
	}
}

int search_data_recursive_parallel(int start, int current, int goal, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links, long &count,
	std::vector<std::set<int>> &cache) {

	search_mask[current] = std::min(search_mask[current], (unsigned char)level);
	int c = 0;
	if (level == 0 && links[current].size() < 50 && max_level > 2) {
		for (int i : links[current]) {
			if (level == 0) {
				printf("\r%d / %lu\t", ++c, links[current].size());
				fflush(stdout);
			}
			search_data_recursive_parallel(start, i, goal, level+1, 
				max_level, search_mask, links, count, cache);
		}
	} else {
		start_parallel_tasks(start, current, goal, level, 
			max_level, search_mask, links, count, cache);
	}
	return count;
}

long find_all_paths(int start, int goal, int length, 
	std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links,
	std::vector<std::set<int>> &cache) {

	long paths = 0;
	if (length < 3) {
		std::vector<int> buffer(length+1);
		search_data_recursive(start, goal, buffer, 
			0, length, search_mask, links, paths, cache);
	} else {
		#pragma omp parallel
		{
			#pragma omp single nowait
			{
				search_data_recursive_parallel(start, start, goal, 
					0, length, search_mask, links, paths, cache);
			}
		}
	}
	return paths;
}



int search_data(int start, int goal, 
	boost::circular_buffer<int> &queue, 
	const std::vector<std::vector<int>> &links,
	std::vector<bool> &searched) {
	
	std::fill(searched.begin(), searched.end(), false);
	queue.clear();

	queue.push_back(start);
	bool found_goal = false;
	int curr_depth = 0;
	int counted = 0;
	std::vector<int> link_counter(10);
	link_counter[curr_depth]++;

	while (true) {
		if (queue.empty()) {
			printf("queue empty\n");
			return -1;
		}

		int art = queue.front();
		queue.pop_front();
		link_counter[curr_depth]--;

		for (int i : links[art]) {
			if (!searched[i]) {
				if (!found_goal) {
					counted++;
					queue.push_back(i);
					link_counter[curr_depth+1]++;
				}
				searched[i] = true;
			}

			if (i == goal) {
				found_goal = true;
				break;
			}
		}
		if (link_counter[curr_depth] == 0) {
			curr_depth++;
		}
		if (found_goal) {
			break;
		}
	}

	// printf("length of path: %d (%d)\n", curr_depth + 1, counted);
	return curr_depth + 1;
}

struct article_count {
	int articles;
	int max_article_id;
};

article_count read_file(const char *filename, 
	std::vector<std::vector<int>> &links) {
	
	int size = 10000;
	int num_articles, article_id, num_links, max_article_id;
	std::vector<int> ret(size);
	std::ifstream in(filename, std::ios::in | std::ios::binary);
	in.read((char*)&num_articles, sizeof(int));
	num_articles = reverse_bytes(num_articles);
	in.read((char*)&max_article_id, sizeof(int));
	max_article_id = reverse_bytes(max_article_id);
	links.resize(max_article_id + 1);
	for (int a = 0; a < num_articles; a++) {
		in.read((char*)&article_id, sizeof(int));
		in.read((char*)&num_links, sizeof(int));
		article_id = reverse_bytes(article_id);
		num_links = reverse_bytes(num_links);
		std::vector<int> temp;
		temp.reserve(num_links);
		for (int i = 0; i < num_links; i += size) {
			int temp_num = std::min(size, num_links - i);
			in.read((char*)&ret[0], temp_num * sizeof(int));
			for (int j = 0; j < temp_num; j++) {
				temp.push_back(reverse_bytes(ret[j]));
			}
		}
		links[article_id] = (const std::vector<int>)temp;
	}
	in.close();
	article_count ret_count;
	ret_count.articles = num_articles;
	ret_count.max_article_id = max_article_id;
	return ret_count;
}

double get_wall_time(){
	struct timeval time;
	if (gettimeofday(&time,NULL)){
		return 0;
	}
	return (double)time.tv_sec + (double)time.tv_usec * .000001;
}

void rebuild_paths(std::vector<std::set<int>> &cache, int start, int goal, 
	int level, std::vector<int> &buffer, int &total) {

	buffer[level] = start;
	if (start == goal) {
		total++;
		// print(total, buffer);
	}
	for (int i : cache[start]) {
		rebuild_paths(cache, i, goal, level + 1, buffer, total);
	}
}

int main(int argc, char **argv) {
	const char *filename = "../memory_map_formatted.dat";
	boost::circular_buffer<int> queue(4000000);
	std::vector<std::vector<int>> links;
	std::vector<std::set<int>> cache;
	std::vector<bool> searched;
	std::vector<unsigned char> search_mask;

	clock_t start, end;
	double ts, te;
	double elapsed_clock, elapsed_wall;

	ts = get_wall_time();
	article_count counts = read_file(filename, links);
	searched.resize(counts.max_article_id + 1);
	search_mask.resize(counts.max_article_id + 1);
	cache.resize(counts.max_article_id + 1);
	te = get_wall_time();
	elapsed_wall = te - ts;
	printf("\rfile read: %.3f seconds (%d / %d)\n", 
		elapsed_wall, counts.articles, counts.max_article_id);

	std::fill(search_mask.begin(), search_mask.end(), 0xff);
	int l;
	// jesus -> europe (2)
	// int s = 1095706;
	// int e = 9239;
	// europe -> sean banan (3)
	// int s = 9239;
	// int e = 27703020;
	// sean banan -> sean whyte (5)
	// int s = 27703020;
	// int e = 22149654;
	// osteoporosis -> danger zone (5)
	int s = 22461;
	int e = 2146249;
	if (argc == 3) {
		s = atoi(argv[1]);
		e = atoi(argv[2]);
	}

	ts = get_wall_time();
	l = search_data(s, e, queue, links, searched);
	te = get_wall_time();
	elapsed_wall = te - ts;
	printf("\rpath of length %d found in %.3f seconds\n", 
		l, elapsed_wall);

	start = clock();
	ts = get_wall_time();

	omp_set_num_threads(8);
	long paths = find_all_paths(s, e, l, search_mask, links, cache);
/*
	for (int i = 0; i < (int)cache.size(); i++) {
		std::set<int> ss = cache[i];
		if (ss.size() > 0) {
			// print(i, v);
			// std::set<int> ss(v.begin(), v.end());
			printf("%d: ", i);
			for (int j : ss) {
				printf("%d ", j);
			}
			printf("\n");
		}
	}
*/
	std::vector<int> buffer(l+1);
	int total = 0;
	rebuild_paths(cache, s, e, 0, buffer, total);
	printf("total number of paths: %d\n", total);

	end = clock();
	te = get_wall_time();
	elapsed_clock = ((double) (end - start)) / CLOCKS_PER_SEC;
	elapsed_wall = te - ts;
	printf("\relapsed (clock): %.3f seconds (%ld)\n", elapsed_clock, paths);
	printf("\relapsed  (wall): %.3f seconds (%ld)\n", elapsed_wall, paths);
	return 0;
}