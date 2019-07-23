#include <boost/circular_buffer.hpp>
#include <iostream>
#include <fstream>
// #include <ios>
#include <vector>
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

void print(std::vector<int> const &input) {
	for (unsigned long i = 0; i < input.size(); i++) {
		std::cout << input.at(i) << ' ';
	}
	std::cout << std::endl;
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
			// printf("couldn't find %d in %d's links\n", target, buffer[i]);
			return false;
		}
	}
	return true;
}

int search_data_recursive(int start, int goal, std::vector<int> &buffer, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links) {
	buffer[level] = start;
	search_mask[start] = std::min(search_mask[start], (unsigned char)level);
	int count = 0;
	for (int i : links[start]) {
		if (level + 1 == max_level && i == goal) {
			#pragma omp critical
			{
				buffer[max_level] = goal;
				count++;
				printf("\r");
				for (int j = 0; j < max_level; j++) {
					printf("%d, ", buffer[j]);
				}
				printf("%d (%d)\n", goal, verify(buffer, max_level, links));
			}
		// } else if (level + 1 < max_level) {
		} else if (level + 1 < max_level && level < search_mask[i]) {
			count += search_data_recursive(i, goal, buffer, level+1, max_level,
				search_mask, links);
		} else {
			// count++;
		}
	}
	return count;
}

inline int start_parallel_tasks(int start, int current, int goal, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links) {

	int count = 0;
	for (unsigned long int k = 0; k < links[current].size(); k++) {
		int i = links[current][k];
		#pragma omp task default(none) \
			firstprivate(k, i, start, current, goal, level, max_level) \
			shared(search_mask, links, count, stdout)
		{
			if (level == 0) {
				printf("\r%lu / %lu\t", k, links[current].size());
				fflush(stdout);
			}
			std::vector<int> buffer(max_level + 1);
			buffer[0] = start;
			buffer[level] = current;

			if (level + 1 < max_level && level < search_mask[i]) {
				int tmp = search_data_recursive(i, goal, buffer, 
					level+1, max_level, search_mask, links);
				#pragma omp atomic
					count += tmp;
			}
		}
	}
	return count;
}

int search_data_recursive_parallel(int start, int current, int goal, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links) {

	search_mask[current] = std::min(search_mask[current], (unsigned char)level);
	int c = 0;
	int count = 0;
	if (level == 0 && links[current].size() < 100 && max_level > 2) {
		for (int i : links[current]) {
			if (level == 0) {
				printf("\r%d / %lu\t", ++c, links[current].size());
				fflush(stdout);
			}
			count += search_data_recursive_parallel(start, i, goal, level+1, 
				max_level, search_mask, links);
		}
	} else {
		#pragma omp parallel
		{
			#pragma omp single
			{
				count += start_parallel_tasks(start, current, goal, level, 
					max_level, search_mask, links);
			}
			#pragma omp taskwait
		}
	}
	return count;
}

int find_all_paths(int start, int goal, int length, 
	std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links) {

	int paths;
	if (length < 3) {
		std::vector<int> buffer(length+1);
		paths = search_data_recursive(start, goal, buffer, 
			0, length, search_mask, links);
	} else {
		paths = search_data_recursive_parallel(start, start, goal, 
			0, length, search_mask, links);
	}
	return paths;
}



int search_data(int start, int goal, 
	boost::circular_buffer<int> &queue, const std::vector<std::vector<int>> &links,
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

	printf("length of path: %d (%d)\n", curr_depth + 1, counted);
	return curr_depth + 1;
}


void read_file(const char *filename, std::vector<std::vector<int>> &links) {
	int size = 10000;
	int num_articles, article_id, num_links;
	std::vector<int> ret(size);
	std::ifstream in(filename, std::ios::in | std::ios::binary);
	in.read((char*)&num_articles, sizeof(int));
	num_articles = reverse_bytes(num_articles);
	links.resize(70000000);
	for (int a = 0; a < num_articles; a++) {
		in.read((char*)&article_id, sizeof(int));
		in.read((char*)&num_links, sizeof(int));
		article_id = reverse_bytes(article_id);
		num_links = reverse_bytes(num_links);
		std::vector<int> temp;
		temp.reserve(num_links);
		// printf("%d, %d\n", article_id, num_links);
	
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
}

double get_wall_time(){
	struct timeval time;
	if (gettimeofday(&time,NULL)){
		return 0;
	}
	return (double)time.tv_sec + (double)time.tv_usec * .000001;
}

int main(int argc, char **argv) {
	const char *filename = "../memory_map_formatted.dat";
	boost::circular_buffer<int> queue(4000000);
	std::vector<std::vector<int>> links(70000000);
	std::vector<bool> searched(70000000);
	std::vector<unsigned char> search_mask(70000000);
	std::vector<int> buffer(10);
	read_file(filename, links);


	clock_t start, end;
	double ts, te;
	double elapsed_clock, elapsed_wall;
	std::fill(search_mask.begin(), search_mask.end(), 0xff);
	int l;
	// something -> europe (2)
	// int s = 1095706;
	// int e = 9239;
	// europe -> sean banan (3)
	// int s = 9239;
	// int e = 27703020;
	// sean banan -> sean whyte (5)
	int s = 27703020;
	int e = 22149654;
	printf("Branching: %lu\n", links[s].size());
	l = search_data(s, e, queue, links, searched);
	omp_set_num_threads(8);
	start = clock();
	ts = get_wall_time();


	int paths = find_all_paths(s, e, l, search_mask, links);

	end = clock();
	te = get_wall_time();
	elapsed_clock = ((double) (end - start)) / CLOCKS_PER_SEC;
	elapsed_wall = te - ts;
	printf("\relapsed (clock): %.3f seconds (%d)\n", elapsed_clock, paths);
	printf("\relapsed  (wall): %.3f seconds (%d)\n", elapsed_wall, paths);
/*
	start = clock();
	std::fill(search_mask.begin(), search_mask.end(), 0xff);
	l = search_data(9239, 27703020, queue, links, searched);
	// buffer[0] = 27703020;
	paths = search_data_recursive(9239, 27703020, buffer, 0, l, search_mask, links);
	end = clock();
	elapsed = ((double) (end - start)) / CLOCKS_PER_SEC;
	printf("elapsed: %.3f seconds (%d)\n", elapsed, paths);

	start = clock();
	std::fill(search_mask.begin(), search_mask.end(), 0xff);
	l = search_data(27703020, 22149654, queue, links, searched);
	// buffer[0] = 27703020;
	paths = search_data_recursive(27703020, 22149654, buffer, 0, l, search_mask, links);
	end = clock();
	elapsed = ((double) (end - start)) / CLOCKS_PER_SEC;
	printf("elapsed: %.3f seconds (%d)\n", elapsed, paths);
*/
	return 0;
}