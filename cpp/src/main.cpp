#include <boost/circular_buffer.hpp>
#include <iostream>
#include <fstream>
// #include <ios>
#include <vector>
#include <algorithm>
#include <time.h>
#include <omp.h>

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

int search_data_recursive(int start, int goal, std::vector<int> &buffer, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links) {
	buffer[level] = start;
	search_mask[start] = std::min(search_mask[start], (unsigned char)level);
	int count = 0;
	for (int i : links[start]) {
		// count += (i == goal);
		if (i == goal) {
			count++;
			for (int j = 0; j < level+1; j++) {
				printf("%d, ", buffer[j]);
			}
			printf("%d\n", goal);
		} else if (level + 1 < max_level && level < search_mask[i]) {
			count += search_data_recursive(i, goal, buffer, level+1, max_level,
				search_mask, links);
		}
	}
	return count;
}

void search_data_recursive_parallel(int start, int goal, 
	int level, int max_level, std::vector<unsigned char> &search_mask,
	const std::vector<std::vector<int>> &links) {

	// search_mask[start] = std::min(search_mask[start], (unsigned char)level);
	int c = 0;
	// int s, g, l, m;
	// #pragma omp parallel for
	// for (unsigned long int k = 0; k < links[start].size(); k++) {
	for (int i : links[start]) {
		// int i = links[start][k];
		if (level == 0) {
			printf("\r%d / %lu", ++c, links[start].size());
			fflush(stdout);
		}
		// if (level == 0 && level + 1 < max_level) {
			// #pragma omp task private(start, goal, level, max_level)
			// {
				std::vector<int> buffer(10);
				buffer[0] = start;

				// if (level + 1 < max_level && level < search_mask[i]) {
				if (level + 1 < max_level) {
					buffer[level+1] = i;
					search_data_recursive(i, goal, buffer, level+1, max_level,
						search_mask, links);
				}
			// }
		// } else {
			// if (level + 1 < max_level && level < search_mask[i]) {
			// if (level + 1 < max_level) {
				// buffer[level+1] = i;
				// search_data_recursive(i, goal, buffer, level+1, max_level,
					// search_mask, links);
			// }
		// }
	}
	#pragma omp taskwait
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

		if (art == goal) {
			break;		
		}

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
			// printf("new depth: %d, %lu\n", curr_depth, queue.size());
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
	std::vector<int> ret(size); //std::list may be preferable for large files
	std::ifstream in(filename, std::ios::in | std::ios::binary);
	in.read((char*)&num_articles, sizeof(int));
	num_articles = reverse_bytes(num_articles);
	links.resize(70000000);
	for (int a = 0; a < num_articles; a++) {
		in.read((char*)&article_id, sizeof(int));
		in.read((char*)&num_links, sizeof(int));
		article_id = reverse_bytes(article_id);
		num_links = reverse_bytes(num_links);
		std::vector<int> temp(num_links);
		// printf("%d, %d\n", article_id, num_links);
		for (int i = 0; i < num_links; i += size) {
			int temp_num = std::min(size, num_links - i);
			in.read((char*)&ret[0], temp_num * sizeof(int));
			for (int j = 0; j < temp_num; j++) {
				temp.push_back(reverse_bytes(ret[j]));
				// links[article_id].push_back(reverse_bytes(ret[j]));	
			}
		}
		links[article_id] = (const std::vector<int>)temp;
	}
	in.close();
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
	double elapsed;
	start = clock();
	std::fill(search_mask.begin(), search_mask.end(), 0xff);
	int l = search_data(1095706, 9239, queue, links, searched);
	omp_set_num_threads(8);
	int paths = search_data_recursive(1095706, 9239, buffer, 0, l, search_mask, links);
	end = clock();
	elapsed = ((double) (end - start)) / CLOCKS_PER_SEC;
	printf("elapsed: %.3f seconds (%d)\n", elapsed, paths);

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

	return 0;
}