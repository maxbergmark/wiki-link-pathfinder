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

inline bool valid_candidate(std::set<int> s) {
	if (s.size() == 0) {
		return false;
	}
	auto search = s.find(-1);
    return search != s.end();
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
		// if (level + 1 == max_level && i == goal) {
		if (level + 1 == max_level && valid_candidate(cache[i])) {
			#pragma omp critical
			{
				buffer[max_level] = i;
				// count++;
				// print(count, buffer);
				for (int j = 0; j < (int) buffer.size() - 1; j++) {
					cache[buffer[j]].insert(buffer[j+1]);
				}
				// for (int s : cache[i]) {
					// printf("%d ", s);
				// }
				// printf("\n");
				// cache[i]
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
	std::vector<bool> &searched, std::vector<std::set<int>> &cache) {
	
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

			// if (i == goal) {
			if (valid_candidate(cache[i])) {
				found_goal = true;
				break;
			}
		}
		if (found_goal) {
			break;
		}
		if (link_counter[curr_depth] == 0) {
			curr_depth++;
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

void read_back_links(const char *filename, 
	std::vector<std::vector<int>> &back_links) {
	
	int size = 10000;
	int num_articles, article_id, num_links, max_article_id;
	std::vector<int> ret(size);
	std::ifstream in(filename, std::ios::in | std::ios::binary);
	in.read((char*)&num_articles, sizeof(int));
	num_articles = reverse_bytes(num_articles);
	in.read((char*)&max_article_id, sizeof(int));
	max_article_id = reverse_bytes(max_article_id);
	back_links.resize(max_article_id + 1);
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
		back_links[article_id] = (const std::vector<int>)temp;
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

// 27703020 ->     4489 ->    85971 ->    92615 ->  4386742 -> 22149654

void rebuild_paths(std::vector<std::set<int>> &cache, int start, int goal, 
	int level, std::vector<int> &buffer, int &total) {
		
	buffer[level] = start;
	if (start == goal) {
		total++;
		// print(total, buffer);
		return;
	}
	for (int i : cache[start]) {
		if (i >= 0 && level + 1 < (int) buffer.size()) {
			// printf("next iteration: %d -> %d\n", start, i);
			rebuild_paths(cache, i, goal, level + 1, buffer, total);
		}
	}
}

void backwards_pass(int start, int goal, int level, int &max_level, 
	std::vector<std::vector<int>> &back_links,
	std::vector<std::set<int>> &cache) {

	if (level == max_level || start == goal) {
		cache[goal].insert(-1);
		return;
	}

	if (level == 0 && back_links[goal].size() > 10000) {
		// printf("links: %lu\n", back_links[goal].size());
		for (int i : back_links[goal]) {
			cache[i].insert(goal);
			cache[i].insert(-1);
		}
		max_level = 1;
		return;
	}

	for (int i : back_links[goal]) {
		cache[i].insert(goal);
		if (level + 1 == max_level) {
			cache[i].insert(-1);
		}
		if (level + 1 < max_level) {
			backwards_pass(start, i, level + 1, max_level, back_links, cache);
		}
	}
	// return max_level;
}

void print_cache(std::vector<std::set<int>> &cache) {
	for (int i = 0; i < (int)cache.size(); i++) {
		std::set<int> ss = cache[i];
		if (ss.size() > 0) {
			printf("\r%d: ", i);
			for (int j : ss) {
				printf("%d ", j);
			}
			printf("\n");
		}
	}	
}

void reset_containers(std::vector<std::set<int>> &cache, 
	std::vector<bool> &searched, 
	std::vector<unsigned char> &search_mask) {

	std::fill(searched.begin(), searched.end(), false);
	std::fill(search_mask.begin(), search_mask.end(), 0xff);
	// int size = cache.size();
	// cache.clear();
	// cache.resize(size);
	#pragma omp parallel for
	for (int i = 0; i < (int) cache.size(); i++) {
		cache[i].clear();
	}
	// for (std::set<int> s : cache) {
		// printf("analyzing set: %lu\n", s.size());
		// if (s.size() > 0) {
			// printf("size too large\n");
		// }
	// }
	// printf("size of set %d: %lu\n", 9239, cache[9239].size());
}

void perform_search(int s, int e, boost::circular_buffer<int> &queue,
	std::vector<std::vector<int>> &links,
	std::vector<std::vector<int>> &back_links,
	std::vector<std::set<int>> &cache,
	std::vector<bool> &searched,
	std::vector<unsigned char> &search_mask

	) {

	clock_t start, end;
	double ts, te, tots, tote;
	double elapsed_clock, elapsed_wall;
	double bfs_wall, reset_time, total_wall_time, path_rebuild_wall;

	tots = get_wall_time(); 
	int l, total = 0;
	long paths = 0;
	ts = get_wall_time();
	int backpass_level = 3;
	backwards_pass(s, e, 0, backpass_level, back_links, cache);
	te = get_wall_time();
	double backpass_time = te - ts;
	// print_cache(cache);
	if (valid_candidate(cache[s])) {
		printf("found solution with the backpass: %.3f ms\n", backpass_time * 1e3);
		l = 0;
		bfs_wall = 0;
		elapsed_wall = 0;
		elapsed_clock = 0;
	} else {
		ts = get_wall_time();
		l = search_data(s, e, queue, links, searched, cache);
		te = get_wall_time();
		bfs_wall = te - ts;

		start = clock();
		ts = get_wall_time();
		paths = find_all_paths(s, e, l, search_mask, links, cache);
		end = clock();
		te = get_wall_time();
		elapsed_clock = ((double) (end - start)) / CLOCKS_PER_SEC;
		elapsed_wall = te - ts;
	}

	ts = get_wall_time();
	std::vector<int> buffer(l + backpass_level + 1);
	rebuild_paths(cache, s, e, 0, buffer, total);
	te = get_wall_time();
	path_rebuild_wall = te - ts;

	ts = get_wall_time();
	reset_containers(cache, searched, search_mask);
	te = get_wall_time();
	reset_time = te - ts;

	tote = get_wall_time();
	total_wall_time = tote - tots;		

	printf("\rcontainer reset time: %.3f ms\n", reset_time * 1e3);
	printf("\rbackpass time: %.3f ms\n", backpass_time * 1e3);
	printf("\rpath of length %d found in %.3f ms\n", 
		l + backpass_level, bfs_wall * 1e3);
	printf("\rtotal number of paths: %d\n", total);
	printf("\relapsed (clock): %.3f ms (%ld)\n", elapsed_clock * 1e3, paths);
	printf("\relapsed  (wall): %.3f ms (%ld)\n", elapsed_wall * 1e3, paths);
	printf("\rrebuild paths: %.3f ms\n", path_rebuild_wall * 1e3);
	printf("\rtotal elapsed  (wall): %.3f ms\n\n", total_wall_time * 1e3);

}

struct search_pair {
	int start;
	int end;
};

int main(int argc, char **argv) {
	const char *filename = "../memory_map_formatted.dat";
	const char *back_filename = "../memory_map_backlinks.dat";
	boost::circular_buffer<int> queue(4000000);
	std::vector<std::vector<int>> links;
	std::vector<std::vector<int>> back_links;
	std::vector<std::set<int>> cache;
	std::vector<bool> searched;
	std::vector<unsigned char> search_mask;
	omp_set_num_threads(8);

	double ts, te;

	ts = get_wall_time();
	article_count counts = read_file(filename, links);
	read_back_links(back_filename, back_links);
	searched.resize(counts.max_article_id + 1);
	search_mask.resize(counts.max_article_id + 1);
	cache.resize(counts.max_article_id + 1);
	reset_containers(cache, searched, search_mask);
	te = get_wall_time();
	double file_read_wall = te - ts;
	printf("\rfile read: %.3f seconds (%d / %d)\n", 
		file_read_wall, counts.articles, counts.max_article_id);
	std::vector<search_pair> searches;
	searches.push_back({1095706, 9239}); // jesus -> europe (2)
	searches.push_back({9239, 27703020}); // europe -> sean banan (3)
	searches.push_back({27703020, 22149654}); // sean banan -> sean whyte (5)	
	searches.push_back({22461, 2146249}); // osteoporosis -> danger zone (5)
	if (argc == 3) {
		searches.push_back({atoi(argv[1]), atoi(argv[2])});
	}
	for (search_pair p : searches) {
		perform_search(p.start, p.end, queue, links, 
			back_links, cache, searched, search_mask);		
	}

	return 0;
}