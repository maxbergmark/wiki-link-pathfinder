#include "wiki_searcher.h"
#include <fstream>
#include <omp.h>
#include <string.h>

#define DEBUG false

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

void WikiSearcher::print(long id, std::vector<int> const &input) {
	printf("\r%4ld: ", id);
	for (unsigned long i = 0; i < input.size(); i++) {
		printf("%8d%s", input[i], (i+1 < input.size()) ? " -> " : "\n");
	}
}

inline bool WikiSearcher::valid_candidate(std::vector<int> &v) {
	if (v.size() == 0) {
		return false;
	}
	return std::find(v.begin(), v.end(), -1) != v.end();
	// return v[v.size() - 1] == -1;
}

bool WikiSearcher::verify(std::vector<int> &buffer, int level) {
	
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

void WikiSearcher::cache_insert(int idx, int i) {
	// if(cache[idx].size() > 0 && cache[idx][cache[idx].size() - 1] == -1) {
		// return;
	// }
	if(std::find(cache[idx].begin(), cache[idx].end(), i) == cache[idx].end()) {
		cache[idx].push_back(i);
	}
}

void WikiSearcher::search_data_recursive(int start, int goal, 
	std::vector<int> &buffer, int level, int max_level, long &count) {

	buffer[level] = start;
	search_mask[start] = std::min(search_mask[start], (unsigned char)level);
	int tmp_count = 0;
	for (int i : links[start]) {
		if (level + 1 == max_level && valid_candidate(cache[i])) {
			#pragma omp critical
			{
				buffer[max_level] = i;
				for (int j = 0; j < (int) buffer.size() - 1; j++) {
					cache_insert(buffer[j], buffer[j+1]);
				}
			}
		} else if (level + 1 < max_level && level + 1 < search_mask[i]) {
			search_data_recursive(i, goal, buffer, level+1, max_level, count);
		} else if (level + 1 == search_mask[i] && cache[i].size() > 0) {
			#pragma omp critical
			{
				for (int j = 0; j < level; j++) {
					cache_insert(buffer[j], buffer[j+1]);
				}
				cache_insert(start, i);
			}
		} else {
			tmp_count++;
		}
	}
	#pragma omp atomic
		count += tmp_count;
}

void WikiSearcher::start_parallel_tasks(int start, int current, int goal, 
	int level, int max_level, long &count) {

	for (unsigned long int k = 0; k < links[current].size(); k++) {
		int i = links[current][k];
		#pragma omp task default(none) \
			firstprivate(k, i, start, current, goal, level, max_level) \
			shared(search_mask, links, count, cache, stdout)
		{
			if (DEBUG && level == 0) {
				printf("\r%lu / %lu\t", k, links[current].size());
				fflush(stdout);
			}
			std::vector<int> buffer(max_level + 1);
			buffer[0] = start;
			buffer[level] = current;

			if (level + 1 < max_level && level + 1 < search_mask[i]) {
				search_data_recursive(i, goal, buffer, 
					level+1, max_level, count);
			} else if (level + 1 == search_mask[i] && cache[i].size() > 0) {
				#pragma omp critical
				{
					for (int j = 0; j < level; j++) {
						cache_insert(buffer[j], buffer[j+1]);
					}
					cache_insert(current, i);
				}
			}
		}
	}
}

int WikiSearcher::search_data_recursive_parallel(int start, int current, 
	int goal, int level, int max_level, long &count) {

	search_mask[current] = std::min(search_mask[current], (unsigned char)level);
	int c = 0;
	if (level == 0 && links[current].size() < 50 && max_level > 2) {
		for (int i : links[current]) {
			if (DEBUG && level == 0) {
				printf("\r%d / %lu\t", ++c, links[current].size());
				fflush(stdout);
			}
			search_data_recursive_parallel(start, i, goal, level+1, 
				max_level, count);
		}
	} else {
		start_parallel_tasks(start, current, goal, level, 
			max_level, count);
	}
	return count;
}

long WikiSearcher::find_all_paths(int start, int goal, int length) {

	long paths = 0;
	if (length < 3) {
		std::vector<int> buffer(length+1);
		search_data_recursive(start, goal, buffer, 
			0, length, paths);
	} else {
		#pragma omp parallel
		{
			#pragma omp single nowait
			{
				search_data_recursive_parallel(start, start, goal, 
					0, length, paths);
			}
		}
	}
	return paths;
}



int WikiSearcher::search_data(int start, int goal) {
	
	queue.clear();
	queue.push_back(start);
	bool found_goal = false;
	int curr_depth = 0;
	int counted = 0;
	std::vector<int> link_counter(100);
	link_counter[curr_depth]++;

	while (true) {
		if (queue.empty()) {
			// printf("queue empty: %d\n", curr_depth);
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
	return curr_depth + 1;
}

void WikiSearcher::read_file(const char *filename) {
	
	int size = 10000;
	int article_id, num_links;
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
		// printf("article id: %d -> %d\n", article_id, num_links);
		article_id = reverse_bytes(article_id);
		num_links = reverse_bytes(num_links);
		std::vector<int> temp(num_links);
		for (int i = 0; i < num_links; i += size) {
			int temp_num = std::min(size, num_links - i);
			in.read((char*)&ret[0], temp_num * sizeof(int));
			for (int j = 0; j < temp_num; j++) {
				// printf("link: %d\n", ret[j]);
				temp[i+j] = reverse_bytes(ret[j]);
			}
		}
		links[article_id] = (const std::vector<int>)temp;
	}
	in.close();
}

void WikiSearcher::read_back_links(const char *filename) {
	
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
		std::vector<int> temp(num_links);
		for (int i = 0; i < num_links; i += size) {
			int temp_num = std::min(size, num_links - i);
			in.read((char*)&ret[0], temp_num * sizeof(int));
			for (int j = 0; j < temp_num; j++) {
				temp[i+j] = reverse_bytes(ret[j]);
			}
		}
		back_links[article_id] = (const std::vector<int>)temp;
	}
	in.close();
}

void WikiSearcher::rebuild_paths(int start, int goal, int level, 
	std::vector<int> &buffer, int &total, std::string &paths_string) {

	if (level == 0) {
		paths_string += "{\"paths\":[";
	}
	buffer[level] = start;
	if (start == goal) {
		total++;
		paths_string += '[';
		for (int i = 0; i < (int) buffer.size() - 1; i++) {
			paths_string += std::to_string(buffer[i]);
			paths_string += ',';
		}
		paths_string += std::to_string(buffer[buffer.size() - 1]);
		paths_string += "],";
		// print(total, buffer);
		return;
	}
	for (int i : cache[start]) {
		if (i >= 0 && level + 1 < (int) buffer.size()) {
			rebuild_paths(i, goal, level + 1, buffer, total, paths_string);
		}
	}
	if (level == 0) {
		paths_string.pop_back();
		paths_string += "],\"num_paths\":";
		paths_string += std::to_string(total);
		paths_string += '}';
	}
}

void WikiSearcher::backwards_pass(int start, int goal, 
	int level, int &max_level, bool &valid) {

	if (level == max_level || start == goal) {
		valid = true;
		cache_insert(goal, -1);
		return;
	}
	int f = 5;
	if (level == 0 && (int)back_links[goal].size() > 10000*f) {
		valid = true;
		cache_insert(goal, -1);
		max_level = std::min(max_level, 0);
		return;
	} else if (level == 0 && (int)back_links[goal].size() > 1000*f) {
		max_level = std::min(max_level, 1);
	} else if (level == 0) {
		int tot_links = 0;
		for (int i : back_links[goal]) {
			tot_links += back_links[i].size();
		}
		if (tot_links > 1000*f) {
			max_level = std::min(max_level, 1);
		} else {
			int tot_further = 0;
			for (int i : back_links[goal]) {
				for (int j : back_links[i]) {
					tot_further += back_links[j].size();
				}
			}
			if (tot_further > 20000*f) {
				max_level = std::min(max_level, 2);
			}

		}
	}

	for (int i : back_links[goal]) {
		cache_insert(i, goal);
		if (level + 1 < max_level) {
			backwards_pass(start, i, level + 1, max_level, valid);
		} else {
			valid = true;
			cache_insert(i, -1);
		}
	}
}

void WikiSearcher::print_cache() {
	for (int i = 0; i < (int)cache.size(); i++) {
		std::vector<int> v = cache[i];
		if (v.size() > 0) {
			printf("\r%d: ", i);
			for (int j : v) {
				printf("%d ", j);
			}
			printf("\n");
		}
	}	
}

void WikiSearcher::reset_containers() {
	std::fill(searched.begin(), searched.end(), false);
	std::fill(search_mask.begin(), search_mask.end(), 0xff);
	#pragma omp parallel for schedule(static)
	for (int i = 0; i < (int) cache.size(); i++) {
		if (cache[i].size() > 0) {
			cache[i].clear();
			// cache[i].reserve(10);
		}
	}
}

std::string WikiSearcher::perform_search(int s, int e) {

	if (links[s].size() == 0 || back_links[e].size() == 0) {
		std::string error_string = "{\"error\":404,"
			"\"message\":\"articles_do_not_have_links\"}";
		return error_string;
	}
	if (s == e) {
		std::string same_start_end = "{\"paths\":[" + std::to_string(s) 
			+ "],\"num_paths\":1,\"elapsed_time\":0,\"clicks\":0}";
		return same_start_end;

	}

	clock_t start, end;
	double ts, te, tots, tote;
	double elapsed_clock, elapsed_wall, path_rebuild_wall;

	ts = get_wall_time();
	long paths = 0;
	double bfs_wall, reset_time, total_wall_time;
	tots = get_wall_time(); 

	int l, total = 0;
	int backpass_level = 3;
	bool valid = false;
	backwards_pass(s, e, 0, backpass_level, valid);
	if (!valid) {
		std::string no_paths_string = "{\"paths\":[],\"num_paths\":0,"
			"\"elapsed_time\":" + std::to_string(bfs_wall)
			+ "\"clicks\":-1}";
		printf("no valid backpass found\n");
		return no_paths_string;
	}
	// return "debug";
	te = get_wall_time();
	double backpass_time = te - ts;
	if (valid_candidate(cache[s])) {
		// printf("found solution with the backpass: %.3f ms\n", 
			// backpass_time * 1e3);
		l = 0;
		bfs_wall = 0;
		elapsed_wall = 0;
		elapsed_clock = 0;
	} else {
		ts = get_wall_time();
		l = search_data(s, e);
		te = get_wall_time();
		bfs_wall = te - ts;

		if (l == -1) {
			std::string no_paths_string = "{\"paths\":[],\"num_paths\":0,"
				"\"elapsed_time\":" + std::to_string(bfs_wall)
				+ "\"clicks\":-1}";
			if (bfs_wall + backpass_time > 1) {
				printf("No path found: %.3fms / %.3fms\n", 
					bfs_wall * 1e3, backpass_time * 1e3);
			}
			return no_paths_string;
		}
		if (l + backpass_level > maximum_length) {
			maximum_length = std::max(maximum_length, l + backpass_level);
			printf("\rNew maximum length found: %d (%d -> %d)\n", 
				maximum_length, s, e);
		}

		start = clock();
		ts = get_wall_time();
		paths = find_all_paths(s, e, l);
		end = clock();
		te = get_wall_time();
		elapsed_clock = ((double) (end - start)) / CLOCKS_PER_SEC;
		elapsed_wall = te - ts;
	}

	ts = get_wall_time();
	std::vector<int> buffer(l + backpass_level + 1);
	std::string paths_string;
	rebuild_paths(s, e, 0, buffer, total, paths_string);
	te = get_wall_time();
	path_rebuild_wall = te - ts;

	ts = get_wall_time();
	reset_containers();
	te = get_wall_time();
	reset_time = te - ts;

	tote = get_wall_time();
	total_wall_time = tote - tots;

	double without_reset = total_wall_time - reset_time;
	paths_string.pop_back();
	paths_string += ",\"elapsed_time\":" + std::to_string(without_reset)
		+",\"clicks\":" + std::to_string(l) + "}";

	if (DEBUG || without_reset > 1) {
		printf("\rpath from %d -> %d\n", s, e);
		printf("\rcontainer reset time: %.3f ms\n", reset_time * 1e3);
		printf("\rbackpass time: %.3f ms (%d)\n", 
			backpass_time * 1e3, backpass_level);
		printf("\rpath of length %d found in %.3f ms\n", 
			l + backpass_level, bfs_wall * 1e3);
		printf("\rtotal number of paths: %d\n", total);
		printf("\relapsed (clock): %.3f ms (%ld)\n", 
			elapsed_clock * 1e3, paths);
		printf("\relapsed  (wall): %.3f ms (%ld)\n", elapsed_wall * 1e3, paths);
		printf("\rrebuild paths: %.3f ms\n", path_rebuild_wall * 1e3);
		printf("\rtotal elapsed (wall): %.3f ms\n", total_wall_time * 1e3);
		printf("\rtotal search time (wall): %.3f ms (%d)\n\n", 
			without_reset * 1e3, l + backpass_level);
	}
	// return without_reset;
	return paths_string;
}

void WikiSearcher::read_files(const char *filename, const char *back_filename) {

	double ts, te;
	ts = get_wall_time();
	read_file(filename);
	read_back_links(back_filename);
	searched.resize(max_article_id + 1);
	search_mask.resize(max_article_id + 1);

	cache.resize(max_article_id + 1);
	reset_containers();
	te = get_wall_time();
	double file_read_wall = te - ts;
	if (DEBUG) {
		printf("\rfile read: %.3f seconds (%d / %d)\n\n", 
			file_read_wall, num_articles, max_article_id);
	}
}

void WikiSearcher::read_files() {
	const char *filename = "../memory_mapped_files/memory_map_formatted.dat";
	const char *back_filename = "../memory_mapped_files/"
		"memory_map_backlinks.dat";
	read_files(filename, back_filename);
}

WikiSearcher::WikiSearcher() : queue(8000000) {
	maximum_length = 0;
	omp_set_num_threads(8);
}
extern "C" {
	WikiSearcher* WikiSearcher_new() {
		return new WikiSearcher();
	}
	void load_files(WikiSearcher* searcher) {
		const char *filename = "../../memory_mapped_files/"
			"memory_map_formatted.dat";
		const char *back_filename = "../../memory_mapped_files/"
			"memory_map_backlinks.dat";
		searcher->read_files(filename, back_filename);
	}
	char* search_wiki(WikiSearcher* searcher, int s, int e) {
		std::string str = searcher->perform_search(s, e);
		const char *c_str = str.c_str();
		int size = strlen(c_str);
		char* arr = (char*) malloc(size * sizeof(char));
		printf("created %p with size %d\n", arr, size);
		strcpy(arr, str.c_str());
   		return arr;
	}
	void free_pointer(char* p) {
		printf("freeing %p\n", p);
		free(p);
		printf("pointer freed\n");
	}
}