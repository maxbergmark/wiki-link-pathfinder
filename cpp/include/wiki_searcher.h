#pragma once

#include <vector>
#include <string>
#include <boost/circular_buffer.hpp>
#include <sys/time.h>


struct search_pair {
	int start;
	int end;
};

double get_wall_time();


class WikiSearcher {
private:

	int num_articles;
	int max_article_id;
	int maximum_length;
	std::vector<int> buffer; 
	std::vector<unsigned char> search_mask;
	std::vector<std::vector<int>> links;
	std::vector<std::vector<int>> back_links;
	std::vector<std::vector<int>> cache;
	boost::circular_buffer<int> queue;
	std::vector<bool> searched;

	inline bool valid_candidate(std::vector<int> &v);
	bool verify(std::vector<int> &buffer, int level);
	void cache_insert(int idx, int i);
	void search_data_recursive(int start, int goal, std::vector<int> &buffer, 
		int level, int max_level, long &count);
	void start_parallel_tasks(int start, int current, int goal, 
		int level, int max_level, long &count);
	int search_data_recursive_parallel(int start, int current, int goal, 
		int level, int max_level, long &count);
	long find_all_paths(int start, int goal, int length);
	int search_data(int start, int goal);
	void read_file(const char *filename);
	void read_back_links(const char *filename);
	void rebuild_paths(int start, int goal, int level, 
		std::vector<int> &buffer, int &total, std::string &paths_string);
	void backwards_pass(int start, int goal, 
		int level, int &max_level, bool &valid);
	void print_cache();
	void print(long id, std::vector<int> const &input);
	void reset_containers();

public:
	std::string perform_search(int s, int e);
	void read_files(const char *filename, const char *back_filename);
	void read_files();
	WikiSearcher();
};