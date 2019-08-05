//regular imports
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Stack;
import java.util.Scanner;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.stream.Collectors;


//from .jar file
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

//for fetching URLs
import java.net.URLEncoder;
import java.net.URL;

import java.lang.reflect.Array;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.*;

public class WikiSearcher {

	private int[][] m2;
	private int[][] distances;
	private int[][] complete_data;
	// jesus = 1095706:
	private int goal = 1095706; //2731583;// 
	private int max_number;
	private int total_articles;
	private HashMap<Integer, Integer> searched;
	private int max_branch = Integer.MAX_VALUE;

	private List<List<Integer>> foundSolutions;
	private int[] searchMask;
	private CircularQueue<Integer> articles;
	private WikiAPIHandler wikiAPI;

	/**
	* Constructor for the Process class, starts data processing.
	*/

	public WikiSearcher() {

		articles = new CircularQueue<Integer>(Integer.class, 1000000);
		wikiAPI = new WikiAPIHandler();

		System.out.println("\nData intitialized.\nReading files.");
		long t0 = System.nanoTime();
		readFiles3();
		searchMask = new int[max_number + 1];
		long t1 = System.nanoTime();
		System.out.println(String.format(
			"Files read in %.3f seconds", (t1-t0) * 1e-9));
		// playGame();

	}

	private void playGame() {

		while (true) {
			System.out.print("\nStarting point: ");
			String inp = System.console().readLine();
			if (inp.equals("change_max_branch")) {
				System.out.print("New max branching: ");
				String branch = System.console().readLine();
				if (branch.equals("max")) {
					max_branch = Integer.MAX_VALUE;
				} else {
					int temp = Integer.parseInt(branch);
					if (temp <= 0) {
						System.out.println(
							"Branch must be greater than zero.");
						continue;
					} else {
						max_branch = temp;
					}
				}
				System.out.println("Branch changed, restarting.");
				continue;
			}
			if (inp.equals("get_article_info")) {
				System.out.print("Article ID: ");
				String strID = System.console().readLine();
				int tempID = Integer.parseInt(strID);
				for (int i : m2[tempID]) {
					System.out.print(i + " ");
				}
				System.out.println(Arrays.toString(distances[tempID]));
				System.out.println();
				System.out.println("Done!");
				continue;
			}
			int id;
			try {
				id = wikiAPI.getId(inp);
			} catch (Exception e) {
				System.out.println(e.getMessage());
				continue;
			}
			String goal_inp = System.console().readLine();
			try {
				goal = wikiAPI.getId(goal_inp);
			} catch (Exception e) {
				System.out.println(e.getMessage());
				continue;
			}
		}
	}

	public List<List<WikiArticle>> findAllPathsByName(String start, String end) {

		int id;
		try {
			id = wikiAPI.getId(start);
			goal = wikiAPI.getId(end);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
		return findAllPathsById(id, goal);	
	}

	public List<List<WikiArticle>> findAllPathsById(int start, int end) {

		int id = start;
		goal = end;
		searched = new HashMap<>();
		foundSolutions = new ArrayList<>();
		int maxLength = searchData(id);
		int[] buffer = new int[16];
		for (int i = 0; i < max_number; i++) {
			searchMask[i] = maxLength;
		}
		buffer[0] = id;
		checked = 0;
		skipped = 0;
		articles.reset();
		long t0 = System.nanoTime();
		searchDataRecursive(id, buffer, 0, maxLength);
		long t1 = System.nanoTime();
		// printPaths(foundSolutions);
		long totalCount = checked + skipped;
		System.out.println(String.format(
			"\nChecked: %d (%.2f%%)\ttime: %.3fs", 
			checked, (1e2 * checked) / totalCount, (t1-t0) * 1e-9));
		Map<Integer, String> titles = getMultpleTitles(foundSolutions);
		List<List<WikiArticle>> articlePaths = new ArrayList<>();
		for (List<Integer> path : foundSolutions) {
			List<WikiArticle> p  = new ArrayList<>();
			for (int i : path) {
				p.add(new WikiArticle(i, titles.get(i)));
			}
			articlePaths.add(p);
		}
		return articlePaths;
		// return null;
		// return foundSolutions;	
	}

	private void printInfo(long t0, int artSize) {
		System.out.print(String.format(
			"\r%8d/%8d (%8d) articles searched. (%5.2f%%) (%6.2fs)", 
			searched.size(), total_articles, artSize, 
			(double) (searched.size())*1e2/(double) (total_articles),
			(System.nanoTime()-t0)/1e9));		
	}

	private long checked;
	private long skipped;

	private int searchDataRecursive(int start, int[] buffer, int level, int maxLevel) {
		searchMask[start] = searchMask[start] > level ? level : searchMask[start];
		int c = 0;
		for (int i : m2[start]) {
			if (level == 0) {
				System.out.print(String.format("\r%d / %d", ++c, m2[start].length));
			}
			if (i == goal) {
				ArrayList<Integer> temp = new ArrayList<>();
				HashSet<Integer> ids = new HashSet<>();
				for (int j = 0; j < level+1; j++) {
					temp.add(buffer[j]);
				}
				temp.add(i);
				foundSolutions.add(temp);
				return level + 1;
			}
			if (searchMask[i] <= level || level + 1 >= maxLevel) {
				skipped++;
				continue;
			}
			checked++;
			buffer[level+1] = i;
			int length = searchDataRecursive(i, buffer, level+1, maxLevel);
			if (length != -1 && length < maxLevel) {
				maxLevel = length;
			}
		}
		return maxLevel;
	}


	private int searchData(int start) {
		articles.add(start);
		boolean foundGoal = false;
		// int minLength;

		long t0 = System.nanoTime();
		long t1;
		System.out.println();

		while (true) {
			if (articles.isEmpty()) {
				t1 = System.nanoTime();
				double fail_time = (t1-t0)/1e9;
				printInfo(t0, articles.size());
				System.out.println("\n\nNo path found.");
				System.out.println(String.format(
					"Time elapsed: %.3f seconds.", fail_time));
				return -1;
			}

			int art = articles.poll();
			if (art == goal) {
				break;		
			}
			if (searched.size() % 1000 == 0) {
				printInfo(t0, articles.size());
			}

			if (m2[art].length <= max_branch || art == start) {
				for (int i : m2[art]) {
					if (!searched.containsKey(i)) {
						if (!foundGoal) {
							articles.add(i);
						}
						searched.put(i, art);
					}

					if (i == goal) {
						foundGoal = true;
						break;
					}
				}
			}
			if (foundGoal) {
				break;
			}
		}

		t1 = System.nanoTime();
		double time_elapsed = (t1-t0)/1e9;
		printInfo(t0, articles.size());
		System.out.println(String.format(
			"\n\nGoal found in %.3f seconds!\n", time_elapsed));

		int g = goal;
		int count = 0;
		while (g != start) {
			g = searched.get(g);
			count++;
		}
		System.out.println("Found path of length " + count);
		return count;
	}

	private Map<Integer, String> getMultpleTitles(List<List<Integer>> paths) {
		HashSet<Integer> ids = new HashSet<>();
		for (List<Integer> path : paths) {
			for (int i : path) {
				ids.add(i);
			}
		}

		return wikiAPI.getAllTitles(ids);
	}

	private void printPaths(List<List<Integer>> paths) {
		Map<Integer, String> titles = getMultpleTitles(paths);
		System.out.println(String.format("\rFound %d paths of minimum length:\n", paths.size()));
		for (List<Integer> path : paths) {
			printPath(path, titles);
		}

	}

	private void printPath(List<Integer> path, Map<Integer, String> titles) {
		System.out.print("\r\t");
		for (int i = 0; i < path.size() - 1; i++) {
			System.out.print(titles.get(path.get(i)) + " -> ");
		}
		System.out.println(titles.get(path.get(path.size() - 1)));

	}

	/**
	* Older method for processing the search paths to a specific article.
	* Dumps the result to a file specified within the method.
	*/

	private void finalProcess() {

		int c = 0;
		for (int[] i : distances) {
			if (i != null) {
				c++;
			}
		}
		System.out.println(Arrays.toString(distances[goal]));

		complete_data = new int[c][];
		c = 0;
		for (int i = 0; i < distances.length; i++) {
			if (distances[i] != null) {
				complete_data[c++] = new int[] 
				{i, distances[i][0], distances[i][1]};
			}
		}
		System.out.println(Arrays.toString(complete_data[goal]));

		Arrays.sort(complete_data, new Comparator<int[]>() {
			public int compare(int[] a, int[] b) {
				return Integer.compare(a[0], b[0]);
			}
		});

		System.out.println("Data formatted and sorted.");

		// m2 = null;
		// distances = null;
		StringBuilder s = new StringBuilder();
		for (int[] i : complete_data) {
			for (int j : i) {
				s.append(j + " ");
			}
			s.append("\n");
		}
		try {
			File f = new File("game_data_jesus.txt");
			PrintWriter p = new PrintWriter(f);
			p.print(s);
			p.flush();
			p.close();
			System.out.println("Data printed to file.");
		} catch (Exception e) {
			System.out.println("Error in file writing.");
		}
	}

	/**
	* Method for checking if a value exists in a sorted list.
	* @return true if value exists in list, else false.
	* @param list Sorted list of integers to be checked.
	* @param value Integer value to be checked for.
	*/

	private static boolean find(int[] list, int value) {
		if (list.length == 0) {
			return false;
		}
		int i0 = 0;
		int i1 = list.length-1;
		int count = 0;
		if (value < list[i0] || value > list[i1]) {
			return false;
		} else if (value == list[i0] || value == list[i1]) {
			return true;
		}
		while (i1-i0 > 1) {
			count++;
			int i = (i0+i1)/2;
			if (value < list[i]) {
				i1 = i;
			} else if (value > list[i]) {
				i0 = i;
			} else {
				return true;
			}
		}
		return false;
	}

	/**
	* Older method for pre-rendering search paths to a specific article.
	*/

	private void getDistances() {

		Queue<Integer> q = new LinkedList<>();
		long count = 0;
		int processed = 0;
		int lastSize = -1;
		for (int i = 0; i < max_number; i++) {
			if (m2[i] != null) {
				if (i == goal) {
					System.out.println("Original article found.");
					distances[i] = new int[] {0, -1};
					processed++;
				} else if (find(m2[i], goal)) {
					distances[i] = new int[] {1, goal};
					processed++;
				} else {
					q.add(i);
				}
			}
		}

		while (!q.isEmpty()) {
			int art = q.poll();
			count++;
			if (count % 1000000 == 0) {
				System.out.println(String.format(
					"Count: %d, Queue size: %d", count, q.size()));
				if (processed == lastSize) {
					System.out.println("No new edges found.");
					break;
				}
				lastSize = processed;
			}
			int minVal = Integer.MAX_VALUE;
			int minArt = -1;
			for (int i : m2[art]) {
				if (distances[i] != null && distances[i][0] < Integer.MAX_VALUE) {
					if (distances[i][0] + 1 < minVal) {
						minVal = distances[i][0] + 1;
						minArt = i;
					}
				}
			}
			q.add(art);
			
			if (distances[art] == null || minVal < distances[art][0]) {
				distances[art] = new int[] {minVal, minArt};
				processed++;
			}
		}

		System.out.println(Arrays.toString(distances[goal]));

	}

	private void readFiles3() {
		try(RandomAccessFile file = new RandomAccessFile("memory_map_formatted.dat", "rw")) {
			MappedByteBuffer out = file.getChannel()
				.map(FileChannel.MapMode.READ_ONLY, 0, 2_000_000_000);
			total_articles = out.getInt();
			max_number = out.getInt();
			System.out.println("max_number: " + max_number);
			m2 = new int[max_number + 1][];
			System.out.println("articles: " + total_articles);
			for (int i = 0; i < total_articles; i++) {
				int article = out.getInt();
				int numLinks = out.getInt();
				m2[article] = new int[numLinks];
				for (int j = 0; j < numLinks; j++) {
					m2[article][j] = out.getInt();
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println(e);
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public static void main(String[] args) {
		new WikiSearcher();
	}
}