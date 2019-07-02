//regular imports
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
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


public class Process {

	private int[][] m2;
	private int[][] distances;
	private int[][] complete_data;
	// jesus = 1095706:
	private int goal = 1095706; //2731583;// 
	private int max_number = 53000000;
	private int total_articles;
	private HashMap<Integer, Integer> searched;
	private int max_branch = Integer.MAX_VALUE;

	/**
	* Constructor for the Process class, starts data processing.
	*/

	private Process() {

		m2 = new int[max_number][];

		System.out.println("\nData intitialized.\nReading files.");
		readFiles2();
/*
		System.out.println("Removing excess articles.");
		System.out.println("\n\nLinks before removal:");
		for (int i : m2[50453534]) {
			System.out.println(i + "\t" + getTitle(i));
		}
*/
		removeExcess();
		// playGame();
		distances = new int[max_number][];
/*
		System.out.println("\n\nLinks after removal:");
		for (int i : m2[50453534]) {
			System.out.println(i + "\t" + getTitle(i));
		}
*/
		// getDistances();
		// finalProcess();
		playGame();
	}

	private void playGame() {

		while (true) {
			try {
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
							throw new Exception(
								"Branch must be greater than zero.");
						} else {
							max_branch = temp;
						}
					}
					throw new Exception("Branch changed, restarting.");
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
					throw new Exception("Done!");
				}
				int id = getId(inp);
				System.out.println("Starting id: " + id);
				System.out.print("End point: ");
				String goal_inp = System.console().readLine();
				goal = getId(goal_inp);
				System.out.println("Goal id: " + goal);
				if (max_branch < Integer.MAX_VALUE) {
					System.out.println("Max branching: " + max_branch);
				}
				searched = new HashMap<>();
				searchData(id);
			} catch (Exception e) {
				System.out.println("error");
				System.out.println(e);
			}
		}
	}

	private void printInfo(long t0, int artSize) {
		System.out.print(String.format(
			"\r%8d/%8d (%8d) articles searched. (%5.2f%%) (%6.2fs)", 
			searched.size(), total_articles, artSize, 
			(double) (searched.size())*1e2/(double) (total_articles),
			(System.nanoTime()-t0)/1e9));		
	}

	private int searchData(int start) {
		Queue<Integer> articles = new LinkedList<>();
		articles.add(start);
		boolean willBreak = false;

		long t0 = System.nanoTime();
		long t1;
		System.out.println();

		while (true) {
			if (articles.isEmpty()) {
				t1 = System.nanoTime();
				double fail_time = (t1-t0)/1000000/1e3;
				printInfo(t0, articles.size());
				System.out.println("\n\nNo path found.");
				System.out.println("Time elapsed: " + fail_time + " seconds.");
				return -1;
			}
			int art = articles.poll();
			if (art == goal) {
				System.out.println("\nGoal found in zero clicks.\n");
				break;				
			}
			if (searched.size() % 1000 == 0) {
				printInfo(t0, articles.size());
			}
			if (m2[art].length <= max_branch || art == start) {
				for (int i : m2[art]) {
					if (!searched.containsKey(i)) {
						articles.add(i);
						searched.put(i, art);
					}
					if (i == goal) {
						t1 = System.nanoTime();
						double time_elapsed = (t1-t0)/1e9;
						printInfo(t0, articles.size());
						System.out.println(String.format(
							"\n\nGoal found in %.2f seconds!\n", time_elapsed));
						willBreak = true;
						break;
					}
				}
			}

			if (willBreak) {
				break;
			}
		}
		int g = goal;
		ArrayList<Integer> path = new ArrayList<>();
		path.add(goal);
		while (g != start) {
			g = searched.get(g);
			path.add(g);
		}

		ArrayList<String> titles = getAllTitles(path);

		for (int i = path.size() - 1; i > 0; i--) {
			System.out.println(titles.get(i) + " ->");
		}
		System.out.print(getTitle(path.get(0)));
		System.out.println(String.format(
			"\n\nGame completed in %d clicks.", (path.size()-1)));
		return path.size()-1;
	}

	private static JSONObject getJson(String urlString) {
		
		try {
			URL url = new URL(urlString);
			BufferedReader in = new BufferedReader(
				new InputStreamReader(url.openStream()));
			String total = "";
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				total += inputLine;
			}
			in.close();

			JSONParser parser = new JSONParser();
			Object obj = parser.parse(total);
			JSONObject o = (JSONObject) obj;
			return o;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;

	}

	private static int getId(String title) throws Exception {
		JSONObject o = getJson(getIdUrl(title));
		o = (JSONObject) o.get("query");
		o = (JSONObject) o.get("pages");
		for (Object s : o.keySet()) {
			String s2 = (String) s;
			int i2 = Integer.parseInt(s2);
			if (i2 == -1) {
				throw new Exception("Article not found.");
			}
			return Integer.parseInt(s2);
		}
		return -1;

	}

	private static String getIdUrl(String title) {
		try {
			return "https://en.wikipedia.org/w/api.php"
			+ "?format=json&action=query&titles="
			+ URLEncoder.encode(title, "UTF-8") + "&redirects";
		} catch (Exception e) {}
		return null;
	}

	/** 
	* Fetches titles for all article IDs in the input ArrayList.
	* @param ids An ArrayList containing all article IDs to be checked.
	* @return An ArrayList containing corresponding Wikipedia article titles.
	*/

	private static ArrayList<String> getAllTitles(ArrayList<Integer> ids) {
		ArrayList<String> ret = new ArrayList<>();
		JSONObject o = getJson(getMultipleUrl(ids));
		o = (JSONObject) o.get("query");
		o = (JSONObject) o.get("pages");
		for (int i : ids) {
			JSONObject o2 = (JSONObject) o.get("" + i);
			String s = (String) o2.get("title");
			ret.add(s);
		}
		return ret;

	}

	/**
	* Fetches the title of a Wikipedia article given its article ID.
	* @param id The article ID to be checked.
	* @return The title of the article with said article ID.
	*/	

	private static String getTitle(int id) {
		JSONObject o = getJson(getUrl(id));
		o = (JSONObject) o.get("query");
		o = (JSONObject) o.get("pages");
		o = (JSONObject) o.get("" + id);
		String s = (String) o.get("title");

		return s;
	}

	/**
	* Composes the API call for fetching a single 
	* Wikipedia article with an API call.
	* @param id The article ID to be checked.
	* @return A URL String to be used for the API call.
	*/

	private static String getUrl(int id) {
		return "https://en.wikipedia.org/w/api.php?"
		+ "format=json&action=query&pageids=" + id;
	}

	/**
	* Composes the API call for fetching multiple 
	* Wikipedia articles with a single API call.
	* @param ids An ArrayList containing all article IDs to be checked.
	* @return A URL String to be used for the API call.
	*/

	private static String getMultipleUrl(ArrayList<Integer> ids) {
		String s = "https://en.wikipedia.org/w/api.php?"
		+ "format=json&action=query&pageids=";
		for (int i = 0; i < ids.size()-1; i++) {
			s += ids.get(i) + "%7C";
		}
		s += ids.get(ids.size()-1);
		return s;
	}

	/**
	* Removes links to non-articles, saving 15% memory and reducing
	* search time in the graph m2.
	*/

	private void removeExcess() {
		int removed = 0;
		int total = 0;
		for (int i = 0; i < m2.length; i++) {
			if (m2[i] != null) {
				int count = 0;
				total += m2[i].length;
				for (int j : m2[i]) {
					if (m2[j] != null) {
						count++;
					} else {
						removed++;
					}
				}
				int[] temp = new int[count];
				count = 0;
				for (int j : m2[i]) {
					if (m2[j] != null) {
						temp[count++] = j;
					}
				}
				m2[i] = temp;
			}
		}
		System.out.println(String.format("%d/%d links removed. (%.2f%%)",
			removed, total, (double) (removed)*1e2/(double) (total)));
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



	private void readFiles2() {
		int count = 0;
		int total_count = 0;
		int size;
		long sum = 0;
//		Scanner s = new Scanner("");

		long t0 = System.nanoTime();

		try {
			for (int i = 0; i < 11; i++) {

				String fileName = "formatted_data/data_dump_" + i + ".txt";
				count++;
				ArrayList<List<Integer>> temp;

				System.out.print(String.format("\r%4d   %4dMB", i, sum/250000));


				try (Stream<String> lines = Files.lines(Paths.get(fileName)).parallel()) {
					temp = lines
					.map(String::trim)
					.map(s -> Arrays.stream(s.split(" "))
						.filter(w -> !w.trim().isEmpty())
						.map(s2 -> Integer.parseInt(s2, 16))
						.collect(Collectors.toList()))
					.collect(Collectors.toCollection(ArrayList<List<Integer>>::new));
				}
				total_count += temp.size()/3;
				for (int j = 0; 3*j < temp.size(); j++) {
					Stream<Integer> intStream = temp.get(3*j+2).stream();
					int[] intArray = intStream.mapToInt(k -> k).toArray();
					sum += intArray.length;
					// System.out.println(Arrays.toString(intArray));
					m2[temp.get(3*j).get(0)] = intArray;
					// System.out.println(temp.get(3*j+1) + "\t" + temp.get(3*j+2).size());
				}

			}
		} catch (NoSuchElementException e) {
//		} catch (IOException e) {
//			System.out.println("\n\nAll files loaded.");
		} catch (Exception e) {
			// System.out.println(e);
		}
		long t1 = System.nanoTime();
		total_articles = total_count;
		double elapsed = (t1-t0)/1e9;
		System.out.println(String.format(
			"\n\nAll %d files loaded in %.2f seconds.\n", 
			total_articles, elapsed));

	}

	/**
	* Reads all files from the new_data/ directory, 
	* and stores it in the int[][] m2.
	*/

	private  void readFiles() {

		int count = 0;
		int total_count = 0;
		int size;
		long sum = 0;
//		Scanner s = new Scanner("");
		FileReader f;
		BufferedReader b;

		long t0 = System.nanoTime();

		try {
			for (int i = 0; i < 11; i++) {
				f = new FileReader("formatted_data/data_dump_" + i + ".txt");
				b = new BufferedReader(f);
				size = 500000;
				System.out.print(String.format("\r%4d   %4dMB", i, sum/250000));

				for (int i2 = 0; i2 < size; i2++) {

					count++;
					int key = Integer.parseInt(b.readLine().trim(), 16);
					int line_length = Integer.parseInt(b.readLine().trim(), 16);
//					String s2 = b.readLine().trim();

					m2[key] = new int[line_length];
					sum += line_length;

					StringTokenizer st = new StringTokenizer(b.readLine()," ");
					int col = 0;
					while (st.hasMoreTokens())
					{
						//get next token and store it in the array
						m2[key][col] = Integer.parseInt(st.nextToken(), 16);
						col++;
					}
					Arrays.sort(m2[key]);
					/*
					s = new Scanner(s2);
					m2[key] = new int[line_length];
					sum += line_length;
					for (int j = 0; j < line_length; j++) {
						m2[key][j] = Integer.parseInt(s.next(), 16);
					}

					Arrays.sort(m2[key]);
					*/

					total_count++;
				}
				
				b.close();
				f.close();

			}
		} catch (NoSuchElementException e) {
			total_articles = count;
		} catch (IOException e) {
			System.out.println("\n\nAll files loaded.");
		} catch (Exception e) {
			System.out.println(e);
		}
		long t1 = System.nanoTime();
		double elapsed = (t1-t0)/1e9;
		System.out.println(String.format(
			"\n\nAll %d files loaded in %.2f seconds.\n", 
			total_articles, elapsed));
	}

	public static void main(String[] args) {
		new Process();
	}
}