//regular imports
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


//from .jar file
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

//for fetching URLs
import java.net.URLEncoder;
import java.net.URL;


public class GetArticles {

	static GetArticles curr;
	static Object printLock = new Object();

	ArrayList<Thread> threadList;
	static int count;
	static int complete = 0;
	static long startTime;



	public static String formatURL(String cont, String apcont) {
		try {
			String cF = URLEncoder.encode(cont, "UTF-8");
			String apF = URLEncoder.encode(apcont, "UTF-8");
			return "https://en.wikipedia.org/w/api.php?"
			 + "action=query&format=json&prop=&list=allpages&continue="
			 + cF + "&apcontinue=" + apF
			  + "&apfilterredir=nonredirects&aplimit=max";
		} catch (Exception e) {
			return null;
		}
	}

	public static String getContinued(int id, String cont) {
		if (cont == null) {
			return "https://en.wikipedia.org/w/api.php?action=query&pageids=" 
			+ id + "&generator=links&format=json&gpllimit=max&redirects=1&gplnamespace=0";
		}
		try {
			return "https://en.wikipedia.org/w/api.php?action=query&pageids=" 
			+ id + "&generator=links&format=json&gplcontinue="
			+ URLEncoder.encode(cont, "UTF-8") + "&gpllimit=max&redirects=1&gplnamespace=0";
		} catch (Exception e) {}
		return null;
	}

	private static String getIdUrl(String title) {
		try {
			return "https://en.wikipedia.org/w/api.php"
			+ "?format=json&action=query&titles="
			+ URLEncoder.encode(title, "UTF-8") + "&redirects";
		} catch (Exception e) {}
		return null;
	}


	public static JSONObject getJson(String urlString) {
		
		try {
			URL url = new URL(urlString);
			String total = "";
			boolean error = false;
			int errors = 0;
			while (total.equals("")) {
				total = "";
				try {
					BufferedReader in = new BufferedReader(
						new InputStreamReader(url.openStream()));

					String inputLine = in.readLine();
					total += inputLine;
					while ((inputLine = in.readLine()) != null) {
						total += inputLine;
					}
					in.close();
				} catch (Exception e) {
					error = true;
					errors++;
					// System.out.println("\nNo response.");
					try {
						Thread.sleep(100 * (int)Math.pow(2, Math.max(7, errors)));
					} catch (Exception e2) {}
					//System.out.println(e);
				}
			}
			if (error) {
				// System.out.println("\nError resolved.");
			}

			JSONParser parser = new JSONParser();
			Object obj = parser.parse(total);
			JSONObject o = (JSONObject) obj;
			return o;
		} catch (Exception e) {
			System.out.println("Json fetch error.");
			System.out.println(e);
		}
		System.out.println("\n\nFatal error!\n\n");
		return null;

	}

	public void getAllArticles() {

		count = 0;
		String cont = "";
		String apcont = "";
		ExecutorService executor = Executors.newFixedThreadPool(20);


		while (true) {
		// for (int i2 = 0; i2 < 7; i2++) {
			String url = formatURL(cont,apcont);
			Map data = (Map) getJson(url);
			Map temp = (Map) data.get("query");
			JSONArray temp2 = (JSONArray) temp.get("allpages");
			ArrayList<Integer> pages = new ArrayList<>(500);
			for (Object o : temp2) {
				Map i = (Map) o;
				int parsed = (int) ((long) i.get("pageid"));
				if (parsed > 0) {
					pages.add(parsed);
				}
			}
			Runnable t = new FetchThread(count++, pages);
			executor.execute(t);

			if (!data.containsKey("continue")) {
				break;
			}
			temp = (Map) data.get("continue");
			cont = (String) temp.get("continue");
			apcont = (String) temp.get("apcontinue");

		}

		executor.shutdown();

		while (!executor.isTerminated()) {

		}
		System.out.println("Finished all threads");

	}

	public static void addComplete() {
		synchronized(curr) {
			complete++;
		}
	}

	public static void mainPrint(String info) {
		synchronized(printLock) {
			double elapsed = (System.nanoTime()-startTime)/1e9;
			int tempProcessed = Math.max(count, 10000);
			double estimate = 0.0;
			if (complete > 0){
				estimate = elapsed * (tempProcessed - complete) / (double) complete;
			}
			String s = String.format(
				"\r(%5d/%5d) (%7.1fs) (%7.1fs) %s", 
				complete, tempProcessed, elapsed, estimate, info);
			if (s.length() >= 80) {
				s = s.substring(0,80);
			}
			System.out.print(s);
			for (int i = 0; i < 80-s.length(); i++) {
				System.out.print(" ");
			}
		}
	}

	public GetArticles() {
		threadList = new ArrayList<>();
		curr = this;
		System.out.println("\nCompleted     Elapsed    Estimate   Message");
		startTime = System.nanoTime();
		getAllArticles();
	}


	public static void main(String[] args) {
		new GetArticles();
	}
}