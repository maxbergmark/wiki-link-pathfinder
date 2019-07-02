import java.util.*;
import java.io.*;

//from .jar file
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

//for fetching URLs
import java.net.URLEncoder;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;

public class FetchThread implements Runnable {


	int process_id;
	ArrayList<Integer> pages;
	HashMap<Integer, ArrayList<Integer>> map;

	public FetchThread(int process_id, ArrayList<Integer> pages) {
		this.process_id = process_id;
		this.pages = pages;
	}


	public void run() {


		GetArticles.mainPrint("Starting thread " + process_id + ".");
		map = new HashMap<>();

		for (int page : pages) {

			ArrayList<Integer> ids = new ArrayList<>();
			String cont = null;
			while (true) {

				String pageURL = GetArticles.getContinued(page, cont);
				Map o = (Map) GetArticles.getJson(pageURL);
				if (o.containsKey("query")) {
					Map temp = (Map) o.get("query");
					temp = (Map) temp.get("pages");
					for (Object key : temp.keySet()) {
						int parsed = Integer.parseInt((String) key);
						if (parsed > 0) {
							ids.add(parsed);
						}
					}
				}
				if (!o.containsKey("continue")) {
					break;
				}
				Map temp = (Map) o.get("continue");
				cont = (String) temp.get("gplcontinue");
			}
			map.put(page, ids);
		}
		saveData();
	}



	private void saveData() {
		GetArticles.mainPrint("Beginning save of thread " + process_id);
		String fileName = "new_data/data_dump_" + process_id + ".txt";
		StringBuilder s = new StringBuilder();
		for (int key : map.keySet()) {
			s.append(String.format("%X\n", key));
			s.append(String.format("%X\n", map.get(key).size()));
			for (int j : map.get(key)) {
				s.append(String.format("%X ", j));
			}
			s.append("\n");
		}
		try {
			PrintWriter p = new PrintWriter(new File(fileName));
			p.print(s);
			p.flush();
			p.close();
			GetArticles.mainPrint("Thread " + process_id + " completed.");
			GetArticles.addComplete();
		} catch (Exception e) {
			System.out.println("Save error.");
		}
	}

}