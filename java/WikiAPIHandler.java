import java.util.*;
import java.io.*;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import java.net.URLEncoder;
import java.net.URL;


public class WikiAPIHandler {

	public JSONObject getJson(String urlString) {		
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

	public int getId(String title) throws Exception {
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

	private String getIdUrl(String title) {
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

	public Map<Integer, String> getAllTitles(Set<Integer> ids) {
		Map<Integer, String> ret = new HashMap<>();
		for (String url : getMultipleUrl(ids)) {
			JSONObject o = getJson(url);
			o = (JSONObject) o.get("query");
			o = (JSONObject) o.get("pages");
			for (int i : ids) {
				JSONObject o2 = (JSONObject) o.get("" + i);
				if (o2 != null) {
					String s = (String) o2.get("title");
					// System.out.println("Found title: " + i + "\t" + s);
					ret.put(i, s);
				}
				// ret.add(s);
			}
		}
		return ret;

	}

	/**
	* Fetches the title of a Wikipedia article given its article ID.
	* @param id The article ID to be checked.
	* @return The title of the article with said article ID.
	*/	

	public String getTitle(int id) {
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

	private String getUrl(int id) {
		return "https://en.wikipedia.org/w/api.php?"
		+ "format=json&action=query&pageids=" + id;
	}

	/**
	* Composes the API call for fetching multiple 
	* Wikipedia articles with a single API call.
	* @param ids An ArrayList containing all article IDs to be checked.
	* @return A URL String to be used for the API call.
	*/

	private List<String> getMultipleUrl(Set<Integer> ids) {
		List<String> ret = new ArrayList<>();
		String s0 = "https://en.wikipedia.org/w/api.php?"
		+ "format=json&action=query&pageids=";
		int c = 0;
		String s = s0;
		for (int i : ids) {
		// for (int i = 0; i < ids.size()-1; i++) {
			s += i;
			c++;
			if (c % 50 == 0) {
				// System.out.println(ids.size() + "\t" + s);
				ret.add(s);
				s = s0;
				continue;
			}
			if (c < ids.size()) {
				s += "%7C";
			}
		}
		// System.out.println(ids.size() + "\t" + s);
		ret.add(s);
		return ret;
	}

}