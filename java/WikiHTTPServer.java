
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.util.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.*;

public class WikiHTTPServer {

	private HttpServer server;
	private WikiSearcher wikiSearcher;
	private WikiAPIHandler apiHandler;
	private LinkedBlockingQueue<WikiPair> submitted;

	private Map<String, String> queryToMap(String query) {
		Map<String, String> result = new HashMap<>();
		for (String param : query.split("&")) {
			String[] entry = param.split("=");
			if (entry.length > 1) {
				result.put(entry[0], entry[1]);
			} else {
				result.put(entry[0], "");
			}
		}
		return result;
	}

	public WikiHTTPServer() {
		wikiSearcher = new WikiSearcher();
		apiHandler = new WikiAPIHandler();
		submitted = new LinkedBlockingQueue<WikiPair>();
		new Thread(() -> {
			checkRequestQueue();
		}).start();
		try {
			server = HttpServer.create(new InetSocketAddress(8000), 0);
			HttpContext context = server.createContext("/get-all-paths");
			context.setHandler((he) -> {getAllPaths(he);});
			server.start();
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	private boolean fileExists(String fileName) {
		File f = new File(fileName);
		return f.exists() && !f.isDirectory();
	}

	private void articleNotFound(HttpExchange exchange) throws IOException {
		OutputStream os = exchange.getResponseBody();
		String response = "{\"message\":\"article_not_found\",\"code\":400}";
		byte[] bytes = response.toString().getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(400, bytes.length);
		os.write(bytes);
		os.close();
	}

	private void returnFile(HttpExchange exchange, String fileName) throws IOException {
		File file = new File(fileName);
		byte[] bytes = Files.readAllBytes(file.toPath());

		OutputStream os = exchange.getResponseBody();
		// byte[] bytes = response.toString().getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(400, bytes.length);
		os.write(bytes);
		os.close();		
	}

	private void writeTempFile(String fileName) {
		// File f = new File(fileName);
		// String fileName = String.format("results/%d_%d.json", start, end);
		String s = "{\"message\":\"awaiting_results\",\"code\":200}";
		byte data[] = s.toString().getBytes(StandardCharsets.UTF_8);
		try {
			FileOutputStream out = new FileOutputStream(fileName);
			out.write(data);
			out.close();
		} catch (IOException e) {
			System.out.println("File write error: " + e.getMessage());
		}
	}

	private void checkRequestQueue() {
		while (true) {
			if (!submitted.isEmpty()) {
				WikiPair p = submitted.poll();
				getPathsThreaded(p.start.id, p.end.id);				
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				System.out.println("Interrupted");
			}
		}
	}

	private void getPathsThreaded(int start, int end) {
		List<List<WikiArticle>> paths = wikiSearcher.findAllPathsById(start, end);
		StringBuilder s = new StringBuilder();
		StringJoiner ret = new StringJoiner(", ", "[", "]");
		for (List<WikiArticle> path : paths) {
			StringJoiner p = new StringJoiner(", ", "[", "]");
			for (WikiArticle i : path) {
				p.add(i.toString());
			}
			ret.add(p.toString());
		}

		s.append("{\"num_paths\":" + paths.size());
		s.append(",\"min_clicks\":" + (paths.get(0).size() - 1) + ",\"paths\":");
		s.append(ret.toString());
		s.append("}");
		String fileName = String.format("results/%d_%d.json", start, end);
		byte data[] = s.toString().getBytes(StandardCharsets.UTF_8);
		try {
			FileOutputStream out = new FileOutputStream(fileName);
			out.write(data);
			out.close();
		} catch (IOException e) {
			System.out.println("File write error: " + e.getMessage());
		}
	}

	private void getAllPaths(HttpExchange exchange) throws IOException {
		OutputStream os = exchange.getResponseBody();
		Map<String, String> params = queryToMap(
			exchange.getRequestURI().getQuery()); 

		WikiPair articlePair;
		WikiArticle startArticle, endArticle;
		int startId, endId;
		String startName, endName;
		startName = params.get("start");
		endName = params.get("end");
		try {
			startId = apiHandler.getId(startName);
			endId = apiHandler.getId(endName);
		} catch (Exception e) {
			articleNotFound(exchange);
			return;
		}
		startArticle = new WikiArticle(startId, startName);
		endArticle = new WikiArticle(endId, endName);
		articlePair = new WikiPair(startArticle, endArticle);
		String tempName = String.format("results/%d_%d.json", startId, endId);
		if (!fileExists(tempName)) {
			writeTempFile(tempName);
			try {
				submitted.put(articlePair);
			} catch (InterruptedException e) {
				System.out.println("Interrupted");
			}
		}

		exchange.getResponseHeaders().set(
			"Content-Type", "application/json; charset=utf-8");
		returnFile(exchange, tempName); 
		// byte[] bytes = s.toString().getBytes(StandardCharsets.UTF_8);
		// exchange.sendResponseHeaders(200, bytes.length);
		// os.write(bytes);
		// os.close();

	}
	

	public static void main(String[] args) {
		new WikiHTTPServer();
	}
}