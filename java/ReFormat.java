import java.io.*;
import java.util.*;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ReFormat {

	private int[][] links;
	private int[][] backLinks;
	private int max_number = 0;
	private int numLines;
	private long allCount = 0;
	private long allBackCount = 0;

	private ReFormat() {
	}

	public void saveFormatted() {
		int count = 0;
		int files = 0;
		StringBuilder s = new StringBuilder();
		boolean first = true;
		int numFiles = new File("data").list().length;
		for (int i = 0; i < numFiles; i++) {
			try {
				FileReader f = new FileReader("data/data_dump_" + i + ".txt");
				BufferedReader b = new BufferedReader(f);
				String line;
				while ((line = b.readLine()) != null) {
					if (first) {
						first = false;
					} else {
						s.append("\n");
					}
					s.append(line);
				}

				if (++count % 1000 == 0) {
					System.out.println("Saving file: " + files);
					PrintWriter p = new PrintWriter(new File("formatted_data/data_dump_" + files + ".txt"));
					files++;
					p.println(s);
					p.flush();
					p.close();
					s = new StringBuilder();
					first = true;

				}
			} catch (Exception e) {
				System.out.println("No file for: " + i);
			}
		}
		try {
			System.out.println("Final saving.");
			PrintWriter p = new PrintWriter(new File("formatted_data/data_dump_" + files + ".txt"));
			files++;
			p.println(s);
			p.flush();
			p.close();
			s = new StringBuilder();
		} catch (Exception e) {}		
	}

	public void saveMemoryMap() {
		int numFiles = new File("data").list().length;
		int totalArticles = 0;
		try(RandomAccessFile file = new RandomAccessFile("memory_map.dat", "rw")) {
			MappedByteBuffer out = file.getChannel()
				.map(FileChannel.MapMode.READ_WRITE, 0, 2_000_000_000);
				out.putInt(0);
				out.putInt(0);
			for (int i = 0; i < numFiles; i++) {
				System.out.print(String.format("\r%d / %d", i, numFiles));
				try {
					FileReader f = new FileReader("data/data_dump_" + i + ".txt");
					BufferedReader b = new BufferedReader(f);
					String line;
					int l = 0;
					while ((line = b.readLine()) != null) {
						totalArticles++;
						int article = Integer.parseInt(line, 16);
						if (article == 61199280) {
							System.out.println("FOUND THE ERROR");
						}
						max_number = Math.max(article, max_number);
						line = b.readLine();
						int numLinks = Integer.parseInt(line, 16);
						out.putInt(article);
						out.putInt(numLinks);

						line = b.readLine();
						if (numLinks > 0) {
							int[] links = Arrays.stream(line.split(" "))
								.mapToInt(num -> Integer.parseInt(num, 16)).toArray();
							// System.out.println(String.format("Article: %d -> %d / %d", article, numLinks, links.length));
							for (int j = 0; j < numLinks; j++) {
								out.putInt(links[j]);
							}
						}

					}
				} catch (Exception e) {
					System.out.println("Error: " + e);
				}
			}
			out.putInt(0, totalArticles);
			out.putInt(4, max_number);
			System.out.println("\nmax_number: " + max_number);
		} catch (FileNotFoundException e) {

		} catch (IOException e) {

		}
	}

	private void removeExcess() {
		int removed = 0;
		int total = 0;
		allCount = 2;
		for (int i = 0; i < links.length; i++) {
			if (links[i] != null) {
				int count = 0;
				total += links[i].length;
				for (int j : links[i]) {
					if (j < links.length && links[j] != null) {
						count++;
					} else {
						removed++;
					}
				}
				int[] temp = new int[count];
				count = 0;
				allCount += 2;
				for (int j : links[i]) {
					if (j < links.length && links[j] != null) {
						temp[count++] = j;
						allCount++;
					}
				}
				links[i] = temp;
			}
		}
	}

	private void readFiles3() {
		try(RandomAccessFile file = new RandomAccessFile("memory_map.dat", "rw")) {
			MappedByteBuffer out = file.getChannel()
				.map(FileChannel.MapMode.READ_ONLY, 0, 2_000_000_000);
			numLines = out.getInt();
			max_number = out.getInt();
			System.out.println("max_number: " + max_number);
			links = new int[max_number+1][];
			backLinks = new int[max_number+1][];

			for (int i = 0; i < numLines; i++) {
				int article = out.getInt();
				int numLinks = out.getInt();
				links[article] = new int[numLinks];
				for (int j = 0; j < numLinks; j++) {
					links[article][j] = out.getInt();
				}
			}
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}
	}

	private void generateBackLinks() {

		int[] backLengths = new int[max_number+1];
		for (int i = 0; i < links.length; i++) {
			if (i % 1000 == 0) {
				System.out.print("\ri: " + i);
			}
			if (links[i] != null) {			
				for (int j : links[i]) {
					backLengths[j]++;
				}
			}
		}
		for (int i = 0; i < links.length; i++) {
			if (backLengths[i] > 0) {
				backLinks[i] = new int[backLengths[i]];
			}
		}
		System.out.println();

		allBackCount = 2;
		int[] backIndices = new int[max_number+1];
		for (int i = 0; i < links.length; i++) {
			if (i % 1000 == 0) {
				System.out.print("\ri: " + i);
			}
			if (links[i] != null) {			
				allBackCount += 2;
				for (int j : links[i]) {
					backLinks[j][backIndices[j]++] = i;
					allBackCount++;
				}
			}
		}
		System.out.println();

	}

	private void saveFile() {

		System.out.println("allCount: " + 4*allCount);
		System.out.println("allBackCount: " + 4*allBackCount);
		try(RandomAccessFile file = new RandomAccessFile("memory_map_formatted.dat", "rw")) {

			MappedByteBuffer out = file.getChannel()
				.map(FileChannel.MapMode.READ_WRITE, 0, 4*allCount);
			System.out.println("allCount: " + 4*allCount);
			out.putInt(numLines);
			out.putInt(max_number);
			for (int i = 0; i < max_number; i++) {
				if (links[i] != null) {
					out.putInt(i);
					out.putInt(links[i].length);
					for (int j = 0; j < links[i].length; j++) {
						out.putInt(links[i][j]);
					}
				}
			}
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}

		try(RandomAccessFile file = new RandomAccessFile("memory_map_backlinks.dat", "rw")) {
			MappedByteBuffer out = file.getChannel()
				.map(FileChannel.MapMode.READ_WRITE, 0, 4*allBackCount);
			System.out.println("allBackCount: " + 4*allBackCount);
			out.putInt(numLines);
			out.putInt(max_number);
			for (int i = 0; i < max_number; i++) {
				if (backLinks[i] != null) {
					out.putInt(i);
					out.putInt(backLinks[i].length);
					for (int j = 0; j < backLinks[i].length; j++) {
						out.putInt(backLinks[i][j]);
					}
				}
			}




		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}

	}
	
	public static void main(String[] args) {
		// new ReFormat().saveFormatted();
		ReFormat f = new ReFormat();
		f.saveMemoryMap();
		f.readFiles3();
		f.removeExcess();
		f.generateBackLinks();
		f.saveFile();

	}
}