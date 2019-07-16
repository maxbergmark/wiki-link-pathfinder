import java.io.*;
import java.util.*;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ReFormat {

	private int[][] m2;
	private int max_number = 73000000;
	private int numLines;

	private ReFormat() {
		m2 = new int[max_number][];
	}

	public void SaveFormatted() {
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
		} catch (FileNotFoundException e) {

		} catch (IOException e) {

		}
	}

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
	}

	private void readFiles3() {
		try(RandomAccessFile file = new RandomAccessFile("memory_map.dat", "rw")) {
			MappedByteBuffer out = file.getChannel()
				.map(FileChannel.MapMode.READ_ONLY, 0, 2_000_000_000);
			numLines = out.getInt();
			for (int i = 0; i < numLines; i++) {
				int article = out.getInt();
				int numLinks = out.getInt();
				m2[article] = new int[numLinks];
				for (int j = 0; j < numLinks; j++) {
					m2[article][j] = out.getInt();
				}
			}
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}
	}

	private void saveFile() {

		try(RandomAccessFile file = new RandomAccessFile("memory_map_formatted.dat", "rw")) {
			MappedByteBuffer out = file.getChannel()
				.map(FileChannel.MapMode.READ_WRITE, 0, 2_000_000_000);
			
			out.putInt(numLines);
			for (int i = 0; i < max_number; i++) {
				if (m2[i] != null) {
					out.putInt(i);
					out.putInt(m2[i].length);
					for (int j = 0; j < m2[i].length; j++) {
						out.putInt(m2[i][j]);
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
		f.saveFile();

	}
}