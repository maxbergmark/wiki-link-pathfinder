import java.io.*;
import java.util.*;

public class ReFormat {
	
	public static void main(String[] args) {
		int count = 0;
		int files = 0;
		StringBuilder s = new StringBuilder();
		boolean first = true;
		for (int i = 0; i < 11000; i++) {
			try {
				FileReader f = new FileReader("new_data/data_dump_" + i + ".txt");
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
}