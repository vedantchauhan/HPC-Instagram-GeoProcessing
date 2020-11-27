
import mpi.*;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import java.net.InetAddress;

/**
 *
 * @author Vedant Chauhan 
 * Student id: 892758
 */
public class HPC_GeoProcessing {

	public static void main(String[] args) throws Exception {

		File inputFile = null;
		LineIterator it = null;
		int a1 = 0;
		int a2 = 0;
		int a3 = 0;
		int a4 = 0;
		int b1 = 0;
		int b2 = 0;
		int b3 = 0;
		int b4 = 0;
		int c1 = 0;
		int c2 = 0;
		int c3 = 0;
		int c4 = 0;
		int c5 = 0;
		int d3 = 0;
		int d4 = 0;
		int d5 = 0;
		// int []commArray = {a1,a2,a3,a4,b1,b2,b3,b4,c1,c2,c3,c4,c5,d3,d4,d5};
		// Object sendArray = commArray;
		// Grid coordinates A1 -> a11, a14 and so on.
		List<Double> a11 = new ArrayList<>();
		List<Double> a14 = new ArrayList<>();
		List<Double> a21 = new ArrayList<>();
		List<Double> a24 = new ArrayList<>();
		List<Double> a31 = new ArrayList<>();
		List<Double> a34 = new ArrayList<>();
		List<Double> a41 = new ArrayList<>();
		List<Double> a44 = new ArrayList<>();
		List<Double> b11 = new ArrayList<>();
		List<Double> b14 = new ArrayList<>();
		List<Double> b21 = new ArrayList<>();
		List<Double> b24 = new ArrayList<>();
		List<Double> b31 = new ArrayList<>();
		List<Double> b34 = new ArrayList<>();
		List<Double> b41 = new ArrayList<>();
		List<Double> b44 = new ArrayList<>();
		List<Double> c11 = new ArrayList<>();
		List<Double> c14 = new ArrayList<>();
		List<Double> c21 = new ArrayList<>();
		List<Double> c24 = new ArrayList<>();
		List<Double> c31 = new ArrayList<>();
		List<Double> c34 = new ArrayList<>();
		List<Double> c41 = new ArrayList<>();
		List<Double> c44 = new ArrayList<>();
		List<Double> c51 = new ArrayList<>();
		List<Double> c54 = new ArrayList<>();
		List<Double> d31 = new ArrayList<>();
		List<Double> d34 = new ArrayList<>();
		List<Double> d41 = new ArrayList<>();
		List<Double> d44 = new ArrayList<>();
		List<Double> d51 = new ArrayList<>();
		List<Double> d54 = new ArrayList<>();
		try {
			int rank, size;

			MPI.Init(args);
			double startTime = MPI.Wtime();

			size = MPI.COMM_WORLD.Size();
			rank = MPI.COMM_WORLD.Rank();

			// if (rank != 0) {
			// Reading grid values from melbGrid file
			JsonReader reader = new JsonReader(new FileReader("melbGrid.json"));
			reader.beginObject();

			while (reader.hasNext()) {

				Object obj = reader.nextName();
				if ("features".equals(obj)) {
					reader.beginArray();
					while (reader.hasNext()) {
						reader.beginObject();
						while (reader.hasNext()) {
							Object obj1 = reader.nextName();
							if ("properties".equals(obj1)) {
								reader.beginObject();
								while (reader.hasNext()) {
									Object obj2 = reader.nextName();
									String id = reader.nextString();
									if ("A1".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										a11.add(ymin);
										a11.add(xmin);
										a14.add(ymax);
										a14.add(xmax);
									} else if ("A2".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										a21.add(ymin);
										a21.add(xmin);
										a24.add(ymax);
										a24.add(xmax);
									} else if ("A3".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										a31.add(ymin);
										a31.add(xmin);
										a34.add(ymax);
										a34.add(xmax);
									} else if ("A4".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										a41.add(ymin);
										a41.add(xmin);
										a44.add(ymax);
										a44.add(xmax);
									} else if ("B1".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										b11.add(ymin);
										b11.add(xmin);
										b14.add(ymax);
										b14.add(xmax);
									} else if ("B2".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										b21.add(ymin);
										b21.add(xmin);
										b24.add(ymax);
										b24.add(xmax);
									} else if ("B3".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										b31.add(ymin);
										b31.add(xmin);
										b34.add(ymax);
										b34.add(xmax);
									} else if ("B4".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										b41.add(ymin);
										b41.add(xmin);
										b44.add(ymax);
										b44.add(xmax);
									} else if ("C1".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										c11.add(ymin);
										c11.add(xmin);
										c14.add(ymax);
										c14.add(xmax);
									} else if ("C2".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										c21.add(ymin);
										c21.add(xmin);
										c24.add(ymax);
										c24.add(xmax);
									} else if ("C3".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										c31.add(ymin);
										c31.add(xmin);
										c34.add(ymax);
										c34.add(xmax);
									} else if ("C4".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										c41.add(ymin);
										c41.add(xmin);
										c44.add(ymax);
										c44.add(xmax);
									} else if ("C5".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										c51.add(ymin);
										c51.add(xmin);
										c54.add(ymax);
										c54.add(xmax);
									} else if ("D3".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										d31.add(ymin);
										d31.add(xmin);
										d34.add(ymax);
										d34.add(xmax);
									} else if ("D4".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										d41.add(ymin);
										d41.add(xmin);
										d44.add(ymax);
										d44.add(xmax);
									} else if ("D5".equals(id)) {
										Object obj3 = reader.nextName();
										double xmin = reader.nextDouble();
										Object obj4 = reader.nextName();
										double xmax = reader.nextDouble();
										Object obj5 = reader.nextName();
										double ymin = reader.nextDouble();
										Object obj6 = reader.nextName();
										double ymax = reader.nextDouble();
										d51.add(ymin);
										d51.add(xmin);
										d54.add(ymax);
										d54.add(xmax);
									} else {
										reader.skipValue();
									}
								}
								reader.endObject();
							} else {
								reader.skipValue();
							}
						}
						reader.endObject();
					}
					reader.endArray();
				} else {
					reader.skipValue();
				}
			}
			reader.endObject();
			// grid read ends here

			// instagram data parsing

			inputFile = new File("bigInstagram.json");
			it = FileUtils.lineIterator(inputFile, "UTF-8");
			double y = 0.0;
			double x = 0.0;
			while (it.hasNext()) {
				String line = it.nextLine();
				if (line.contains("coordinates")) {
					String[] lineSplit = line.split("coordinates");
					if (lineSplit.length == 3) {
						String value = lineSplit[2];

						if (!value.equals("\":[null,null]}}},")) {
							String[] valueSplit = value.split("\\[");
							String value1 = valueSplit[1];
							String[] value1Split = value1.split(",");
							y = Double.parseDouble(value1Split[0]);

							String[] value2 = value1Split[1].split("\\]");
							x = Double.parseDouble(value2[0]);
						}
					}
					// Comparison of coordinates
					if ((y >= a11.get(0) && x >= a11.get(1)) && (y <= a14.get(0) && x <= a14.get(1))) {
						a1++;
					} else if ((y >= a21.get(0) && x >= a21.get(1)) && (y <= a24.get(0) && x <= a24.get(1))) {
						a2++;
					} else if ((y >= a31.get(0) && x >= a31.get(1)) && (y <= a34.get(0) && x <= a34.get(1))) {
						a3++;
					} else if ((y >= a41.get(0) && x >= a41.get(1)) && (y <= a44.get(0) && x <= a44.get(1))) {
						a4++;
					} else if ((y >= b11.get(0) && x >= b11.get(1)) && (y <= b14.get(0) && x <= b14.get(1))) {
						b1++;
					} else if ((y >= b21.get(0) && x >= b21.get(1)) && (y <= b24.get(0) && x <= b24.get(1))) {
						b2++;
					} else if ((y >= b31.get(0) && x >= b31.get(1)) && (y <= b34.get(0) && x <= b34.get(1))) {
						b3++;
					} else if ((y >= b41.get(0) && x >= b41.get(1)) && (y <= b44.get(0) && x <= b44.get(1))) {
						b4++;
					} else if ((y >= c11.get(0) && x >= c11.get(1)) && (y <= c14.get(0) && x <= c14.get(1))) {
						c1++;
					} else if ((y >= c21.get(0) && x >= c21.get(1)) && (y <= c24.get(0) && x <= c24.get(1))) {
						c2++;
					} else if ((y >= c31.get(0) && x >= c31.get(1)) && (y <= c34.get(0) && x <= c34.get(1))) {
						c3++;
					} else if ((y >= c41.get(0) && x >= c41.get(1)) && (y <= c44.get(0) && x <= c44.get(1))) {
						c4++;
					} else if ((y >= c51.get(0) && x >= c51.get(1)) && (y <= c54.get(0) && x <= c54.get(1))) {
						c5++;
					} else if ((y >= d31.get(0) && x >= d31.get(1)) && (y <= d34.get(0) && x <= d34.get(1))) {
						d3++;
					} else if ((y >= d41.get(0) && x >= d41.get(1)) && (y <= d44.get(0) && x <= d44.get(1))) {
						d4++;
					} else if ((y >= d51.get(0) && x >= d51.get(1)) && (y <= d54.get(0) && x <= d54.get(1))) {
						d5++;
					}
				}
			}
			// }
			System.out.println("Hostname: " + InetAddress.getLocalHost().getHostName());

			// Order of posts
			// Grid Wise
			// Object recArray = new int[16];
			if (rank == 0) {
				// MPI.COMM_WORLD.Gatherv(sendArray, 0, 16, MPI.INT, recArray, 0, commArray,
				// commArray, MPI.INT, 0);
				// System.out.println(recArray);

				Map<String, Integer> sortedMap = new TreeMap<String, Integer>(Collections.reverseOrder());
				sortedMap.put("A1", a1);
				sortedMap.put("A2", a2);
				sortedMap.put("A3", a3);
				sortedMap.put("A4", a4);
				sortedMap.put("B1", b1);
				sortedMap.put("B2", b2);
				sortedMap.put("B3", b3);
				sortedMap.put("B4", b4);
				sortedMap.put("C1", c1);
				sortedMap.put("C2", c2);
				sortedMap.put("C3", c3);
				sortedMap.put("C4", c4);
				sortedMap.put("C5", c5);
				sortedMap.put("D3", d3);
				sortedMap.put("D4", d4);
				sortedMap.put("D5", d5);

				Map<String, Integer> map = sortByValues(sortedMap);
				Object key = null;
				int count = 0;

				System.out.println("-----Grid Ordering-----");

				ArrayList<String> keys = new ArrayList<String>(map.keySet());
				for (int i = keys.size() - 1; i >= 0; i--) {
					key = keys.get(i);
					count = sortedMap.get(key);
					System.out.println(key + ": " + count);
				}

				// Row wise
				int a = a1 + a2 + a3 + a4;
				int b = b1 + b2 + b3 + b4;
				int c = c1 + c2 + c3 + c4 + c5;
				int d = d3 + d4 + d5;

				sortedMap = new TreeMap<String, Integer>(Collections.reverseOrder());
				sortedMap.put("A-Row", a);
				sortedMap.put("B-Row", b);
				sortedMap.put("C-Row", c);
				sortedMap.put("D-Row", d);

				map = sortByValues(sortedMap);

				System.out.println("-----Row Ordering-----");

				keys = new ArrayList<String>(map.keySet());
				for (int i = keys.size() - 1; i >= 0; i--) {
					key = keys.get(i);
					count = sortedMap.get(key);
					System.out.println(key + ": " + count);
				}

				// Column Wise
				int col1 = a1 + b1 + c1;
				int col2 = a2 + b2 + c2;
				int col3 = a3 + b3 + c3 + d3;
				int col4 = a4 + b4 + c4 + d4;
				int col5 = c5 + d5;

				sortedMap = new TreeMap<String, Integer>(Collections.reverseOrder());
				sortedMap.put("Column 1", col1);
				sortedMap.put("Column 2", col2);
				sortedMap.put("Column 3", col3);
				sortedMap.put("Column 4", col4);
				sortedMap.put("Column 5", col5);

				map = sortByValues(sortedMap);

				System.out.println("-----Column Ordering-----");

				keys = new ArrayList<String>(map.keySet());
				for (int i = keys.size() - 1; i >= 0; i--) {
					key = keys.get(i);
					count = sortedMap.get(key);
					System.out.println(key + ": " + count);
				}

				// MPI code for wall time
				double stopTime = MPI.Wtime();
				System.out.println("Time usage = " + (stopTime - startTime) + " s");
			}

			MPI.Finalize();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Code to compare values of Treemap for ordering
	 * 
	 * @param <K>
	 * @param <V>
	 * @param map
	 * @return
	 */
	public static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {

		Comparator<K> valueComparator = new Comparator<K>() {
			public int compare(K k1, K k2) {
				int compare = map.get(k1).compareTo(map.get(k2));
				if (compare == 0) {
					return 1;
				} else {
					return compare;
				}
			}
		};

		Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
		sortedByValues.putAll(map);
		return sortedByValues;
	}
}
