from pyspark import SparkContext
import sys

# Create a SparkContext
sc = SparkContext(appName="MarvelAnalysis")

# Get the file names from the command-line arguments
graph_file = sys.argv[1]
names_file = sys.argv[2]

# Read the Marvel-Graph.txt file
graph_lines = sc.textFile(graph_file)

# Split the lines and extract the superhero IDs
hero_ids = graph_lines.map(lambda line: (int(line.split()[0]), len(line.split()) - 1))

# Calculate the total connections for each superhero
hero_connections_counts = hero_ids.reduceByKey(lambda x, y: x + y)

# Read the Marvel-Names.txt file and create a dictionary of superhero IDs and names
names = sc.textFile(names_file).map(lambda line: line.split(" ", 1)).map(lambda x: (int(x[0]), x[1])).collectAsMap()

# Join the superhero connections with their names
hero_connections_with_names = hero_connections_counts.map(lambda x: (x[0], names[x[0]], x[1]))

# Find the superhero with the maximum connections
most_popular_hero = hero_connections_with_names.max(key=lambda x: x[2])

# Print the superhero ID, name, and the number of connections
print("Most popular superhero ID:", most_popular_hero[0])
print("Name:", most_popular_hero[1])
print("Number of connections:", most_popular_hero[2])

# Stop the SparkContext
sc.stop()
