from pyspark.sql import SparkSession

# Kreiraj SparkSession
spark = SparkSession.builder \
    .appName("Minimal Spark Example") \
    .getOrCreate()

# Dummy podaci kao lista Python dict-ova
data = [
    {"ime": "Antonio", "godine": 21},
    {"ime": "Flavio", "godine": 35},
    {"ime": "Marija", "godine": 29}
]

# Kreiranje DataFrame-a
df = spark.createDataFrame(data)

# Prikaz podataka
df.show()

# Neka osnovna operacija
df.filter(df.godine > 25).show()

# Zaustavi SparkSession
spark.stop()
