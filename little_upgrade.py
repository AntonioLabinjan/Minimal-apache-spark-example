from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, count
import random
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("Spark Power + Charts").getOrCreate()

def generiraj_korisnike(n):
    imena = ["Antonio", "Flavio", "Marija", "Petra", "Luka", "Ivana", "Ana", "Karlo"]
    gradovi = ["Zagreb", "Split", "Rijeka", "Osijek", "Pula", "Zadar", "Varaždin"]
    return [
        {
            "ime": random.choice(imena),
            "godine": random.randint(18, 80),
            "grad": random.choice(gradovi),
            "bodovi": random.randint(0, 1000)
        }
        for _ in range(n)
    ]

korisnici_df = spark.createDataFrame(generiraj_korisnike(1_000_000))

statistika_po_gradu = korisnici_df.groupBy("grad").agg(
    avg("godine").alias("prosjek_godina"),
    avg("bodovi").alias("prosjek_bodova"),
    max("bodovi").alias("max_bodova"),
    count("*").alias("broj_korisnika")
).orderBy("prosjek_bodova", ascending=False)

print("Statistika po gradovima:")
statistika_po_gradu.show()

print("Najaktivniji korisnici (bodovi > 900):")
korisnici_df.filter(col("bodovi") > 900).show(10)

print("Grad s najviše korisnika:")
statistika_po_gradu.orderBy("broj_korisnika", ascending=False).limit(1).show()

pdf = statistika_po_gradu.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(pdf["grad"], pdf["prosjek_bodova"], color='skyblue', edgecolor='black')
plt.title("📈 Prosječni bodovi po gradu")
plt.xlabel("Grad")
plt.ylabel("Prosječni bodovi")
plt.grid(axis='y')
plt.tight_layout()
plt.show()

plt.figure(figsize=(8, 8))
plt.pie(pdf["broj_korisnika"], labels=pdf["grad"], autopct="%1.1f%%", startangle=140)
plt.title("🍰 Udio korisnika po gradu")
plt.axis("equal")
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
plt.plot(pdf["grad"], pdf["max_bodova"], marker='o', linestyle='-', color='green')
plt.title("🔝 Max bodovi po gradu")
plt.xlabel("Grad")
plt.ylabel("Max bodovi")
plt.grid(True)
plt.tight_layout()
plt.show()

spark.stop()
