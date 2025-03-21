from pyspark.sql import SparkSession
import json
import os

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("agents")\
        .getOrCreate()

    print("Leyendo dataset.csv ... ")
    path_agents = "dataset.csv"
    df_agents = spark.read.csv(path_agents, header=True, inferSchema=True)

    df_agents.createOrReplaceTempView("agents")

    spark.sql('DESCRIBE agents').show()

    query_attributes = "SELECT DISTINCT attribute FROM agents"
    attributes = [row.attribute for row in spark.sql(query_attributes).collect()]
    
    print(f"Atributos encontrados: {attributes}")

    results = {}

    for attr in attributes:
        query = f"""
        SELECT agent, attribute, specialty, type, faction, rarity, hp, def, atk, crit_rate, crit_dmg, impact, atr_mastery, energy
        FROM agents
        WHERE attribute = '{attr}'
        ORDER BY atk DESC
        """
        
        df_attr = spark.sql(query)
        
        print(f"Agentes con atributo {attr}:")
        df_attr.show(5)

        results[attr] = df_attr.toPandas().to_dict(orient="records")

    os.makedirs('results', exist_ok=True)

    with open('results/data.json', 'w') as file:
        json.dump(results, file, indent=4)

    print("Archivo JSON generado en 'results/data.json'.")

    spark.stop()
