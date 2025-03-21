from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("agents")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_agents="dataset.csv"
    # Leemos el archivo CSV de agentes
    df_agents = spark.read.csv(path_agents, header=True, inferSchema=True)

    df_agents.createOrReplaceTempView("agents")

    # Mostramos la estructura de la tabla para verificar las columnas
    query = 'DESCRIBE agents'
    spark.sql(query).show(30)

    # Consulta para mostrar todos los agentes que tienen un atributo "Electric"
    query = """
    SELECT agent, attribute, specialty, type, faction, rarity, hp, def, atk, crit_rate, crit_dmg, impact, atr_mastery, energy
    FROM agents
    WHERE attribute = 'Electric'
    ORDER BY atk DESC
    """

    df_agents_electric = spark.sql(query)
    df_agents_electric.show(20)
    results = df_agents_electric.toJSON().collect()

    # Otra consulta, Agentes con atributo "Fire"
    query = """
    SELECT agent, attribute, specialty, type, faction, rarity, hp, def, atk, crit_rate, crit_dmg, impact, atr_mastery, energy
    FROM agents
    WHERE attribute = 'Fire'
    ORDER BY atk DESC
    """

    df_agents_fire = spark.sql(query)
    df_agents_fire.show(20)

    # Guardamos los resultados en JSON (Electric en este caso)
    df_agents_electric.write.mode("overwrite").json("results/electric_agents")

    # Si quieres guardar los de Fire:
    df_agents_fire.write.mode("overwrite").json("results/fire_agents")

    #print(results)
    df_agents_electric.write.mode("overwrite").json("results")
    #df_people_1903_1906.coalesce(1).write.json('results/data_merged.json')
    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    spark.stop()
