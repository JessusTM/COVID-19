from pymongo import MongoClient
from neo4j import GraphDatabase


client = MongoClient("mongodb://localhost:27017/")
db = client["covid19"]
collection = db["case-death"]

neo4jDriver = GraphDatabase.driver(
    "bolt://localhost:7687", auth=("neo4j", "12345678"))

session = neo4jDriver.session()


##########################
#   Metodos de MongoDB   #
##########################

except_location =  ["World", "Upper-middle-income countries", "South America", "Lower-middle-income countries", "North America", "Lower-middle-income countries", "High-income countries", "European Union (27)", "Europe", "Asia", "Africa", ]

#Funciones de mongodb que busca los paises con mas de cierto total de muertes registradas
def GetCountriesWithTotalDeathsAbove(deaths):
    return list(collection.find({"new_deaths": {"$gt": deaths}, "location": {"$nin": except_location}}))

#Funciones de mongodb que busca los paises con mas de cierto total de casos registrados
def GetCountriesWithTotalCasesAbove(cases):
    return list(collection.find({"new_cases": {"$gt": cases}, "location": {"$nin": ["World", "Upper-middle-income countries", "South America"]}}))

#Funciones de mongodb que retorna el total de casos y muertes de un pais
def GetCovidData(countryName):
    total_cases = 0
    total_deaths = 0
    for countryData in collection.find({"location": countryName}):
        total_cases += countryData.get("new_cases", 0)
        total_deaths += countryData.get("new_deaths", 0)
    return {"cases": total_cases, "deaths": total_deaths} if total_cases >= 0 else None


########################
#   Metodos de Neo4j   #
########################

#Funciones de neo4j que retorna la clasificacion de ingresos de un pais
def GetIncomeClassification(tx, countryName):
    query = """
    MATCH (c:Country {name: $countryName})-[:HAS_INCOME_CLASSIFICATION]->(i:IncomeClassification)
    RETURN i.level AS income_level
    """
    result = tx.run(query, countryName=countryName)
    record = result.single()
    if record:
        return record["income_level"]
    return None

#Funciones de neo4j que retorna el porcentaje de turismo de un pais
def GetTourismData(tx, countryName):
    query = """
    MATCH (c:Country {name: $countryName})-[:DEPENDS_ON_TOURISM]->(t:Tourism)
    RETURN t.percentage AS tourism_level
    """
    result = tx.run(query, countryName=countryName)
    record = result.single()
    if record:
        return record["tourism_level"]
    return None

#Funciones de neo4j que retorna los paises con cierta clasificacion de ingresos
def GetAllCountriesWhitIncomeClassification(tx, incomeClassification):
    query = """
    MATCH (c:Country)-[:HAS_INCOME_CLASSIFICATION]->(i:IncomeClassification {level: $incomeClassification})
    RETURN c.name AS country
    """
    result = tx.run(query, incomeClassification=incomeClassification)
    return [record["country"] for record in result]

#Funciones de neo4j que retorna los paises con porcentaje de turismo similar al pais ingresado
def GetSimilarTourismCountries(tx, countryName, tolerance=10):
    countryName = countryName

    query = """
    MATCH (c:Country {name: $countryName})-[:DEPENDS_ON_TOURISM]->(t:Tourism)
    WITH t.percentage AS tourism_level
    MATCH (c2:Country)-[:DEPENDS_ON_TOURISM]->(t2:Tourism)
    WHERE abs(t2.percentage - tourism_level) <= $tolerance
    RETURN c2.name AS country_name, t2.percentage AS tourism_level
    """

    result_similar = tx.run(query, countryName=countryName, tolerance=tolerance)

    similar_countries = []

    
    for record in result_similar:
        similar_countries.append({
            "country_name": record["country_name"],
            "tourism_level": record["tourism_level"]
        })
    
    return similar_countries


######################################
#  Integracion de MongoDB y Neo4j    #
######################################

#Funcion que imprime los paises con mas de cierto numero de muertes en un dia
def CountryWithDeathAbove(deaths):
    if deaths <= 0:
        raise Exception("El numero de muertes debe ser mayor o igual a 0")

    countries = GetCountriesWithTotalDeathsAbove(deaths)
    for country in countries:
        date = country["date"]
        countryName = country["location"]
        deaths = country["new_deaths"]
        
        incomeClassification = session.execute_read(GetIncomeClassification, countryName)
        print(f"{date}\t{countryName}\tMuertes del dia: {country["new_deaths"]}\tMuertes totales: {country["total_deaths"]}\t Clasificacion de ingresos: {incomeClassification}")

#Funcion que imprime los paises con mas de cierto numero de casos en un dia
def CountryWithCasesAbove(cases):
    if cases <= 0:
        raise Exception("El numero de casos debe ser mayor o igual a 0")

    countries = GetCountriesWithTotalCasesAbove(cases)
    for country in countries:
        date = country["date"]
        countryName = country["location"]
        cases = country["new_cases"]

        incomeClassification = session.execute_read(GetIncomeClassification, countryName)
        print(f"{date}\t{countryName}\tCasos del dia: {country["new_cases"]}\tCasos totales: {country["total_cases"]}\t Clasificacion de ingresos: {incomeClassification}")

#Funcion que imprime las muertes y casos de los paises segun su clasificacion de ingresos
def DeathAndCasesInCountryWhitIncomeClassification(incomeClassification):
    if incomeClassification not in ["LIC", "MIC", "HIC"]:
        raise Exception("Clasificación de ingresos no válida")

    countries = session.execute_read(GetAllCountriesWhitIncomeClassification, incomeClassification)
    
    for country in countries:
        try:
            incomeClassification = session.execute_read(GetIncomeClassification, country)
            tourismPercentage = session.execute_read(GetTourismData, country)
            covidData = GetCovidData(country)
            deaths = covidData["deaths"]
            cases = covidData["cases"]
            print(f"\n{country}:\nMuertes totales: {deaths}\tCasos totales: {cases}\tClasificacion de ingresos: {incomeClassification}\t Porcentaje PIB de turismo: {tourismPercentage}")
        except:
            print(f"\n{country}\t No se encontraron datos de covid")

#Funcion que imprime el estado de los paises con porcentaje de turismo similar al pais ingresado
def CountryWithTourismSimilarity(countryName, tolerance=10):

    try:
        country_tourism = session.execute_read(GetTourismData, countryName)
        similar_countries = session.execute_read(GetSimilarTourismCountries, countryName, tolerance)
        similar_countries.append({"country_name": countryName, "tourism_level": country_tourism})

    except:
        raise Exception("No se encontraron datos de turismo")
    
    for country in similar_countries:
        country_data = GetCovidData(country["country_name"])
        total_cases = country_data["cases"]
        total_deaths = country_data["deaths"]
        print(f"\n{country['country_name']}\nPorcentaje PIB de turismo: {country['tourism_level']}\tMuertes totales: {total_deaths}\tCasos totales: {total_cases}")

def menu():

    while True:
        print("\nMenu:\n")
        print("1. Consultar paises con mas de n muertes en un dia")
        print("2. Consultar paises con mas de n casos en un dia")
        print("3. Muertes y casos en paises segun su clasificacion de ingresos")
        print("4. Estado paises con porcentaje de turismo similar")
        print("0. Salir")

        option = input("Ingrese una opcion: ")

        try:

            match(option):
                case "1":
                    deaths = int(input("Ingrese el numero de muertes: "))
                    CountryWithDeathAbove(deaths)
                case "2":
                    cases = int(input("Ingrese el numero de casos: "))
                    CountryWithCasesAbove(cases)
                case "3":
                    incomeClassification = input("Ingrese la clasificacion de ingresos (LIC, MIC, HIC): ")
                    DeathAndCasesInCountryWhitIncomeClassification(incomeClassification)
                case "4":
                    countryName = input("Ingrese el nombre del pais: ")
                    CountryWithTourismSimilarity(countryName, 2) # +- 2 de tolerancia
                case "0":
                    print("Saliendo...")
                    break
                case _:
                    print("Opcion no valida")

        except ValueError as e:
            print(f"Error: Opcion invalida")
        except Exception as e:
            print(f"Error: {e}")



menu()
