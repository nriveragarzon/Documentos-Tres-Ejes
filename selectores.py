# Librerias
from snowflake.snowpark import Session
import pandas as pd
import numpy as np

###############################################################
# FUNCIONES PARA GENERAR LAS OPCIONES DE ELECCIÓN PARA USUARIOS
###############################################################

# Selector de continentes
def selector_continentes(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de continentes distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de continentes distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.CONTINENTE_DANE_DIAN_EXPORTACIONES
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.PAISES_CORRELATIVA AS A
    WHERE A.CONTINENTE_DANE_DIAN_EXPORTACIONES NOT IN ('NO ENCONTRADO EN BASE DE EXPORTACIONES') 
        AND A.CONTINENTE_DANE_DIAN_EXPORTACIONES IS NOT NULL
        AND A.CONTINENTE_DANE_DIAN_EXPORTACIONES NOT IN ('No Declarados')
    ORDER BY 1 ASC;
    """

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['CONTINENTE_DANE_DIAN_EXPORTACIONES'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones

# Selector de tlcs
def selector_tlcs(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de tlcs distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de tlcs distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.TLCS_EXPORTACIONES
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.PAISES_CORRELATIVA AS A
    WHERE A.TLCS_EXPORTACIONES IS NOT NULL
        AND A.TLCS_EXPORTACIONES NOT IN ('No Declarados', 'NO ENCONTRADO EN BASE DE EXPORTACIONES')
    ORDER BY 1 ASC;
    """

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['TLCS_EXPORTACIONES'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones

# Selector de HUBS
def selector_hubs(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de hubs distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de hubs distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.HUB__C_EXPORTACIONES
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.PAISES_CORRELATIVA AS A
    WHERE A.HUB__C_EXPORTACIONES NOT IN ('NO ENCONTRADO EN BASE DE EXPORTACIONES', 'Colombia') 
        AND A.HUB__C_EXPORTACIONES  IS NOT NULL
    ORDER BY 1 ASC;
    """

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['HUB__C_EXPORTACIONES'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones

# Selector de continentes para paises
def selector_continentes_paises(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de continentes distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada para luego usarlos como selectores de países.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de continentes distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.REGION_NAME_UNSD
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.PAISES_CORRELATIVA AS A
    WHERE A.REGION_NAME_UNSD IS NOT NULL
        AND A.REGION_NAME_UNSD NOT IN ('Antártida')
    ORDER BY 1 ASC;
    """

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['REGION_NAME_UNSD'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones


# Selector de países
def selector_paises(session, continentes):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de países distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.
    - continentes: lista de strings con los continentes seleccionados para filtrar los países de interés.

    Retorna:
    - opciones: Lista de países distintos ordenada alfabéticamente.
    """
    # 0. Transformar lista de continentes 
    continente_pais_list = [continentes] if continentes else []

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.COUNTRY_OR_AREA_UNSD
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.PAISES_CORRELATIVA AS A
    WHERE A.COUNTRY_OR_AREA_UNSD IS NOT NULL
    """
    # Agrupación geográfica: 
    if continentes:
        query += f""" AND A.REGION_NAME_UNSD IN ({','.join([f"'{continente}'" for continente in continente_pais_list])});"""

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['COUNTRY_OR_AREA_UNSD'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones

# Selector de departamentos
def selector_departamento(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de departamentos distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de hubs distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.DEPARTAMENTO_DIAN
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIAN_DEPARTAMENTOS AS A
    WHERE A.DEPARTAMENTO_DIAN NOT IN ('Desconocido', 'Sin especificar')
    ORDER BY 1 ASC;
    """

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['DEPARTAMENTO_DIAN'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones