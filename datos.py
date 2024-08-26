# Librerias
import pandas as pd
import numpy as np
# import snowflake.connector # [pip install snowflake-connector-python]
from snowflake.connector.pandas_tools import write_pandas # [pip install "snowflake-connector-python[pandas]"]
from snowflake.snowpark import Session

# test de commit

######################################################
# FUNCIONES PARA OBTENER Y TRANSFORMAR DATOS TRES EJES
######################################################

def get_data_parametros(session, agrupacion, continentes=None, paises=None, hubs=None, tlcs=None, departamentos=None, umbral=None):
    """
    Extrae datos desde Snowflake aplicando filtros específicos y devuelve los nombres de las columnas para usar como parámetros en las consultas posteriores.

    Parámetros:
    session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    agrupacion: define el nivel de agrupación para el que se van a obtener los parámetros: continente, país, hubs, tlcs, departamentos o Colombia.
    continentes (list): Lista de continentes a filtrar.
    paises (list): Lista de países a filtrar.
    hubs (list): Lista de hubs a filtrar.
    tlcs (list): Lista de tratados de libre comercio a filtrar.
    departamentos (list): Lista de departamentos a filtrar.
    umbral (list): Lista con el valor de umbral para contar empresas.
    
    Pasos del proceso:
    1. Verificar que los parámetros son listas o None.
    2. Construir la consulta SQL base.
    3. Añadir condiciones a la consulta SQL según los parámetros proporcionados.
    4. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas.
    5. Devolver los nombres de las columnas en una lista.

    Retorna:
    list: Lista con los nombres de las columnas del DataFrame resultante.
    """
    
    # 1. Verificar que los parámetros son listas o None
    # Para cada parámetro en la lista [continentes, paises, hubs, tlcs, departamentos], verifica si es una lista o None.
    # Si algún parámetro no es una lista y no es None, lanza una excepción.
    for param in [continentes, paises, hubs, tlcs, departamentos]:
        if param is not None and not isinstance(param, list):
            raise ValueError("Todos los parámetros deben ser listas o None")
        
    # 2. Construir la consulta SQL base
    # Define una consulta SQL básica para extraer datos desde la tabla PAISES_CORRELATIVA en Snowflake.
    query = """
    SELECT A.PAIS_LLAVE_EXPORTACIONES,
        A.CONTINENTE_DANE_DIAN_EXPORTACIONES,
        A.OFICINA_COMERCIAL_EXPORTACIONES,
        A.HUB__C_EXPORTACIONES,
        A.TIPO_ACUERDO_EXPORTACIONES,
        A.TLCS_EXPORTACIONES,
        A.PAIS_INVERSION_BANREP,
        A.PAIS_CODIGO_TURISMO,
        A.NOMBRE_PAIS_CODIGO_TURISMO,
        A.COUNTRY_OR_AREA_UNSD
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.PAISES_CORRELATIVA AS A
    WHERE A.PAIS_LLAVE_EXPORTACIONES IS NOT NULL
    """
    
    # 3. Añadir condiciones a la consulta SQL según los parámetros proporcionados
    # Añade condiciones a la consulta SQL para filtrar según los parámetros proporcionados (continentes, paises, hubs, tlcs).
    if continentes:
        query += f""" AND A.CONTINENTE_DANE_DIAN_EXPORTACIONES IN ({','.join([f"'{continente}'" for continente in continentes])})"""
    if paises:
        query += f""" AND A.COUNTRY_OR_AREA_UNSD IN ({','.join([f"'{pais}'" for pais in paises])})"""
    if hubs:
        query += f""" AND A.HUB__C_EXPORTACIONES IN ({','.join([f"'{hub}'" for hub in hubs])})"""
    if tlcs:
        query += f""" AND A.TLCS_EXPORTACIONES IN ({','.join([f"'{tlc}'" for tlc in tlcs])})"""
    
    # 4. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    # Ejecuta la consulta SQL y convierte los resultados en un DataFrame de pandas.
    data = pd.DataFrame(session.sql(query).collect()) 
    
    # Obtener la lista de países para las agrupaciones de países como continentes, tlcs, hubs.
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS']:
        PAISES_lista =  data['COUNTRY_OR_AREA_UNSD'].dropna().unique().tolist()
        paises_anexo_str = ', '.join(PAISES_lista)


    # 5. Presentar parámetros según agrupación
    # Dependiendo del valor de 'agrupacion', define los parámetros resultantes y devuelve el diccionario correspondiente.
    if agrupacion == 'COLOMBIA':
          unidad_list = [agrupacion]
          return {
            'AGRUPACION': agrupacion,
            'UNIDAD': unidad_list,
            'UMBRAL': umbral
        }
    
    if agrupacion == 'CONTINENTES':
        return {
            'AGRUPACION': agrupacion,
            'UNIDAD': data['CONTINENTE_DANE_DIAN_EXPORTACIONES'].unique().tolist(),
            'PAISES_INVERSION': data['PAIS_INVERSION_BANREP'].unique().tolist(),
            'PAISES_TURISMO_COD': data['PAIS_CODIGO_TURISMO'].unique().tolist(),
            'PAISES_TURISMO': data['NOMBRE_PAIS_CODIGO_TURISMO'].unique().tolist(),
            'UMBRAL': umbral,
            'PAISES_ANEXO' : paises_anexo_str
            
        }

    if agrupacion == 'HUBS':
        return {
            'AGRUPACION': agrupacion,
            'UNIDAD': data['HUB__C_EXPORTACIONES'].unique().tolist(),
            'PAISES_INVERSION': data['PAIS_INVERSION_BANREP'].unique().tolist(),
            'PAISES_TURISMO_COD': data['PAIS_CODIGO_TURISMO'].unique().tolist(),
            'PAISES_TURISMO': data['NOMBRE_PAIS_CODIGO_TURISMO'].unique().tolist(),
            'UMBRAL': umbral,
            'PAISES_ANEXO' : paises_anexo_str
        }
    
    if agrupacion == 'TLCS':
        return {
            'AGRUPACION': agrupacion,
            'UNIDAD': data['TLCS_EXPORTACIONES'].unique().tolist(),
            'PAISES_INVERSION': data['PAIS_INVERSION_BANREP'].unique().tolist(),
            'PAISES_TURISMO_COD': data['PAIS_CODIGO_TURISMO'].unique().tolist(),
            'PAISES_TURISMO': data['NOMBRE_PAIS_CODIGO_TURISMO'].unique().tolist(),
            'UMBRAL': umbral,
            'PAISES_ANEXO' : paises_anexo_str
        }

    if agrupacion == 'PAISES':
        # Se agrega un condicional para que se tenga en cuenta los países sin llave de exportaciones
        if data.empty:
            # Se asegura la creación del documento sin datos pero se asegura para evitar errores. 
            return {
            'AGRUPACION': agrupacion,
            'UNIDAD': ['PAÍS NO INCLUIDO'],
            'PAISES_INVERSION': ['PAÍS NO INCLUIDO'],
            'PAISES_TURISMO_COD': ['PAÍS NO INCLUIDO'],
            'PAISES_TURISMO': ['PAÍS NO INCLUIDO'],
            'UMBRAL': umbral,
            'NOMBRE PAIS' : ['PAÍS NO INCLUIDO'] 
            }
        else:
            # Se ejecuta el documento para los países con datos.           
            return {
                'AGRUPACION': agrupacion,
                'UNIDAD': data['PAIS_LLAVE_EXPORTACIONES'].unique().tolist(),
                'PAISES_INVERSION': data['PAIS_INVERSION_BANREP'].unique().tolist(),
                'PAISES_TURISMO_COD': data['PAIS_CODIGO_TURISMO'].unique().tolist(),
                'PAISES_TURISMO': data['NOMBRE_PAIS_CODIGO_TURISMO'].unique().tolist(),
                'UMBRAL': umbral,
                'NOMBRE PAIS' : data['COUNTRY_OR_AREA_UNSD'].unique().tolist()
            }
    
    if agrupacion == 'DEPARTAMENTOS':
        query_dept = """
        SELECT A.COD_DIAN_DEPARTAMENTO,
            A.DEPARTAMENTO_DIAN
        FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIAN_DEPARTAMENTOS AS A
        """
        query_dept += f"""WHERE A.DEPARTAMENTO_DIAN IN ({','.join([f"'{departamento}'" for departamento in departamentos])})"""
        data = pd.DataFrame(session.sql(query_dept).collect())
        UNIDAD = data['DEPARTAMENTO_DIAN'].unique().tolist()
        UNIDAD_COD = data['COD_DIAN_DEPARTAMENTO'].unique().tolist()
        query_mun = """
        SELECT A.COD_DANE_DEPARTAMENTO,
            A.DEPARTAMENTO_DANE,
            A.COD_DANE_MUNICIPIO, 
            A.MUNICIPIO_DANE
        FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIVIPOLA_DEPARTAMENTOS_MUNICIPIOS AS A
        """
        query_mun += f"""WHERE COD_DANE_DEPARTAMENTO IN ({','.join([f"'{UNIDAD_COD_INDIVIDUAL}'" for UNIDAD_COD_INDIVIDUAL in UNIDAD_COD])})"""
        data_mun = pd.DataFrame(session.sql(query_mun).collect())
        MUNICIPIO_TURISMO_COD = data_mun['COD_DANE_MUNICIPIO'].unique().tolist()
        MUNICIPIO_TURISMO = data_mun['MUNICIPIO_DANE'].unique().tolist()
        return {
            'AGRUPACION': agrupacion,
            'UNIDAD': UNIDAD,
            'UNIDAD_COD': UNIDAD_COD,
            'MUNICIPIO_TURISMO_COD': MUNICIPIO_TURISMO_COD,
            'MUNICIPIO_TURISMO': MUNICIPIO_TURISMO,
            'UMBRAL': umbral
        }
    

def verif_ejes(session, params):
    """
    Función para verificar la existencia de datos en diferentes categorías (exportaciones, inversión y turismo)
    agrupados por diferentes criterios (CONTINENTES, HUBS, TLCS, PAISES, DEPARTAMENTOS). La función ejecuta
    consultas SQL en una sesión de Snowflake y agrega los resultados a un diccionario indicando si hay datos
    disponibles o no en cada categoría y periodo (cerrado o corrido).

    Parámetros:
    - session: Sesión activa de Snowflake.
    - params: Diccionario con los parámetros necesarios para ejecutar las consultas, incluyendo AGRUPACION,
              UNIDAD, UMBRAL, PAISES_INVERSION, PAISES_TURISMO_COD, y UNIDAD_COD.

    Retorna:
    - dict_verif: Diccionario con los resultados de la verificación, indicando si hay datos disponibles o no
                  para cada categoría y periodo.
    """

    # 1. Obtener los parámetros según sea la agrupación y unidad
    # Parámetros para los datos de exportaciones
    AGRUPACION = params['AGRUPACION']
    UNIDAD = params['UNIDAD'][0]
    UMBRAL = params['UMBRAL'][0]

    # Parámetros para los datos de inversión
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        PAISES_INVERSION = [pais for pais in params['PAISES_INVERSION'] if pais is not None]
        PAISES_INVERSION_sql = ', '.join(f"'{pais}'" for pais in PAISES_INVERSION)

    # Parámetros para los datos de turismo
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        PAISES_TURISMO = [pais for pais in params['PAISES_TURISMO_COD'] if pais is not None]
        PAISES_TURISMO_sql = ', '.join(f"'{pais}'" for pais in PAISES_TURISMO)
    if AGRUPACION in ['DEPARTAMENTOS']:
        DEPARTAMENTOS_TURISMO = [departamento for departamento in params['UNIDAD_COD'] if departamento is not None]
        DEPARTAMENTOS_TURISMO_sql = ', '.join(f"'{departamento}'" for departamento in DEPARTAMENTOS_TURISMO)

    # 2. Diccionario para almacenar los resultados
    dict_verif = {}

    # El siguiente proceso no es válido para la agrupación de COLOMBIA ya que esta siempre tiene datos: 
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'DEPARTAMENTOS']:

        # 3. Verificación de exportaciones
        # Consultas para verificar datos de exportaciones en periodos cerrados y corridos, tanto totales como No Mineras (NME)
        
        # Totales
        # Cerrado
        query_verif_exportaciones_total_cerrado = f"""
        SELECT DISTINCT A.UNIDAD
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CERRADO AS A
        WHERE A.AGRUPACION = '{AGRUPACION}' 
            AND A.UNIDAD IN ('{UNIDAD}')
            AND A.TABLA = 'TOTAL';
        """
        # Corrido
        query_verif_exportaciones_total_corrido = f"""
        SELECT DISTINCT A.UNIDAD
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CORRIDO AS A
        WHERE A.AGRUPACION = '{AGRUPACION}' 
            AND A.UNIDAD IN ('{UNIDAD}')
            AND A.TABLA = 'TOTAL';
        """

        # NME
        # Cerrado
        query_verif_exportaciones_nme_cerrado = f"""
        SELECT DISTINCT A.UNIDAD
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CERRADO AS A
        WHERE A.AGRUPACION = '{AGRUPACION}' 
            AND A.UNIDAD IN ('{UNIDAD}')
            AND A.TABLA = 'TIPOS'
            AND A.CATEGORIA = 'No Mineras';
        """
        # Corrido
        query_verif_exportaciones_nme_corrido = f"""
        SELECT DISTINCT A.UNIDAD
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CORRIDO AS A
        WHERE A.AGRUPACION = '{AGRUPACION}' 
            AND A.UNIDAD IN ('{UNIDAD}')
            AND A.TABLA = 'TIPOS'
            AND A.CATEGORIA = 'No Mineras';
        """

        # Conteo de empresas
        # Cerrado
        query_verif_exportaciones_cuenta_cerrado = f"""
        SELECT A.NIT_EXPORTADOR, A.YEAR
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CONTEO_EMPRESAS_CERRADO AS A
            WHERE A.AGRUPACION = '{AGRUPACION}' 
                AND A.UNIDAD = '{UNIDAD}'
                AND A.VALOR_USD > {UMBRAL}
            ORDER BY A.YEAR ASC;
        """
        # Corrido
        query_verif_exportaciones_cuenta_corrido = f"""
        SELECT A.NIT_EXPORTADOR, A.YEAR
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CONTEO_EMPRESAS_CORRIDO AS A
            WHERE A.AGRUPACION = '{AGRUPACION}' 
                AND A.UNIDAD = '{UNIDAD}'
                AND A.VALOR_USD > {UMBRAL}
            ORDER BY A.YEAR ASC;
        """

        # Datos de empresas
        # Cerrado
        query_verif_exportaciones_empresas_cerrado = f"""
            SELECT A.CATEGORIA, 
                    A.RAZON_SOCIAL,
                    A.SECTOR_ESTRELLA,
                    A.SUMA_USD_T_1, 
                    A.SUMA_USD_T, 
                    A.DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_NIT_CERRADO AS A 
            WHERE A.AGRUPACION = '{AGRUPACION}' 
                AND A.UNIDAD = '{UNIDAD}'
            ORDER BY SUMA_USD_T DESC LIMIT 5;
        """
        # Corrido
        query_verif_exportaciones_empresas_corrido = f"""
            SELECT A.CATEGORIA, 
                    A.RAZON_SOCIAL,
                    A.SECTOR_ESTRELLA,
                    A.SUMA_USD_T_1, 
                    A.SUMA_USD_T, 
                    A.DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_NIT_CORRIDO AS A 
            WHERE A.AGRUPACION = '{AGRUPACION}' 
                AND A.UNIDAD = '{UNIDAD}'
            ORDER BY SUMA_USD_T DESC LIMIT 5;
        """

        # Ejecución de consultas y agregar al diccionario si los países o departamentos son válidos o no        
        df_export_total_cerrado = pd.DataFrame(session.sql(query_verif_exportaciones_total_cerrado).collect())
        df_export_total_corrido = pd.DataFrame(session.sql(query_verif_exportaciones_total_corrido).collect())
        df_nme_cerrado = pd.DataFrame(session.sql(query_verif_exportaciones_nme_cerrado).collect())
        df_nme_corrido = pd.DataFrame(session.sql(query_verif_exportaciones_nme_corrido).collect())
        df_conteo_cerrado = pd.DataFrame(session.sql(query_verif_exportaciones_cuenta_cerrado).collect())
        df_conteo_corrido = pd.DataFrame(session.sql(query_verif_exportaciones_cuenta_corrido).collect())
        df_empresas_cerrado = pd.DataFrame(session.sql(query_verif_exportaciones_empresas_cerrado).collect())
        df_empresas_corrido = pd.DataFrame(session.sql(query_verif_exportaciones_empresas_corrido).collect())


        # Verificar y agregar al diccionario dict_verif
        # Exportaciones Totales
        if df_export_total_cerrado.empty:
            dict_verif['exportaciones_totales_cerrado'] = "SIN DATOS DE EXPORTACIONES TOTALES CERRADO"
        else:
            dict_verif['exportaciones_totales_cerrado'] = "CON DATOS DE EXPORTACIONES TOTALES CERRADO"

        if df_export_total_corrido.empty:
            dict_verif['exportaciones_totales_corrido'] = "SIN DATOS DE EXPORTACIONES TOTALES CORRIDO"
        else:
            dict_verif['exportaciones_totales_corrido'] = "CON DATOS DE EXPORTACIONES TOTALES CORRIDO"

        # Exportaciones NME
        if df_nme_cerrado.empty:
            dict_verif['exportaciones_nme_cerrado'] = "SIN DATOS DE EXPORTACIONES NME CERRADO"
        else:
            dict_verif['exportaciones_nme_cerrado'] = "CON DATOS DE EXPORTACIONES NME CERRADO"

        if df_nme_corrido.empty:
            dict_verif['exportaciones_nme_corrido'] = "SIN DATOS DE EXPORTACIONES NME CORRIDO"
        else:
            dict_verif['exportaciones_nme_corrido'] = "CON DATOS DE EXPORTACIONES NME CORRIDO"

        # Conteo de empresas
        if df_conteo_cerrado.empty:
            dict_verif['exportaciones_conteo_cerrado'] = "SIN DATOS DE CONTEO CERRADO"
        else:
            dict_verif['exportaciones_conteo_cerrado'] = "CON DATOS DE CONTEO CERRADO"

        if df_conteo_corrido.empty:
            dict_verif['exportaciones_conteo_corrido'] = "SIN DATOS DE CONTEO CORRIDO"
        else:
            dict_verif['exportaciones_conteo_corrido'] = "CON DATOS DE CONTEO CORRIDO"

        # Empresas
        if df_empresas_cerrado.empty:
            dict_verif['exportaciones_empresas_cerrado'] = "SIN DATOS DE EMPRESAS CERRADO"
        else:
            dict_verif['exportaciones_empresas_cerrado'] = "CON DATOS DE EMPRESAS CERRADO"

        if df_empresas_corrido.empty:
            dict_verif['exportaciones_empresas_corrido'] = "SIN DATOS DE EMPRESAS CORRIDO"
        else:
            dict_verif['exportaciones_empresas_corrido'] = "CON DATOS DE EMPRESAS CORRIDO"


        # 4. Verificación de inversión 
        # Consultas para verificar datos de inversión extranjera directa (IED) e inversión colombiana en el exterior (ICE)
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
            # IED
            # CERRADO
            query_verif_ied_cerrado = f"""
            SELECT DISTINCT A.UNIDAD
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD NOT IN ('TOTAL')
                AND A.CATEGORIA = 'IED'
            """
            query_verif_ied_cerrado += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"
            # CORRIDO
            query_verif_ied_corrido = f"""
            SELECT DISTINCT A.UNIDAD
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD NOT IN ('TOTAL')
                AND A.CATEGORIA = 'IED'
            """
            query_verif_ied_corrido += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"

            # ICE
            # CERRADO
            query_verif_ice_cerrado = f"""
            SELECT DISTINCT A.UNIDAD
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD NOT IN ('TOTAL')
                AND A.CATEGORIA = 'ICE'
            """
            query_verif_ice_cerrado += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"
            # CORRIDO
            query_verif_ice_corrido = f"""
            SELECT DISTINCT A.UNIDAD
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD NOT IN ('TOTAL')
                AND A.CATEGORIA = 'ICE'
            """
            query_verif_ice_corrido += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"

            # Ejecución de consultas y agregar al diccionario si los países son válidos o no

            # IED
            try:
                df_ied_cerrado = pd.DataFrame(session.sql(query_verif_ied_cerrado).collect())
                if df_ied_cerrado.empty:
                    dict_verif['ied_cerrado'] = "SIN DATOS DE IED CERRADO"
                else:
                    dict_verif['ied_cerrado'] = "CON DATOS DE IED CERRADO"
            except Exception as e:
                dict_verif['ied_cerrado'] = "SIN DATOS DE IED CERRADO"

            try:
                df_ied_corrido = pd.DataFrame(session.sql(query_verif_ied_corrido).collect())
                if df_ied_corrido.empty:
                    dict_verif['ied_corrido'] = "SIN DATOS DE IED CORRIDO"
                else:
                    dict_verif['ied_corrido'] = "CON DATOS DE IED CORRIDO"
            except Exception as e:
                dict_verif['ied_corrido'] = "SIN DATOS DE IED CORRIDO"

            # ICE
            try:
                df_ice_cerrado = pd.DataFrame(session.sql(query_verif_ice_cerrado).collect())
                if df_ice_cerrado.empty:
                    dict_verif['ice_cerrado'] = "SIN DATOS DE ICE CERRADO"
                else:
                    dict_verif['ice_cerrado'] = "CON DATOS DE ICE CERRADO"
            except Exception as e:
                dict_verif['ice_cerrado'] = "SIN DATOS DE ICE CERRADO"

            try:
                df_ice_corrido = pd.DataFrame(session.sql(query_verif_ice_corrido).collect())
                if df_ice_corrido.empty:
                    dict_verif['ice_corrido'] = "SIN DATOS DE ICE CORRIDO"
                else:
                    dict_verif['ice_corrido'] = "CON DATOS DE ICE CORRIDO"
            except Exception as e:
                dict_verif['ice_corrido'] = "SIN DATOS DE ICE CORRIDO"

        
        # 5. Verificación de turismo
        # Consultas para verificar datos de turismo en periodos cerrados y corridos, considerando agrupación por países o departamentos
        # Cerrado
        query_verif_turismo_cerrado = f"""
        SELECT A.PAIS_RESIDENCIA,
            FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
        WHERE 1=1
        """
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_verif_turismo_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
        if AGRUPACION in ['DEPARTAMENTOS']:
            query_verif_turismo_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
        
        # Corrido
        query_verif_turismo_corrido = f"""
        SELECT A.PAIS_RESIDENCIA,
            FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
        WHERE 1=1
        """
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_verif_turismo_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
        if AGRUPACION in ['DEPARTAMENTOS']:
            query_verif_turismo_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
        
        # Ejecución de consultas y agregar al diccionario si los países son válidos o no
        # Turismo
        try:
            df_turismo_cerrado = pd.DataFrame(session.sql(query_verif_turismo_cerrado).collect())
            if df_turismo_cerrado.empty:
                dict_verif['turismo_cerrado'] = "SIN DATOS DE TURISMO CERRADO"
            else:
                dict_verif['turismo_cerrado'] = "CON DATOS DE TURISMO CERRADO"
        except Exception as e:
            dict_verif['turismo_cerrado'] = "SIN DATOS DE TURISMO CERRADO"

        try:
            df_turismo_corrido = pd.DataFrame(session.sql(query_verif_turismo_corrido).collect())
            if df_turismo_corrido.empty:
                dict_verif['turismo_corrido'] = "SIN DATOS DE TURISMO CORRIDO"
            else:
                dict_verif['turismo_corrido'] = "CON DATOS DE TURISMO CORRIDO"
        except Exception as e:
            dict_verif['turismo_corrido'] = "SIN DATOS DE TURISMO CORRIDO"
        
    ##############
    # CONECTIVIDAD
    ##############

    if AGRUPACION in ['DEPARTAMENTOS']:
        # Construir consulta
        query_conectividad = """
        SELECT A.AEROLINEA AS "Aerolínea",
            A.CIUDAD_ORIGEN AS "Ciudad Origen",
            A.CIUDAD_DESTINO AS "Ciudad Destino",
            A.FRECUENCIAS AS "Frecuencias",
            A.SEMANA AS "Semana de análisis"
        FROM DOCUMENTOS_COLOMBIA.TURISMO.CONECTIVIDAD AS A
        WHERE 1 = 1
        """
        # Agregar departamento
        query_conectividad += f" AND A.COD_DIVIPOLA_DEPARTAMENTO_DESTINO IN ({DEPARTAMENTOS_TURISMO_sql})"

        # Verificación
        try:
            # Ejecutar la consulta y recolectar los resultados en un DataFrame
            df_conectividad = pd.DataFrame(session.sql(query_conectividad).collect())
            
            # Verificar si el DataFrame está vacío
            if df_conectividad.empty:
                dict_verif['conectividad'] = "SIN DATOS DE CONECTIVIDAD"
            else:
                dict_verif['conectividad'] = "CON DATOS DE CONECTIVIDAD"
        except Exception as e:
            dict_verif['conectividad'] = "SIN DATOS DE CONECTIVIDAD"
    
    ###############
    # OPORTUNIDADES
    ###############
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'DEPARTAMENTOS']:
        # Exportación
        query_oportunidades_exportacion = """
        SELECT DISTINCT A.CADENA,
            LOWER(A.SUBSECTOR) AS SUBSECTOR
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
        WHERE A.OPORTUNIDAD = 'Exportación' 
            AND A.CADENA NOT IN ('Turismo')
        """
        # Países y agrupaciones de países
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_oportunidades_exportacion += f" AND A.COD_PAIS IN ({PAISES_TURISMO_sql})"
        # Departamentos
        if AGRUPACION in ['DEPARTAMENTOS']:
            query_oportunidades_exportacion += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
        # Order
        query_oportunidades_exportacion += f" ORDER BY 1, 2 ASC"

        # Verificación
        try:
            df_oportunidades_exportacion = pd.DataFrame(session.sql(query_oportunidades_exportacion).collect())
            if df_oportunidades_exportacion.empty:
                dict_verif['oportunidades_exportacion'] = "SIN OPORTUNIDADES"
            else:
                dict_verif['oportunidades_exportacion'] = "CON OPORTUNIDADES"
        except Exception as e:
            dict_verif['oportunidades_exportacion'] = "SIN OPORTUNIDADES"

        # Inversión
        query_oportunidades_ied = """
        SELECT DISTINCT A.CADENA,
            LOWER(A.SUBSECTOR) AS SUBSECTOR
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
        WHERE A.OPORTUNIDAD = 'IED'
        """
        # Países y agrupaciones de países
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_oportunidades_ied += f" AND A.COD_PAIS IN ({PAISES_TURISMO_sql})"
        # Departamentos
        if AGRUPACION in ['DEPARTAMENTOS']:
            query_oportunidades_ied += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
        # Order
        query_oportunidades_ied += f" ORDER BY 1, 2 ASC"

        # Verificación
        try:
            df_oportunidades_inversion = pd.DataFrame(session.sql(query_oportunidades_ied).collect())
            if df_oportunidades_inversion.empty:
                dict_verif['oportunidades_inversion'] = "SIN OPORTUNIDADES"
            else:
                dict_verif['oportunidades_inversion'] = "CON OPORTUNIDADES"
        except Exception as e:
            dict_verif['oportunidades_inversion'] = "SIN OPORTUNIDADES"
        
        # Turismo
        query_oportunidades_turismo = """
        SELECT DISTINCT A.SECTOR,
            LOWER(A.SUBSECTOR) AS SUBSECTOR
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
        WHERE A.CADENA IN ('Turismo')
        """
        # Países y agrupaciones de países
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_oportunidades_turismo += f" AND A.COD_PAIS IN ({PAISES_TURISMO_sql})"
        # Departamentos
        if AGRUPACION in ['DEPARTAMENTOS']:
            query_oportunidades_turismo += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
        # Order
        query_oportunidades_turismo += f" ORDER BY 1, 2 ASC"
        # Verificación
        try:
            df_oportunidades_turismo = pd.DataFrame(session.sql(query_oportunidades_turismo).collect())
            if df_oportunidades_turismo.empty:
                dict_verif['oportunidades_turismo'] = "SIN OPORTUNIDADES"
            else:
                dict_verif['oportunidades_turismo'] = "CON OPORTUNIDADES"
        except Exception as e:
            dict_verif['oportunidades_turismo'] = "SIN OPORTUNIDADES"

    
    #################
    # PESOS POR MEDIO
    #################
        # Mineros
        # Cerrado
        query_pesos_mineros_cerrado = f"""
        SELECT A.CATEGORIA, 
            A.SUMA_PESO_T_1, 
            A.SUMA_PESO_T, 
            A.DIFERENCIA_PORCENTUAL 
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_PESO_CERRADO AS A 
        WHERE A.TABLA = 'MEDIO MINERAS'
            AND A.AGRUPACION = '{AGRUPACION}' 
            AND A.UNIDAD IN ('{UNIDAD}');
        """
        # Verificación
        try:
            df_pesos_minero_cerrado = pd.DataFrame(session.sql(query_pesos_mineros_cerrado).collect())
            if df_pesos_minero_cerrado.empty:
                dict_verif['pesos_minero_cerrado'] = "SIN DATOS CERRADO"
            else:
                dict_verif['pesos_minero_cerrado'] = "CON DATOS CERRADO"
        except Exception as e:
            dict_verif['pesos_minero_cerrado'] = "SIN DATOS CERRADO"

        # Corrido
        query_pesos_mineros_corrido = f"""
        SELECT A.CATEGORIA, 
            A.SUMA_PESO_T_1, 
            A.SUMA_PESO_T, 
            A.DIFERENCIA_PORCENTUAL 
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_PESO_CORRIDO AS A 
        WHERE A.TABLA = 'MEDIO MINERAS'
            AND A.AGRUPACION = '{AGRUPACION}' 
            AND A.UNIDAD IN ('{UNIDAD}');
        """
        # Verificación
        try:
            df_pesos_minero_corrido = pd.DataFrame(session.sql(query_pesos_mineros_corrido).collect())
            if df_pesos_minero_corrido.empty:
                dict_verif['pesos_minero_corrido'] = "SIN DATOS CORRIDO"
            else:
                dict_verif['pesos_minero_corrido'] = "CON DATOS CORRIDO"
        except Exception as e:
            dict_verif['pesos_minero_corrido'] = "SIN DATOS CORRIDO"

        # No mineros
        # Cerrado
        query_pesos_no_mineros_cerrado = f"""
        SELECT A.CATEGORIA, 
            A.SUMA_PESO_T_1, 
            A.SUMA_PESO_T, 
            A.DIFERENCIA_PORCENTUAL 
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_PESO_CERRADO AS A 
        WHERE A.TABLA = 'MEDIO NO MINERAS'
            AND A.AGRUPACION = '{AGRUPACION}' 
            AND A.UNIDAD IN ('{UNIDAD}');
        """
        # Verificación
        try:
            df_pesos_no_minero_cerrado = pd.DataFrame(session.sql(query_pesos_no_mineros_cerrado).collect())
            if df_pesos_no_minero_cerrado.empty:
                dict_verif['pesos_no_minero_cerrado'] = "SIN DATOS CERRADO"
            else:
                dict_verif['pesos_no_minero_cerrado'] = "CON DATOS CERRADO"
        except Exception as e:
            dict_verif['pesos_no_minero_cerrado'] = "SIN DATOS CERRADO"

        # Corrido
        query_pesos_no_mineros_corrido = f"""
        SELECT A.CATEGORIA, 
            A.SUMA_PESO_T_1, 
            A.SUMA_PESO_T, 
            A.DIFERENCIA_PORCENTUAL 
        FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_PESO_CORRIDO AS A 
        WHERE A.TABLA = 'MEDIO NO MINERAS'
            AND A.AGRUPACION = '{AGRUPACION}' 
            AND A.UNIDAD IN ('{UNIDAD}');
        """
        # Verificación
        try:
            df_pesos_no_minero_corrido = pd.DataFrame(session.sql(query_pesos_no_mineros_corrido).collect())
            if df_pesos_no_minero_corrido.empty:
                dict_verif['pesos_no_minero_corrido'] = "SIN DATOS CORRIDO"
            else:
                dict_verif['pesos_no_minero_corrido'] = "CON DATOS CORRIDO"
        except Exception as e:
            dict_verif['pesos_no_minero_corrido'] = "SIN DATOS CORRIDO"
   
    # Colombia es válidos para los tres ejes siempre:
    if AGRUPACION == 'COLOMBIA':
        dict_verif['exportaciones_totales_cerrado'] = "CON DATOS DE EXPORTACIONES TOTALES CERRADO"
        dict_verif['exportaciones_totales_corrido'] = "CON DATOS DE EXPORTACIONES TOTALES CORRIDO"
        dict_verif['exportaciones_nme_cerrado'] = "CON DATOS DE EXPORTACIONES NME CERRADO"
        dict_verif['exportaciones_nme_corrido'] = "CON DATOS DE EXPORTACIONES NME CORRIDO"
        dict_verif['ied_cerrado'] = "CON DATOS DE IED CERRADO"
        dict_verif['ied_corrido'] = "CON DATOS DE IED CORRIDO"
        dict_verif['ice_cerrado'] = "CON DATOS DE ICE CERRADO"
        dict_verif['ice_corrido'] = "CON DATOS DE ICE CORRIDO"
        dict_verif['turismo_cerrado'] = "CON DATOS DE TURISMO CERRADO"
        dict_verif['turismo_corrido'] = "CON DATOS DE TURISMO CORRIDO"
        dict_verif['exportaciones_conteo_cerrado'] = "CON DATOS DE CONTEO CERRADO"
        dict_verif['exportaciones_conteo_corrido'] = "CON DATOS DE CONTEO CORRIDO"
        dict_verif['exportaciones_empresas_cerrado'] = "CON DATOS DE EMPRESAS CERRADO"
        dict_verif['exportaciones_empresas_corrido'] = "CON DATOS DE EMPRESAS CORRIDO"
        dict_verif['oportunidades_exportacion'] = "CON OPORTUNIDADES"
        dict_verif['oportunidades_inversion'] = "CON OPORTUNIDADES"
        dict_verif['oportunidades_turismo'] = "CON OPORTUNIDADES"
        dict_verif['conectividad'] = "SIN DATOS DE CONECTIVIDAD"
        dict_verif['pesos_minero_cerrado'] = "CON DATOS CERRADO"
        dict_verif['pesos_minero_corrido'] = "CON DATOS CORRIDO"
        dict_verif['pesos_no_minero_cerrado'] = "CON DATOS CERRADO"
        dict_verif['pesos_no_minero_corrido'] = "CON DATOS CORRIDO"

    return dict_verif


def calcular_diferencia_porcentual(valor_actual, valor_anterior):
    """
    Calcula la diferencia porcentual entre dos valores.

    La función sigue la lógica de cálculo condicional:
    - Si `valor_anterior` es 0 y `valor_actual` es mayor que 0, retorna 100.
    - Si `valor_anterior` es 0 y `valor_actual` es igual a 0, retorna 0.
    - Si `valor_actual` es 0 y `valor_anterior` es mayor que 0, retorna -100.
    - En cualquier otro caso, calcula y retorna la diferencia porcentual como:
      ((`valor_actual` - `valor_anterior`) / `valor_anterior`) * 100.

    Parámetros:
    - valor_actual (float): Valor en el periodo actual.
    - valor_anterior (float): Valor en el periodo anterior.

    Retorna:
    - float: La diferencia porcentual entre los dos periodos.
    """
    if valor_anterior == 0 and valor_actual > 0:
        return 100
    elif valor_anterior == 0 and valor_actual == 0:
        return 0
    elif valor_actual == 0 and valor_anterior > 0:
        return -100
    else:
        return ((valor_actual - valor_anterior) / valor_anterior) * 100



def calcular_participacion_porcentual(df, columna, total, nombre_columna_resultante=None):
    """
    Calcula la participación porcentual de una columna específica en un DataFrame.

    Parámetros:
    df (pandas.DataFrame): El DataFrame que contiene los datos.
    columna (str): El nombre de la columna para la cual se calculará la participación porcentual.
    total (float): El total previamente calculado para usar como denominador.
    nombre_columna_resultante (str, opcional): El nombre de la columna resultante. Si no se especifica, 
                                               se usará el nombre original con el sufijo '_PARTICIPACION'.

    Retorna:
    pandas.DataFrame: El DataFrame con una nueva columna de participación porcentual.
    """
    if nombre_columna_resultante is None:
        nombre_columna_resultante = columna + '_PARTICIPACION'

    if total == 0:
        df[nombre_columna_resultante] = "No aplica"
    else:
        df[nombre_columna_resultante] = df[columna].apply(lambda x: 0 if x == 0 else (x / total) * 100)

    return df


def get_data(session, agrupacion, continentes=None, paises=None, hubs=None, tlcs=None, departamentos=None, umbral=None):
    """
    Esta función extrae y organiza datos de exportaciones desde una base de datos en Snowflake.

    Parámetros:
    - sesion: sesión de Snowflake.
    - agrupacion: el nivel de agrupación para filtrar los datos.
    - unidad: la unidad de medida para filtrar los datos.
    - umbral: valor USD exportado mínimo exportado para contar la empresa. 

    La función realiza los siguientes pasos:
    1. Define las categorías y tipos de tablas a consultar.
    2. Realiza consultas SQL para obtener los totales de exportaciones en USD.
    3. Realiza consultas SQL para obtener los tipos de exportaciones en USD.
    4. Realiza consultas SQL para obtener datos por categoría para año cerrado.
    5. Realiza consultas SQL para obtener datos por categoría para año corrido.
    6. Realiza consultas SQL para obtener información de empresas.
    7. Realiza consultas SQL para contar el número de empresas únicas por año.
    8. Retorna todos los resultados en un diccionario.
    """

    ######################################
    # OBTENER PARÁMETROS SEGÚN SEA EL CASO
    ######################################

    geo_params = get_data_parametros(session, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)

    # Parámetros para los datos de exportaciones
    AGRUPACION = geo_params['AGRUPACION']
    UNIDAD = geo_params['UNIDAD'][0]
    UMBRAL = geo_params['UMBRAL'][0]
    

    # Parámetros para los datos de inversión
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        PAISES_INVERSION = [pais for pais in geo_params['PAISES_INVERSION'] if pais is not None]
        PAISES_INVERSION_sql = ', '.join(f"'{pais}'" for pais in PAISES_INVERSION)
    
    # Parámetros para los datos de turismo
    # 'CONTINENTES' 'HUBS' 'TLCS' 'PAISES
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        PAISES_TURISMO = [pais for pais in geo_params['PAISES_TURISMO_COD'] if pais is not None]
        PAISES_TURISMO_sql = ', '.join(f"'{pais}'" for pais in PAISES_TURISMO)
    # 'DEPARTAMENTOS'
    if AGRUPACION in ['DEPARTAMENTOS']:
        DEPARTAMENTOS_TURISMO = [departamento for departamento in geo_params['UNIDAD_COD'] if departamento is not None]
        DEPARTAMENTOS_TURISMO_sql = ', '.join(f"'{departamento}'" for departamento in DEPARTAMENTOS_TURISMO)
        
    #################################
    # INDICADOR DE PRESENCIA DE DATOS
    #################################

    dict_verificacion = verif_ejes(session, geo_params)
    
    ##################################
    # Diccionario para hoja de resumen
    ##################################
    datos_resumen = {}

    ###############
    # Exportaciones
    ###############

    # 1. Definir las categorías y tipos de tablas a consultar
    categorias = ['CONTINENTE', 'DEPARTAMENTOS', 'HUBS', 'PAIS', 'SECTORES', 'SUBSECTORES', 'TLCS']
    tablas_usd = ['ST_CATEGORIAS_CERRADO', 'ST_CATEGORIAS_CORRIDO']    
    tablas_peso = ['ST_CATEGORIAS_PESO_CERRADO', 'ST_CATEGORIAS_PESO_CORRIDO']
    tablas_nit_empresas = ['ST_NIT_CERRADO', 'ST_NIT_CORRIDO']

    # 2. Consultar los totales de exportaciones en USD
    totales = {}
    for tabla in tablas_usd:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (tabla == 'ST_CATEGORIAS_CERRADO' and dict_verificacion['exportaciones_totales_cerrado'] == 'CON DATOS DE EXPORTACIONES TOTALES CERRADO') or \
           (tabla == 'ST_CATEGORIAS_CORRIDO' and dict_verificacion['exportaciones_totales_corrido'] == 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO'):
            # Construir la consulta SQL para obtener los totales de exportaciones en USD
            query_totales = f"""
                SELECT 'Total' AS CATEGORIA, 
                        A.SUMA_USD_T_1, 
                        A.SUMA_USD_T,  
                        A.DIFERENCIA_PORCENTUAL 
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                WHERE A.AGRUPACION = '{AGRUPACION}' 
                    AND A.UNIDAD IN ('{UNIDAD}')
                    AND A.TABLA = 'TOTAL';
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query_totales).collect())
            # Almacenar el DataFrame en el diccionario 'totales' con el nombre de la tabla como clave
            totales[tabla] = data

    # 3. Consultar los tipos de exportaciones en USD
    tipos = {}
    for tabla in tablas_usd:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (tabla == 'ST_CATEGORIAS_CERRADO' and dict_verificacion['exportaciones_totales_cerrado'] == 'CON DATOS DE EXPORTACIONES TOTALES CERRADO') or \
           (tabla == 'ST_CATEGORIAS_CORRIDO' and dict_verificacion['exportaciones_totales_corrido'] == 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO'):
            # Construir la consulta SQL para obtener los tipos de exportaciones en USD
            query_tipos = f"""
                SELECT A.CATEGORIA, 
                        A.SUMA_USD_T_1, 
                        A.SUMA_USD_T, 
                        A.DIFERENCIA_PORCENTUAL 
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                WHERE A.AGRUPACION = '{AGRUPACION}' 
                    AND A.UNIDAD = '{UNIDAD}'
                    AND A.TABLA = 'TIPOS';
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query_tipos).collect())
            # Calcular el total de exportaciones en USD para agregar participación
            total_t = totales[tabla]['SUMA_USD_T'].sum()
            # Concatenar los datos de tipos con los totales
            data = pd.concat([data, totales[tabla]])
            # Calcular la participación de cada tipo en el total de exportaciones
            data = calcular_participacion_porcentual(data, 'SUMA_USD_T', total_t, 'PARTICIPACION_T')
            # Almacenar el DataFrame en el diccionario 'tipos' con el nombre de la tabla como clave
            tipos[tabla] = data
            # Agregar los datos al diccionario de resumen
            # Inicializar la entrada en el diccionario de resumen para la tabla actual
            if tabla not in datos_resumen:
                datos_resumen[tabla] = {}
            # Recorrer las filas del DataFrame y almacenar los valores en el diccionario
            for index, row in data.iterrows():
                categoria = row['CATEGORIA']
                if categoria not in datos_resumen[tabla]:
                    datos_resumen[tabla][categoria] = []
                datos_resumen[tabla][categoria].append({
                    'sum_usd_t_1': row['SUMA_USD_T_1'],
                    'sum_usd_t': row['SUMA_USD_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                    'participacion_t': row['PARTICIPACION_T']
                })

    # 4. Consultar datos por categoría para año cerrado
    categorias_cerrado = {}
    for categoria in categorias:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (dict_verificacion['exportaciones_nme_cerrado'] == 'CON DATOS DE EXPORTACIONES NME CERRADO'):
            # Construir la consulta SQL para obtener datos por categoría para año cerrado
            query = f"""
                SELECT A.CATEGORIA, 
                        A.SUMA_USD_T_1, 
                        A.SUMA_USD_T, 
                        A.DIFERENCIA_PORCENTUAL 
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CERRADO AS A
                WHERE A.AGRUPACION = '{AGRUPACION}' 
                    AND A.UNIDAD = '{UNIDAD}'
                    AND A.TABLA = '{categoria}'
                ORDER BY A.SUMA_USD_T DESC;
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query).collect())
            row_num = data.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
            data = data.head(5)           
            # Filtrar los datos totales para 'No Mineras' y cambiar la categoría a 'Total'
            df_totales_nme = tipos['ST_CATEGORIAS_CERRADO']
            df_totales_nme = df_totales_nme[df_totales_nme['CATEGORIA'] == 'No Mineras']
            df_totales_nme['CATEGORIA'] = 'Total'
            total_t = df_totales_nme['SUMA_USD_T'].sum()
            total_t_1 = df_totales_nme['SUMA_USD_T_1'].sum()
            # Calcular datos para la categoría 'Otros'
            otros_categoria = 'Otros'
            otros_t = total_t - data['SUMA_USD_T'].sum()
            otros_t_1 = total_t_1 - data['SUMA_USD_T_1'].sum()
            otros_porcentual = calcular_diferencia_porcentual(otros_t, otros_t_1)
            # Crear DataFrame para 'Otros'
            otros_df = pd.DataFrame({
                'CATEGORIA': [otros_categoria],
                'SUMA_USD_T_1': [otros_t_1],
                'SUMA_USD_T': [otros_t],
                'DIFERENCIA_PORCENTUAL': [otros_porcentual]
            })
            # Concatenar los datos de 'Otros' y los datos totales con el DataFrame original
            # Solo se concatena el otro, en caso de que exista esta categoria:
            if (row_num <= 5):
                data = pd.concat([data, df_totales_nme])
            else:
                data = pd.concat([data, otros_df, df_totales_nme])
            # Calcular la participación de cada categoría en el total de exportaciones
            data = calcular_participacion_porcentual(data, 'SUMA_USD_T', total_t, 'PARTICIPACION_T')
            # Almacenar el DataFrame en el diccionario 'categorias_cerrado' con la categoría como clave
            categorias_cerrado[categoria] = data

    # 5. Consultar datos por categoría para año corrido
    categorias_corrido = {}
    for categoria in categorias:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (dict_verificacion['exportaciones_nme_corrido'] == 'CON DATOS DE EXPORTACIONES NME CORRIDO'):
            # Construir la consulta SQL para obtener datos por categoría para año corrido
            query = f"""
                SELECT A.CATEGORIA, 
                        A.SUMA_USD_T_1, 
                        A.SUMA_USD_T, 
                        A.DIFERENCIA_PORCENTUAL 
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CORRIDO AS A
                WHERE A.AGRUPACION = '{AGRUPACION}' 
                    AND A.UNIDAD = '{UNIDAD}'
                    AND A.TABLA = '{categoria}'
                ORDER BY A.SUMA_USD_T DESC;
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query).collect())
            row_num = data.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
            data = data.head(5)
            # Filtrar los datos totales para 'No Mineras' y cambiar la categoría a 'Total'
            df_totales_nme = tipos['ST_CATEGORIAS_CORRIDO']
            df_totales_nme = df_totales_nme[df_totales_nme['CATEGORIA'] == 'No Mineras']
            df_totales_nme['CATEGORIA'] = 'Total'
            total_t = df_totales_nme['SUMA_USD_T'].sum()
            total_t_1 = df_totales_nme['SUMA_USD_T_1'].sum()
            # Calcular datos para la categoría 'Otros'
            otros_categoria = 'Otros'
            otros_t = total_t - data['SUMA_USD_T'].sum()
            otros_t_1 = total_t_1 - data['SUMA_USD_T_1'].sum()
            otros_porcentual = calcular_diferencia_porcentual(otros_t, otros_t_1)
            # Crear DataFrame para 'Otros'
            otros_df = pd.DataFrame({
                'CATEGORIA': [otros_categoria],
                'SUMA_USD_T_1': [otros_t_1],
                'SUMA_USD_T': [otros_t],
                'DIFERENCIA_PORCENTUAL': [otros_porcentual]
            })
            # Concatenar los datos de 'Otros' y los datos totales con el DataFrame original
            # Solo se concatena el otro, en caso de que exista esta categoria:
            if (row_num <= 5):
                data = pd.concat([data, df_totales_nme])
            else:
                data = pd.concat([data, otros_df, df_totales_nme])
            # Calcular la participación de cada categoría en el total de exportaciones
            data = calcular_participacion_porcentual(data, 'SUMA_USD_T', total_t, 'PARTICIPACION_T')
            # Almacenar el DataFrame en el diccionario 'categorias_corrido' con la categoría como clave
            categorias_corrido[categoria] = data

    # 6. Consultar información de empresas
    empresas = {}
    for tabla in tablas_nit_empresas:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (tabla == 'ST_NIT_CERRADO' and dict_verificacion['exportaciones_empresas_cerrado'] == 'CON DATOS DE EMPRESAS CERRADO') or \
           (tabla == 'ST_NIT_CORRIDO' and dict_verificacion['exportaciones_empresas_corrido'] == 'CON DATOS DE EMPRESAS CORRIDO'):
            # Construir la consulta SQL para obtener información de empresas
            query = f"""
                SELECT A.CATEGORIA, 
                        A.RAZON_SOCIAL,
                        A.SECTOR_ESTRELLA,
                        A.SUMA_USD_T_1, 
                        A.SUMA_USD_T, 
                        A.DIFERENCIA_PORCENTUAL
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A 
                WHERE A.AGRUPACION = '{AGRUPACION}' 
                    AND A.UNIDAD = '{UNIDAD}'
                ORDER BY SUMA_USD_T DESC;
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query).collect())
            row_num = data.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
            data = data.head(5)
            # Filtrar y cambiar la categoría a 'Total' para los datos totales de 'No Mineras'
            if tabla == 'ST_NIT_CERRADO':
                df_totales_nme = tipos['ST_CATEGORIAS_CERRADO']
            elif tabla == 'ST_NIT_CORRIDO':
                df_totales_nme = tipos['ST_CATEGORIAS_CORRIDO']
            df_totales_nme = df_totales_nme[df_totales_nme['CATEGORIA'] == 'No Mineras']
            df_totales_nme['CATEGORIA'] = 'Total'
            total_t = df_totales_nme['SUMA_USD_T'].sum()
            total_t_1 = df_totales_nme['SUMA_USD_T_1'].sum()
            df_totales_nme['RAZON_SOCIAL'] = 'No aplica'
            df_totales_nme['SECTOR_ESTRELLA'] = 'No aplica'
            # Calcular datos para la categoría 'Otros'
            otros_categoria = 'Otros'
            otros_razon_social = 'No aplica'
            otros_sector_estrella = 'No aplica'
            otros_t = total_t - data['SUMA_USD_T'].sum()
            otros_t_1 = total_t_1 - data['SUMA_USD_T_1'].sum()
            otros_porcentual = calcular_diferencia_porcentual(otros_t, otros_t_1)
            # Crear DataFrame para 'Otros'
            otros_df = pd.DataFrame({
                'CATEGORIA': [otros_categoria],
                'RAZON_SOCIAL': [otros_razon_social],
                'SECTOR_ESTRELLA': [otros_sector_estrella],       
                'SUMA_USD_T_1': [otros_t_1],
                'SUMA_USD_T': [otros_t],
                'DIFERENCIA_PORCENTUAL': [otros_porcentual]
            })
            # Concatenar los datos de 'Otros' y los datos totales con el DataFrame original
            # Solo se concatena el otro, en caso de que exista esta categoria:
            if (row_num <= 5):
                data = pd.concat([data, df_totales_nme])
            else:
                data = pd.concat([data, otros_df, df_totales_nme])
            # Calcular la participación de cada empresa en el total de exportaciones
            data = calcular_participacion_porcentual(data, 'SUMA_USD_T', total_t, 'PARTICIPACION_T')
            # Almacenar el DataFrame en el diccionario 'empresas' con el nombre de la tabla como clave
            empresas[tabla] = data

    # 7. Contar el número de empresas únicas por año
    conteo = {}
    # Inicializar el diccionario de resumen
    datos_resumen['CONTEO'] = {}

    # Consultar el conteo de empresas para año cerrado
    # Verificar el diccionario de verificación antes de ejecutar la consulta
    if (dict_verificacion['exportaciones_conteo_cerrado'] == 'CON DATOS DE CONTEO CERRADO'):
        query_cerrado = f"""
            SELECT A.NIT_EXPORTADOR, A.YEAR
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CONTEO_EMPRESAS_CERRADO AS A
            WHERE A.AGRUPACION = '{AGRUPACION}' 
                AND A.UNIDAD = '{UNIDAD}'
                AND A.VALOR_USD > {UMBRAL}
            ORDER BY A.YEAR ASC;
        """
        # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
        data_cerrado = pd.DataFrame(session.sql(query_cerrado).collect())
        # Contar el número de empresas únicas por año
        conteo_cerrado = data_cerrado.groupby('YEAR')['NIT_EXPORTADOR'].nunique()
        # Almacenar el resultado en el diccionario 'conteo' bajo la clave 'CERRADO'
        conteo['CERRADO'] = conteo_cerrado
        # Inicializar el diccionario de resumen
        datos_resumen['CONTEO'] = {}
        # Agregar datos de 'CERRADO'
        datos_resumen['CONTEO']['CERRADO'] = conteo_cerrado.to_dict()

    # Consultar el conteo de empresas para año corrido
    # Verificar el diccionario de verificación antes de ejecutar la consulta
    if (dict_verificacion['exportaciones_conteo_corrido'] == 'CON DATOS DE CONTEO CORRIDO'):
        query_corrido = f"""
            SELECT A.NIT_EXPORTADOR, A.YEAR
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CONTEO_EMPRESAS_CORRIDO AS A
            WHERE A.AGRUPACION = '{AGRUPACION}' 
                AND A.UNIDAD = '{UNIDAD}'
                AND A.VALOR_USD > {UMBRAL}
            ORDER BY A.YEAR ASC;
        """
        # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
        data_corrido = pd.DataFrame(session.sql(query_corrido).collect())
        # Contar el número de empresas únicas por año
        conteo_corrido = data_corrido.groupby('YEAR')['NIT_EXPORTADOR'].nunique()
        # Almacenar el resultado en el diccionario 'conteo' bajo la clave 'CORRIDO'
        conteo['CORRIDO'] = conteo_corrido

        # Agregar datos de 'CORRIDO'
        datos_resumen['CONTEO']['CORRIDO'] = conteo_corrido.to_dict()

    # 8. Consultar los totales de exportaciones en peso
    totales_peso = {}
    for tabla in tablas_peso:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (tabla == 'ST_CATEGORIAS_PESO_CERRADO' and dict_verificacion['exportaciones_totales_cerrado'] == 'CON DATOS DE EXPORTACIONES TOTALES CERRADO') or \
           (tabla == 'ST_CATEGORIAS_PESO_CORRIDO' and dict_verificacion['exportaciones_totales_corrido'] == 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO'):
            # Construir la consulta SQL para obtener los totales de exportaciones en peso
            query_totales = f"""
                SELECT 'Total' AS CATEGORIA, 
                        A.SUMA_PESO_T_1, 
                        A.SUMA_PESO_T, 
                        A.DIFERENCIA_PORCENTUAL 
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                WHERE A.AGRUPACION = '{AGRUPACION}'
                    AND A.UNIDAD = '{UNIDAD}'
                    AND A.TABLA = 'TOTAL';
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query_totales).collect())
            # Almacenar el DataFrame en el diccionario 'totales_peso' con el nombre de la tabla como clave
            totales_peso[tabla] = data

    # 9. Consultar los tipos de exportaciones en peso
    tipos_peso = {}
    for tabla in tablas_peso:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (tabla == 'ST_CATEGORIAS_PESO_CERRADO' and dict_verificacion['exportaciones_totales_cerrado'] == 'CON DATOS DE EXPORTACIONES TOTALES CERRADO') or \
           (tabla == 'ST_CATEGORIAS_PESO_CORRIDO' and dict_verificacion['exportaciones_totales_corrido'] == 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO'):
            # Construir la consulta SQL para obtener los tipos de exportaciones en peso
            query_totales = f"""
                SELECT A.CATEGORIA, 
                        A.SUMA_PESO_T_1, 
                        A.SUMA_PESO_T, 
                        A.DIFERENCIA_PORCENTUAL 
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                WHERE A.AGRUPACION = '{AGRUPACION}'
                    AND A.UNIDAD = '{UNIDAD}'
                    AND A.TABLA = 'TIPOS';
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query_totales).collect())
            # Calcular el total de exportaciones en peso para agregar participación
            total_t = totales_peso[tabla]['SUMA_PESO_T'].sum()
            # Concatenar los datos de tipos con los totales
            data = pd.concat([data, totales_peso[tabla]])
            # Calcular la participación de cada tipo en el total de exportaciones en peso
            data = calcular_participacion_porcentual(data, 'SUMA_PESO_T', total_t, 'PARTICIPACION_T')
            # Almacenar el DataFrame en el diccionario 'tipos_peso' con el nombre de la tabla como clave
            tipos_peso[tabla] = data
            # Agregar los datos al diccionario de resumen
            # Inicializar la entrada en el diccionario de resumen para la tabla actual
            if tabla not in datos_resumen:
                datos_resumen[tabla] = {}
            # Recorrer las filas del DataFrame y almacenar los valores en el diccionario
            for index, row in data.iterrows():
                categoria = row['CATEGORIA']
                if categoria not in datos_resumen[tabla]:
                    datos_resumen[tabla][categoria] = []
                datos_resumen[tabla][categoria].append({
                    'sum_peso_t_1': row['SUMA_PESO_T_1'],
                    'sum_peso_t': row['SUMA_PESO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                    'participacion_t': row['PARTICIPACION_T']
                })

    # 9.1 Pesos por medio de transporte: Mineras
    medios_peso_minero = {}
    for tabla in tablas_peso:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (tabla == 'ST_CATEGORIAS_PESO_CERRADO' and dict_verificacion['pesos_minero_cerrado'] == 'CON DATOS CERRADO') or \
        (tabla == 'ST_CATEGORIAS_PESO_CORRIDO' and dict_verificacion['pesos_minero_corrido'] == 'CON DATOS CORRIDO'):
            # Construir la consulta SQL para obtener los tipos de exportaciones por medio de transporte
            query_peso_medios_mineros = f"""
                SELECT A.CATEGORIA, 
                        A.SUMA_PESO_T_1, 
                        A.SUMA_PESO_T 
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                WHERE A.AGRUPACION = '{AGRUPACION}'
                    AND A.UNIDAD = '{UNIDAD}'
                    AND A.TABLA = 'MEDIO MINERAS';
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query_peso_medios_mineros).collect())
            # Calcular el total de exportaciones en peso para agregar participación
            total_t_1 = data['SUMA_PESO_T_1'].sum()
            total_t = data['SUMA_PESO_T'].sum()
            total_categoria = 'Total'
            total_df = pd.DataFrame({
                'CATEGORIA': [total_categoria],
                'SUMA_PESO_T_1': [total_t_1],
                'SUMA_PESO_T': [total_t]})
            # Concatenar los datos de tipos con los totales
            data = pd.concat([data, total_df])
            # Calcular la participación de cada tipo en el total de exportaciones en peso
            data = calcular_participacion_porcentual(data, 'SUMA_PESO_T_1', total_t_1, 'PARTICIPACION_T_1')
            data = calcular_participacion_porcentual(data, 'SUMA_PESO_T', total_t, 'PARTICIPACION_T')
            # Almacenar el DataFrame en el diccionario 'tipos_peso' con el nombre de la tabla como clave
            medios_peso_minero[tabla] = data
        
    # 9.2 Pesos por medio no mineros
    medios_peso_no_minero = {}
    for tabla in tablas_peso:
        # Verificar el diccionario de verificación antes de ejecutar la consulta
        if (tabla == 'ST_CATEGORIAS_PESO_CERRADO' and dict_verificacion['pesos_no_minero_cerrado'] == 'CON DATOS CERRADO') or \
        (tabla == 'ST_CATEGORIAS_PESO_CORRIDO' and dict_verificacion['pesos_no_minero_corrido'] == 'CON DATOS CORRIDO'):
            # Construir la consulta SQL para obtener los tipos de exportaciones por medio de transporte
            query_peso_medios_no_mineros = f"""
                SELECT A.CATEGORIA, 
                        A.SUMA_PESO_T_1, 
                        A.SUMA_PESO_T
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_PESO_CORRIDO AS A 
                WHERE A.AGRUPACION = '{AGRUPACION}'
                    AND A.UNIDAD = '{UNIDAD}'
                    AND A.TABLA = 'MEDIO NO MINERAS';
            """
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            data = pd.DataFrame(session.sql(query_peso_medios_no_mineros).collect())
            # Calcular el total de exportaciones en peso para agregar participación
            total_t_1 = data['SUMA_PESO_T_1'].sum()
            total_t = data['SUMA_PESO_T'].sum()
            total_categoria = 'Total'
            total_df = pd.DataFrame({
                'CATEGORIA': [total_categoria],
                'SUMA_PESO_T_1': [total_t_1],
                'SUMA_PESO_T': [total_t]})
            # Concatenar los datos de tipos con los totales
            data = pd.concat([data, total_df])
            # Calcular la participación de cada tipo en el total de exportaciones en peso
            data = calcular_participacion_porcentual(data, 'SUMA_PESO_T_1', total_t_1, 'PARTICIPACION_T_1')
            data = calcular_participacion_porcentual(data, 'SUMA_PESO_T', total_t, 'PARTICIPACION_T')
            # Almacenar el DataFrame en el diccionario 'tipos_peso' con el nombre de la tabla como clave
            medios_peso_no_minero[tabla] = data

    ###########
    # INVERSIÓN
    ###########

    # Diccionarios para resultados
    # Actividades de Colombia 
    ied_colombia_actividades = {}
    # Países
    ied_paises = {}
    ice_paises = {}
    ied_total = {}
    ice_total = {}


    # Los datos de actividades solo son válidos para la agrupación de Colombia:
    if AGRUPACION == 'COLOMBIA':
        # IED NME ACTIVIDADES
        # Construir consulta de actividades año cerrado
        query_actividades_ied_cerrado = """
        SELECT A.UNIDAD,
            A.SUMA_INVERSION_T_1,
            A.SUMA_INVERSION_T,
            A.DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_ACTIVIDADES_CERRADO AS A
        WHERE A.AGRUPACION = 'ACTIVIDADES'
            AND A.UNIDAD NOT IN ('TOTAL')
            AND A.UNIDAD IN ('Servicios financieros y empresariales',
                            'Industrias manufactureras',
                            'Comercio al por mayor y al por menor, restaurantes y hoteles',
                            'Transportes, almacenamiento y comunicaciones',
                            'Electricidad, gas y agua',
                            'Servicios comunales sociales y personales',
                            'Construcción',
                            'Agricultura, caza, silvicultura y pesca')
            AND A.TABLA = 'INVERSIÓN ACTIVIDADES'
            AND A.CATEGORIA = 'IED';
        """ 
        # Construir consulta de actividades año corrido
        query_actividades_ied_corrido = """
                SELECT A.UNIDAD,
            A.SUMA_INVERSION_T_1,
            A.SUMA_INVERSION_T,
            A.DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_ACTIVIDADES_CORRIDO AS A
        WHERE A.AGRUPACION = 'ACTIVIDADES'
            AND A.UNIDAD NOT IN ('TOTAL')
            AND A.UNIDAD IN ('Servicios financieros y empresariales',
                            'Industrias manufactureras',
                            'Comercio al por mayor y al por menor, restaurantes y hoteles',
                            'Transportes, almacenamiento y comunicaciones',
                            'Electricidad, gas y agua',
                            'Servicios comunales sociales y personales',
                            'Construcción',
                            'Agricultura, caza, silvicultura y pesca')
            AND A.TABLA = 'INVERSIÓN ACTIVIDADES'
            AND A.CATEGORIA = 'IED';
        """
        # Año cerrado
        # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
        ied_actividades_cerrado = pd.DataFrame(session.sql(query_actividades_ied_cerrado).collect())
        ied_actividades_cerrado_totales_unidad = 'Total'
        ied_actividades_cerrado_totales_t_1 = ied_actividades_cerrado['SUMA_INVERSION_T_1'].sum()
        ied_actividades_cerrado_totales_t = ied_actividades_cerrado['SUMA_INVERSION_T'].sum()
        ied_actividades_cerrado_totales_diferencia_porcentual = calcular_diferencia_porcentual(ied_actividades_cerrado_totales_t, ied_actividades_cerrado_totales_t_1)

        # Crear el dataframe
        ied_actividades_cerrado_totales = pd.DataFrame({
            'UNIDAD': [ied_actividades_cerrado_totales_unidad],
            'SUMA_INVERSION_T_1': [ied_actividades_cerrado_totales_t_1],
            'SUMA_INVERSION_T': [ied_actividades_cerrado_totales_t],
            'DIFERENCIA_PORCENTUAL_T': [ied_actividades_cerrado_totales_diferencia_porcentual]
        })
        # Crear el dataframe consolidado de actividades
        ied_actividades_cerrado = pd.concat([ied_actividades_cerrado, ied_actividades_cerrado_totales])
        # Calcular participación en T 
        ied_actividades_cerrado = calcular_participacion_porcentual(ied_actividades_cerrado, 'SUMA_INVERSION_T', ied_actividades_cerrado_totales_t, 'PARTICIPACION_T')
        # Almacenar el DataFrame en el diccionario 'tipos_peso' con el nombre de la tabla como clave
        ied_colombia_actividades['ied_cerrado'] = ied_actividades_cerrado
        # Agregar los datos al diccionario de resumen
        # Inicializar la entrada en el diccionario de resumen para la tabla actual
        datos_resumen['IED CERRADO ACTIVIDADES'] = {}
        # Recorrer las filas del DataFrame y almacenar los valores en el diccionario
        for index, row in ied_actividades_cerrado.iterrows():
            unidad = row['UNIDAD']
            if unidad not in datos_resumen['IED CERRADO ACTIVIDADES']:
                datos_resumen['IED CERRADO ACTIVIDADES'][unidad] = []
            datos_resumen['IED CERRADO ACTIVIDADES'][unidad].append({
                'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                'sum_inversion_t': row['SUMA_INVERSION_T'],
                'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T'],
                'participacion_t': row['PARTICIPACION_T']
            })
    
        # Año corrido
        # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
        ied_actividades_corrido = pd.DataFrame(session.sql(query_actividades_ied_corrido).collect())
        ied_actividades_corrido_totales_unidad = 'Total'
        ied_actividades_corrido_totales_t_1 = ied_actividades_corrido['SUMA_INVERSION_T_1'].sum()
        ied_actividades_corrido_totales_t = ied_actividades_corrido['SUMA_INVERSION_T'].sum()
        ied_actividades_corrido_totales_diferencia_porcentual = calcular_diferencia_porcentual(ied_actividades_corrido_totales_t, ied_actividades_corrido_totales_t_1)

        # Crear el dataframe
        ied_actividades_corrido_totales = pd.DataFrame({
            'UNIDAD': [ied_actividades_corrido_totales_unidad],
            'SUMA_INVERSION_T_1': [ied_actividades_corrido_totales_t_1],
            'SUMA_INVERSION_T': [ied_actividades_corrido_totales_t],
            'DIFERENCIA_PORCENTUAL': [ied_actividades_corrido_totales_diferencia_porcentual]
        })
        # Crear el dataframe consolidado de actividades
        ied_actividades_corrido = pd.concat([ied_actividades_corrido, ied_actividades_corrido_totales])
        # Calcular participación en T 
        ied_actividades_corrido = calcular_participacion_porcentual(ied_actividades_corrido, 'SUMA_INVERSION_T', ied_actividades_corrido_totales_t, 'PARTICIPACION_T')
        # Almacenar el DataFrame en el diccionario 'tipos_peso' con el nombre de la tabla como clave
        ied_colombia_actividades['ied_corrido'] = ied_actividades_corrido
        # Agregar los datos al diccionario de resumen
        # Inicializar la entrada en el diccionario de resumen para la tabla actual
        datos_resumen['IED CORRIDO ACTIVIDADES'] = {}
        # Recorrer las filas del DataFrame y almacenar los valores en el diccionario
        for index, row in ied_actividades_corrido.iterrows():
            unidad = row['UNIDAD']
            if unidad not in datos_resumen['IED CORRIDO ACTIVIDADES']:
                datos_resumen['IED CORRIDO ACTIVIDADES'][unidad] = []
            datos_resumen['IED CORRIDO ACTIVIDADES'][unidad].append({
                'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                'sum_inversion_t': row['SUMA_INVERSION_T'],
                'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                'participacion_t': row['PARTICIPACION_T']
            })

    # IED por países válido para las agrupaciones de 'CONTINENTES', 'HUBS', 'TLCS', 'PAISES' y 'COLOMBIA'
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        # Construir consulta de paises año cerrado
        query_paises_ied_cerrado = f"""
        SELECT A.UNIDAD,
            A.SUMA_INVERSION_T_1,
            A.SUMA_INVERSION_T,
            A.DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
        WHERE A.AGRUPACION = 'PAISES'
            AND A.UNIDAD NOT IN ('TOTAL')
            AND A.CATEGORIA = 'IED'
        """
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_paises_ied_cerrado += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"
        query_paises_ied_cerrado += f" ORDER BY A.SUMA_INVERSION_T DESC;"

        # Construir consulta de totales año cerrado
        if AGRUPACION == 'COLOMBIA':
            query_paises_ied_totales_cerrado = """
            SELECT A.UNIDAD,
                A.SUMA_INVERSION_T_1,
                A.SUMA_INVERSION_T,
                A.DIFERENCIA_PORCENTUAL_T
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD IN ('TOTAL')
                AND A.CATEGORIA = 'IED';
            """ 
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
            query_paises_ied_totales_cerrado = f"""
            SELECT 'TOTAL' AS UNIDAD,
                SUM(A.SUMA_INVERSION_T_1) AS SUMA_INVERSION_T_1,
                SUM(A.SUMA_INVERSION_T) AS SUMA_INVERSION_T,
                CASE 
                    WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) > 0 THEN 100
                    WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) = 0 THEN 0
                    WHEN SUM(A.SUMA_INVERSION_T) = 0 AND SUM(A.SUMA_INVERSION_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_INVERSION_T) - SUM(A.SUMA_INVERSION_T_1)) / SUM(A.SUMA_INVERSION_T_1)) * 100
                END AS DIFERENCIA_PORCENTUAL_T
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.CATEGORIA = 'IED'
            """
            query_paises_ied_totales_cerrado += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql});" 

        # Construir consulta para participación de agrupaciones sobre la IED total
        query_ied_totales_cerrado = """
            SELECT 'Total IED del Mundo en Colombia' AS UNIDAD,
                A.SUMA_INVERSION_T_1,
                A.SUMA_INVERSION_T,
                A.DIFERENCIA_PORCENTUAL_T
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD IN ('TOTAL')
                AND A.CATEGORIA = 'IED';
            """  


        # Construir consulta de paises año corrido
        query_paises_ied_corrido = """
        SELECT A.UNIDAD,
            A.SUMA_INVERSION_T_1,
            A.SUMA_INVERSION_T,
            A.DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
        WHERE A.AGRUPACION = 'PAISES'
            AND A.UNIDAD NOT IN ('TOTAL')
            AND A.CATEGORIA = 'IED'
        """
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_paises_ied_corrido += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"
        query_paises_ied_corrido += f" ORDER BY A.SUMA_INVERSION_T DESC;"

        # Construir consulta de totales año corrido
        if AGRUPACION == 'COLOMBIA':
            query_paises_ied_totales_corrido = """
            SELECT A.UNIDAD,
                A.SUMA_INVERSION_T_1,
                A.SUMA_INVERSION_T,
                A.DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD IN ('TOTAL')
                AND A.CATEGORIA = 'IED';
            """
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
            query_paises_ied_totales_corrido = """
            SELECT 'TOTAL' AS UNIDAD,
                SUM(A.SUMA_INVERSION_T_1) AS SUMA_INVERSION_T_1,
                SUM(A.SUMA_INVERSION_T) AS SUMA_INVERSION_T,
                CASE 
                    WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) > 0 THEN 100
                    WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) = 0 THEN 0
                    WHEN SUM(A.SUMA_INVERSION_T) = 0 AND SUM(A.SUMA_INVERSION_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_INVERSION_T) - SUM(A.SUMA_INVERSION_T_1)) / SUM(A.SUMA_INVERSION_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.CATEGORIA = 'IED'
            """
            query_paises_ied_totales_corrido += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql});" 

        # Construir consulta para participación de agrupaciones sobre la IED total
        query_ied_totales_corrido = """
            SELECT 'Total IED del Mundo en Colombia' AS UNIDAD,
                A.SUMA_INVERSION_T_1,
                A.SUMA_INVERSION_T,
                A.DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD IN ('TOTAL')
                AND A.CATEGORIA = 'IED';
        """

        # Ejecutar las consultas solo si hay datos:
        if dict_verificacion['ied_cerrado'] == 'CON DATOS DE IED CERRADO':
            # Año cerrado
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            ied_paises_cerrado = pd.DataFrame(session.sql(query_paises_ied_cerrado).collect())
            # Calcular tamaño del df original para agregar otros en los casos que sea necesario
            row_num_ied_paises_cerrado = ied_paises_cerrado.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
            ied_paises_cerrado = ied_paises_cerrado.head(5)

            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                ied_paises_cerrado_total = pd.DataFrame(session.sql(query_paises_ied_totales_cerrado).collect())
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                ied_cerrado_total = pd.DataFrame(session.sql(query_ied_totales_cerrado).collect())
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                # Crear otros
                ied_paises_cerrado_unidad = 'Otros'
                ied_paises_cerrado_t_1 = ied_paises_cerrado_total['SUMA_INVERSION_T_1'].sum() - ied_paises_cerrado['SUMA_INVERSION_T_1'].sum()
                ied_paises_cerrado_t = ied_paises_cerrado_total['SUMA_INVERSION_T'].sum() - ied_paises_cerrado['SUMA_INVERSION_T'].sum()
                ied_paises_cerrado_diferencia_porcentual = calcular_diferencia_porcentual(ied_paises_cerrado_t, ied_paises_cerrado_t_1)
                # Crear DataFrame para 'Otros'
                otros_df = pd.DataFrame({
                    'UNIDAD': [ied_paises_cerrado_unidad],
                    'SUMA_INVERSION_T_1' : [ied_paises_cerrado_t_1],
                    'SUMA_INVERSION_T' : [ied_paises_cerrado_t],
                    'DIFERENCIA_PORCENTUAL_T' : [ied_paises_cerrado_diferencia_porcentual]
                })
            # Concatenar los datos de 'Otros' y los datos totales con el DataFrame original según agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                if (row_num_ied_paises_cerrado <= 5):
                    ied_paises_cerrado_otros_totales = pd.concat([ied_paises_cerrado, ied_paises_cerrado_total])
                else:
                    ied_paises_cerrado_otros_totales = pd.concat([ied_paises_cerrado, otros_df, ied_paises_cerrado_total])
            if AGRUPACION in ['PAISES']:
                ied_paises_cerrado_otros_totales = pd.concat([ied_paises_cerrado])
            # Calcular la participación de cada categoría en el total de exportaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                total_t = ied_paises_cerrado_total['SUMA_INVERSION_T'].sum()
                ied_paises_cerrado_otros_totales = calcular_participacion_porcentual(ied_paises_cerrado_otros_totales, 'SUMA_INVERSION_T', total_t, 'PARTICIPACION_T')
            ied_paises['ied_cerrado'] = ied_paises_cerrado_otros_totales
            # Datos de resumen por agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                if AGRUPACION == 'PAISES':
                    ied_cerrado_agrupaciones = pd.concat([ied_paises_cerrado, ied_cerrado_total])
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
                    ied_cerrado_agrupaciones = pd.concat([ied_paises_cerrado_total, ied_cerrado_total])
                total_t_ied = ied_cerrado_total['SUMA_INVERSION_T'].sum()
                ied_cerrado_agrupaciones = calcular_participacion_porcentual(ied_cerrado_agrupaciones, 'SUMA_INVERSION_T', total_t_ied, 'PARTICIPACION_T')
                ied_total['ied_cerrado_total'] = ied_cerrado_agrupaciones

            # Agregar los datos al diccionario de resumen
            datos_resumen['IED CERRADO PAISES'] = {}
            for index, row in ied_paises_cerrado_otros_totales.iterrows():
                unidad = row['UNIDAD']
                if unidad not in datos_resumen['IED CERRADO PAISES']:
                    datos_resumen['IED CERRADO PAISES'][unidad] = []          
                entry = {
                    'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                    'sum_inversion_t': row['SUMA_INVERSION_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T']
                }            
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                    entry['participacion_t'] = row['PARTICIPACION_T']            
                datos_resumen['IED CERRADO PAISES'][unidad].append(entry)

            # Agregar los datos al diccionario de resumen por agrupaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']: 
                datos_resumen['IED CERRADO TOTAL'] = {}
                for index, row in ied_cerrado_agrupaciones.iterrows():
                    unidad = row['UNIDAD']
                    if unidad not in datos_resumen['IED CERRADO TOTAL']:
                        datos_resumen['IED CERRADO TOTAL'][unidad] = []          
                    entry = {
                        'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                        'sum_inversion_t': row['SUMA_INVERSION_T'],
                        'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T'],
                        'participacion_t' : row['PARTICIPACION_T']
                    }            
                    datos_resumen['IED CERRADO TOTAL'][unidad].append(entry)

        # Ejecutar las consultas solo si hay datos:
        if dict_verificacion['ied_corrido'] == 'CON DATOS DE IED CORRIDO':
            # Año corrido
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            ied_paises_corrido = pd.DataFrame(session.sql(query_paises_ied_corrido).collect())
            # Calcular tamaño del df original para agregar otros en los casos que sea necesario
            row_num_ied_paises_corrido = ied_paises_corrido.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
            ied_paises_corrido = ied_paises_corrido.head(5)
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                ied_paises_corrido_total = pd.DataFrame(session.sql(query_paises_ied_totales_corrido).collect())
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                ied_corrido_total = pd.DataFrame(session.sql(query_ied_totales_corrido).collect())
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                # Crear otros
                ied_paises_corrido_unidad = 'Otros'
                ied_paises_corrido_t_1 = ied_paises_corrido_total['SUMA_INVERSION_T_1'].sum() - ied_paises_corrido['SUMA_INVERSION_T_1'].sum()
                ied_paises_corrido_t = ied_paises_corrido_total['SUMA_INVERSION_T'].sum() - ied_paises_corrido['SUMA_INVERSION_T'].sum()
                ied_paises_corrido_diferencia_porcentual = calcular_diferencia_porcentual(ied_paises_corrido_t, ied_paises_corrido_t_1)
                # Crear DataFrame para 'Otros'
                otros_df = pd.DataFrame({
                    'UNIDAD': [ied_paises_corrido_unidad],
                    'SUMA_INVERSION_T_1' : [ied_paises_corrido_t_1],
                    'SUMA_INVERSION_T' : [ied_paises_corrido_t],
                    'DIFERENCIA_PORCENTUAL' : [ied_paises_corrido_diferencia_porcentual]
                })
            # Concatenar los datos de 'Otros' y los datos totales con el DataFrame original según agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                if (row_num_ied_paises_corrido <= 5):
                    ied_paises_corrido_otros_totales = pd.concat([ied_paises_corrido, ied_paises_corrido_total])
                else: 
                    ied_paises_corrido_otros_totales = pd.concat([ied_paises_corrido, otros_df, ied_paises_corrido_total])
            if AGRUPACION in ['PAISES']:
                ied_paises_corrido_otros_totales = pd.concat([ied_paises_corrido])
            # Calcular la participación de cada categoría en el total de exportaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                total_t = ied_paises_corrido_total['SUMA_INVERSION_T'].sum()
                ied_paises_corrido_otros_totales = calcular_participacion_porcentual(ied_paises_corrido_otros_totales, 'SUMA_INVERSION_T', total_t, 'PARTICIPACION_T')
            ied_paises['ied_corrido'] = ied_paises_corrido_otros_totales
            # Datos de resumen por agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                if AGRUPACION == 'PAISES':
                    ied_corrido_agrupaciones = pd.concat([ied_paises_corrido, ied_corrido_total])
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
                    ied_corrido_agrupaciones = pd.concat([ied_paises_corrido_total, ied_corrido_total])
                total_t_ied = ied_corrido_total['SUMA_INVERSION_T'].sum()
                ied_corrido_agrupaciones = calcular_participacion_porcentual(ied_corrido_agrupaciones, 'SUMA_INVERSION_T', total_t_ied, 'PARTICIPACION_T')
                ied_total['ied_corrido_total'] = ied_corrido_agrupaciones

            # Agregar los datos al diccionario de resumen
            datos_resumen['IED CORRIDO PAISES'] = {}
            for index, row in ied_paises_corrido_otros_totales.iterrows():
                unidad = row['UNIDAD']
                if unidad not in datos_resumen['IED CORRIDO PAISES']:
                    datos_resumen['IED CORRIDO PAISES'][unidad] = []
                
                entry = {
                    'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                    'sum_inversion_t': row['SUMA_INVERSION_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL']
                }
                
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                    entry['participacion_t'] = row['PARTICIPACION_T']
                
                datos_resumen['IED CORRIDO PAISES'][unidad].append(entry)
            # Agregar los datos al diccionario de resumen por agrupaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']: 
                datos_resumen['IED CORRIDO TOTAL'] = {}
                for index, row in ied_corrido_agrupaciones.iterrows():
                    unidad = row['UNIDAD']
                    if unidad not in datos_resumen['IED CORRIDO TOTAL']:
                        datos_resumen['IED CORRIDO TOTAL'][unidad] = []               
                    entry = {
                        'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                        'sum_inversion_t': row['SUMA_INVERSION_T'],
                        'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                        'participacion_t' : row['PARTICIPACION_T']
                    }
                    datos_resumen['IED CORRIDO TOTAL'][unidad].append(entry)

    
    # ICE por países válido para las agrupaciones de 'CONTINENTES', 'HUBS', 'TLCS', 'PAISES' y 'COLOMBIA'
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        # Construir consulta de paises año cerrado
        query_paises_ice_cerrado = f"""
        SELECT A.UNIDAD,
            A.SUMA_INVERSION_T_1,
            A.SUMA_INVERSION_T,
            A.DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
        WHERE A.AGRUPACION = 'PAISES'
            AND A.UNIDAD NOT IN ('TOTAL')
            AND A.CATEGORIA = 'ICE'
        """
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_paises_ice_cerrado += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"
        query_paises_ice_cerrado += f" ORDER BY A.SUMA_INVERSION_T DESC;"

        # Construir consulta de totales año cerrado
        if AGRUPACION == 'COLOMBIA':
            query_paises_ice_totales_cerrado = """
            SELECT A.UNIDAD,
                A.SUMA_INVERSION_T_1,
                A.SUMA_INVERSION_T,
                A.DIFERENCIA_PORCENTUAL_T
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD IN ('TOTAL')
                AND A.CATEGORIA = 'ICE';
            """ 
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
            query_paises_ice_totales_cerrado = f"""
            SELECT 'TOTAL' AS UNIDAD,
                SUM(A.SUMA_INVERSION_T_1) AS SUMA_INVERSION_T_1,
                SUM(A.SUMA_INVERSION_T) AS SUMA_INVERSION_T,
                CASE 
                    WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) > 0 THEN 100
                    WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) = 0 THEN 0
                    WHEN SUM(A.SUMA_INVERSION_T) = 0 AND SUM(A.SUMA_INVERSION_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_INVERSION_T) - SUM(A.SUMA_INVERSION_T_1)) / SUM(A.SUMA_INVERSION_T_1)) * 100
                END AS DIFERENCIA_PORCENTUAL_T
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.CATEGORIA = 'ICE'
            """
            query_paises_ice_totales_cerrado += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql});" 

        # Construir consulta para participación de agrupaciones sobre la IED total
        query_ice_totales_cerrado = """
            SELECT 'Total ICE de Colombia en el Mundo' AS UNIDAD,
                A.SUMA_INVERSION_T_1,
                A.SUMA_INVERSION_T,
                A.DIFERENCIA_PORCENTUAL_T
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD IN ('TOTAL')
                AND A.CATEGORIA = 'ICE';
            """ 


        # Construir consulta de paises año corrido
        query_paises_ice_corrido = """
        SELECT A.UNIDAD,
            A.SUMA_INVERSION_T_1,
            A.SUMA_INVERSION_T,
            A.DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
        WHERE A.AGRUPACION = 'PAISES'
            AND A.UNIDAD NOT IN ('TOTAL')
            AND A.CATEGORIA = 'ICE'
        """
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            query_paises_ice_corrido += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"
        query_paises_ice_corrido += f" ORDER BY A.SUMA_INVERSION_T DESC;"

        # Construir consulta de totales año corrido
        if AGRUPACION == 'COLOMBIA':
            query_paises_ice_totales_corrido = """
            SELECT A.UNIDAD,
                A.SUMA_INVERSION_T_1,
                A.SUMA_INVERSION_T,
                A.DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD IN ('TOTAL')
                AND A.CATEGORIA = 'ICE';
            """
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
            query_paises_ice_totales_corrido = """
            SELECT 'TOTAL' AS UNIDAD,
                SUM(A.SUMA_INVERSION_T_1) AS SUMA_INVERSION_T_1,
                SUM(A.SUMA_INVERSION_T) AS SUMA_INVERSION_T,
                CASE 
                    WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) > 0 THEN 100
                    WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) = 0 THEN 0
                    WHEN SUM(A.SUMA_INVERSION_T) = 0 AND SUM(A.SUMA_INVERSION_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_INVERSION_T) - SUM(A.SUMA_INVERSION_T_1)) / SUM(A.SUMA_INVERSION_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.CATEGORIA = 'ICE'
            """
            query_paises_ice_totales_corrido += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql});"

        # Construir consulta para participación de agrupaciones sobre la IED total
        query_ice_totales_corrido = """
            SELECT 'Total ICE de Colombia en el Mundo' AS UNIDAD,
                A.SUMA_INVERSION_T_1,
                A.SUMA_INVERSION_T,
                A.DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO AS A
            WHERE A.AGRUPACION = 'PAISES'
                AND A.UNIDAD IN ('TOTAL')
                AND A.CATEGORIA = 'ICE';
            """

         # Ejecutar las consultas solo si hay datos:
        if dict_verificacion['ice_cerrado'] == 'CON DATOS DE ICE CERRADO':
            # Año cerrado
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            ice_paises_cerrado = pd.DataFrame(session.sql(query_paises_ice_cerrado).collect())
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            row_num_ice_paises_cerrado = ice_paises_cerrado.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
            ice_paises_cerrado = ice_paises_cerrado.head(5)
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                ice_paises_cerrado_total = pd.DataFrame(session.sql(query_paises_ice_totales_cerrado).collect())
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                ice_cerrado_total = pd.DataFrame(session.sql(query_ice_totales_cerrado).collect())
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                # Crear otros
                ice_paises_cerrado_unidad = 'Otros'
                ice_paises_cerrado_t_1 = ice_paises_cerrado_total['SUMA_INVERSION_T_1'].sum() - ice_paises_cerrado['SUMA_INVERSION_T_1'].sum()
                ice_paises_cerrado_t = ice_paises_cerrado_total['SUMA_INVERSION_T'].sum() - ice_paises_cerrado['SUMA_INVERSION_T'].sum()
                ice_paises_cerrado_diferencia_porcentual = calcular_diferencia_porcentual(ice_paises_cerrado_t, ice_paises_cerrado_t_1)
                # Crear DataFrame para 'Otros'
                otros_df = pd.DataFrame({
                    'UNIDAD': [ice_paises_cerrado_unidad],
                    'SUMA_INVERSION_T_1' : [ice_paises_cerrado_t_1],
                    'SUMA_INVERSION_T' : [ice_paises_cerrado_t],
                    'DIFERENCIA_PORCENTUAL_T' : [ice_paises_cerrado_diferencia_porcentual]
                })
            # Concatenar los datos de 'Otros' y los datos totales con el DataFrame original según agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                if (row_num_ice_paises_cerrado <= 5):     
                    ice_paises_cerrado_otros_totales = pd.concat([ice_paises_cerrado, ice_paises_cerrado_total])
                else:
                    ice_paises_cerrado_otros_totales = pd.concat([ice_paises_cerrado, otros_df, ice_paises_cerrado_total])
            if AGRUPACION in ['PAISES']:
                ice_paises_cerrado_otros_totales = pd.concat([ice_paises_cerrado])
            # Calcular la participación de cada categoría en el total de exportaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                total_t = ice_paises_cerrado_total['SUMA_INVERSION_T'].sum()
                ice_paises_cerrado_otros_totales = calcular_participacion_porcentual(ice_paises_cerrado_otros_totales, 'SUMA_INVERSION_T', total_t, 'PARTICIPACION_T')
            ice_paises['ice_cerrado'] = ice_paises_cerrado_otros_totales
            # Datos de resumen por agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                if AGRUPACION == 'PAISES':
                    ice_cerrado_agrupaciones = pd.concat([ice_paises_cerrado, ice_cerrado_total])
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
                    ice_cerrado_agrupaciones = pd.concat([ice_paises_cerrado_total, ice_cerrado_total])
                total_t_ice = ice_cerrado_total['SUMA_INVERSION_T'].sum()
                ice_cerrado_agrupaciones = calcular_participacion_porcentual(ice_cerrado_agrupaciones, 'SUMA_INVERSION_T', total_t_ice, 'PARTICIPACION_T')
                ice_total['ice_cerrado_total'] = ice_cerrado_agrupaciones

            # Agregar los datos al diccionario de resumen
            datos_resumen['ICE CERRADO PAISES'] = {}
            for index, row in ice_paises_cerrado_otros_totales.iterrows():
                unidad = row['UNIDAD']
                if unidad not in datos_resumen['ICE CERRADO PAISES']:
                    datos_resumen['ICE CERRADO PAISES'][unidad] = []
                
                entry = {
                    'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                    'sum_inversion_t': row['SUMA_INVERSION_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T']
                }
                
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                    entry['participacion_t'] = row['PARTICIPACION_T']
                
                datos_resumen['ICE CERRADO PAISES'][unidad].append(entry)

            # Agregar los datos al diccionario de resumen por agrupaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                datos_resumen['ICE CERRADO TOTAL'] = {}
                for index, row in ice_cerrado_agrupaciones.iterrows():
                    unidad = row['UNIDAD']
                    if unidad not in datos_resumen['ICE CERRADO TOTAL']:
                        datos_resumen['ICE CERRADO TOTAL'][unidad] = []
                    
                    entry = {
                        'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                        'sum_inversion_t': row['SUMA_INVERSION_T'],
                        'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T'],
                        'participacion_t' : row['PARTICIPACION_T']
                    }              
                    datos_resumen['ICE CERRADO TOTAL'][unidad].append(entry)


        # Ejecutar las consultas solo si hay datos:
        if dict_verificacion['ice_corrido'] == 'CON DATOS DE ICE CORRIDO':
            # Año corrido
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            ice_paises_corrido = pd.DataFrame(session.sql(query_paises_ice_corrido).collect())
            # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
            row_num_ice_paises_corrido = ice_paises_corrido.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
            ice_paises_corrido = ice_paises_corrido.head(5)
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                ice_paises_corrido_total = pd.DataFrame(session.sql(query_paises_ice_totales_corrido).collect())
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                ice_corrido_total = pd.DataFrame(session.sql(query_ice_totales_corrido).collect())
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                # Crear otros
                ice_paises_corrido_unidad = 'Otros'
                ice_paises_corrido_t_1 = ice_paises_corrido_total['SUMA_INVERSION_T_1'].sum() - ice_paises_corrido['SUMA_INVERSION_T_1'].sum()
                ice_paises_corrido_t = ice_paises_corrido_total['SUMA_INVERSION_T'].sum() - ice_paises_corrido['SUMA_INVERSION_T'].sum()
                ice_paises_corrido_diferencia_porcentual = calcular_diferencia_porcentual(ice_paises_corrido_t, ice_paises_corrido_t_1)
                # Crear DataFrame para 'Otros'
                otros_df = pd.DataFrame({
                    'UNIDAD': [ice_paises_corrido_unidad],
                    'SUMA_INVERSION_T_1' : [ice_paises_corrido_t_1],
                    'SUMA_INVERSION_T' : [ice_paises_corrido_t],
                    'DIFERENCIA_PORCENTUAL' : [ice_paises_corrido_diferencia_porcentual]
                })
            # Concatenar los datos de 'Otros' y los datos totales con el DataFrame original según agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:     
                if (row_num_ice_paises_corrido <= 5):
                    ice_paises_corrido_otros_totales = pd.concat([ice_paises_corrido, ice_paises_corrido_total])
                else: 
                    ice_paises_corrido_otros_totales = pd.concat([ice_paises_corrido, otros_df, ice_paises_corrido_total])
            if AGRUPACION in ['PAISES']:
                ice_paises_corrido_otros_totales = pd.concat([ice_paises_corrido])
            # Calcular la participación de cada categoría en el total de exportaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                total_t = ice_paises_corrido_total['SUMA_INVERSION_T'].sum()
                ice_paises_corrido_otros_totales = calcular_participacion_porcentual(ice_paises_corrido_otros_totales, 'SUMA_INVERSION_T', total_t, 'PARTICIPACION_T')
            ice_paises['ice_corrido'] = ice_paises_corrido_otros_totales
            # Datos de resumen por agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                if AGRUPACION == 'PAISES':
                    ice_corrido_agrupaciones = pd.concat([ice_paises_corrido, ice_corrido_total])
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
                    ice_corrido_agrupaciones = pd.concat([ice_paises_corrido_total, ice_corrido_total])
                total_t_ice = ice_corrido_total['SUMA_INVERSION_T'].sum()
                ice_corrido_agrupaciones = calcular_participacion_porcentual(ice_corrido_agrupaciones, 'SUMA_INVERSION_T', total_t_ice, 'PARTICIPACION_T')
                ice_total['ice_corrido_total'] = ice_corrido_agrupaciones
            # Agregar los datos al diccionario de resumen
            datos_resumen['ICE CORRIDO PAISES'] = {}
            for index, row in ice_paises_corrido_otros_totales.iterrows():
                unidad = row['UNIDAD']
                if unidad not in datos_resumen['ICE CORRIDO PAISES']:
                    datos_resumen['ICE CORRIDO PAISES'][unidad] = []
                
                entry = {
                    'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                    'sum_inversion_t': row['SUMA_INVERSION_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL']
                }
                
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                    entry['participacion_t'] = row['PARTICIPACION_T']
                
                datos_resumen['ICE CORRIDO PAISES'][unidad].append(entry)

            # Agregar los datos al diccionario de resumen por agrupaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                datos_resumen['ICE CORRIDO TOTAL'] = {}
                for index, row in ice_corrido_agrupaciones.iterrows():
                    unidad = row['UNIDAD']
                    if unidad not in datos_resumen['ICE CORRIDO TOTAL']:
                        datos_resumen['ICE CORRIDO TOTAL'][unidad] = []
                    
                    entry = {
                        'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                        'sum_inversion_t': row['SUMA_INVERSION_T'],
                        'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                        'participacion_t' : row['PARTICIPACION_T']
                    }
                    datos_resumen['ICE CORRIDO TOTAL'][unidad].append(entry)
    
    #########
    # TURISMO
    #########

    # Diccionarios para resultados
    # Cerrado
    turismo_cerrado = {}
    # Corrido
    turismo_corrido = {}

    #########
    # CERRADO
    #########

    # Construir consulta países
    query_paises_turismo_paises_cerrado = f"""
        SELECT A.PAIS_RESIDENCIA,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
            ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
    WHERE 1=1
    """
    # Países
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_paises_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    # Departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_paises_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    # Colombia
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_paises_cerrado += f" AND 1=1"
    # Group by
    query_paises_turismo_paises_cerrado += f" GROUP BY A.PAIS_RESIDENCIA ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"


    # Construir consulta departamentos
    query_paises_turismo_departamentos_cerrado = f"""
        SELECT A.DPTO_HOSPEDAJE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
            ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
    WHERE 1 = 1
    """
    # Países
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_departamentos_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    # Departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_departamentos_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    # Colombia
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_departamentos_cerrado += f" AND 1=1"
    # Group by
    query_paises_turismo_departamentos_cerrado += f" GROUP BY A.DPTO_HOSPEDAJE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"


    # Construir consulta municipos
    query_paises_turismo_municipio_cerrado = f"""
        SELECT A.CIUDAD_HOSPEDAJE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
            ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
    WHERE 1 = 1
    """
    # Países
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_municipio_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    # Departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_municipio_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    # Colombia
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_municipio_cerrado += f" AND 1=1"
    # Group by
    query_paises_turismo_municipio_cerrado += f" GROUP BY A.CIUDAD_HOSPEDAJE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"


    # Construir consulta género
    query_paises_turismo_genero_cerrado = f"""
        SELECT A.DESCRIPCION_GENERO,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
            ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
    WHERE 1 = 1
    """
    # Países
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_genero_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    # Departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_genero_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    # Colombia
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_genero_cerrado += f" AND 1=1"
    # Group by
    query_paises_turismo_genero_cerrado += f" GROUP BY A.DESCRIPCION_GENERO ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Construir consulta motivo
    query_paises_turismo_motivo_cerrado = f"""
        SELECT A.MOVC_NOMBRE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
            ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
    WHERE 1 = 1
    """
    # Países
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_motivo_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    # Departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_motivo_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    # Colombia
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_motivo_cerrado += f" AND 1=1"
    # Group by
    query_paises_turismo_motivo_cerrado += f" GROUP BY A.MOVC_NOMBRE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    #########
    # CORRIDO
    #########

    # Construir consulta países
    query_paises_turismo_paises_corrido = f"""
    SELECT A.PAIS_RESIDENCIA,
        SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
        SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
        CASE 
            WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
            WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
            WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
        ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
        END AS DIFERENCIA_PORCENTUAL
    FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
    WHERE 1 = 1
    """
    # Países
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_paises_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    # Departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_paises_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    # Colombia
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_paises_corrido += f" AND 1=1"
    # Group by
    query_paises_turismo_paises_corrido += f" GROUP BY A.PAIS_RESIDENCIA ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"
    
    # Construir consulta departamentos
    query_paises_turismo_departamentos_corrido = f"""
    SELECT A.DPTO_HOSPEDAJE,
        SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
        SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
        CASE 
            WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
            WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
            WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
        ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
        END AS DIFERENCIA_PORCENTUAL
    FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
    WHERE 1 = 1
    """
    # Países
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_departamentos_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    # Departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_departamentos_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    # Colombia
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_departamentos_corrido += f" AND 1=1"
    # Group by
    query_paises_turismo_departamentos_corrido += f" GROUP BY A.DPTO_HOSPEDAJE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Construir consulta municipos
    query_paises_turismo_municipio_corrido = f"""
    SELECT A.CIUDAD_HOSPEDAJE,
        SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
        SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
        CASE 
            WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
            WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
            WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
        ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
        END AS DIFERENCIA_PORCENTUAL
    FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
    WHERE 1 = 1
    """
    # Países
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_municipio_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    # Departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_municipio_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    # Colombia
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_municipio_corrido += f" AND 1=1"
    # Group by
    query_paises_turismo_municipio_corrido += f" GROUP BY A.CIUDAD_HOSPEDAJE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"
    

    # Ejecutar solo si hay datos año cerrado
    if dict_verificacion['turismo_cerrado'] == 'CON DATOS DE TURISMO CERRADO':
        # Tablas año cerrado
        # Ejecutar las consultas y almacenar los resultados en un DataFrame de pandas
        turismo_paises_cerrado = pd.DataFrame(session.sql(query_paises_turismo_paises_cerrado).collect())
        turismo_departamentos_cerrado = pd.DataFrame(session.sql(query_paises_turismo_departamentos_cerrado).collect())
        turismo_municipio_cerrado = pd.DataFrame(session.sql(query_paises_turismo_municipio_cerrado).collect())
        turismo_genero_cerrado = pd.DataFrame(session.sql(query_paises_turismo_genero_cerrado).collect())
        turismo_motivo_cerrado = pd.DataFrame(session.sql(query_paises_turismo_motivo_cerrado).collect())
        # Calcular tamaño de los df
        row_num_turismo_paises_cerrado = turismo_paises_cerrado.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
        row_num_turismo_departamentos_cerrado = turismo_departamentos_cerrado.shape[0] 
        row_num_turismo_municipio_cerrado = turismo_municipio_cerrado.shape[0]
        row_num_turismo_genero_cerrado = turismo_genero_cerrado.shape[0] 
        row_num_turismo_motivo_cerrado = turismo_motivo_cerrado.shape[0] 
        # Calcular total
        turismo_total_cerrado_categoria = 'TOTAL'
        turismo_total_cerrado_t_1 = turismo_paises_cerrado['SUMA_TURISMO_T_1'].sum()
        turismo_total_cerrado_t = turismo_paises_cerrado['SUMA_TURISMO_T'].sum()
        turismo_total_cerrado_diferencia_porcentual = calcular_diferencia_porcentual(turismo_total_cerrado_t, turismo_total_cerrado_t_1)
        # Tomar top 5 en dataframes válidos
        turismo_paises_cerrado = turismo_paises_cerrado.head(5)
        turismo_departamentos_cerrado = turismo_departamentos_cerrado.head(5)
        turismo_municipio_cerrado = turismo_municipio_cerrado.head(5)
        turismo_motivo_cerrado = turismo_motivo_cerrado.head(5)
        # Calcular otros
        # Lista de DataFrames
        # Lista de DataFrames y sus tamaños originales
        dataframes = [
            (turismo_paises_cerrado, row_num_turismo_paises_cerrado),
            (turismo_departamentos_cerrado, row_num_turismo_departamentos_cerrado),
            (turismo_municipio_cerrado, row_num_turismo_municipio_cerrado),
            (turismo_genero_cerrado, row_num_turismo_genero_cerrado),
            (turismo_motivo_cerrado, row_num_turismo_motivo_cerrado)
        ]
        # Lista para almacenar los DataFrames 'otros'
        otros_df_list = []
        # Lista para almacenar los Dataframes 'totales'
        totales_df_list = []
        # Loop a través de los DataFrames
        for df, original_length in dataframes:
            # Obtener el nombre de la primera columna del DataFrame actual
            primera_columna = df.columns[0]
            turismo_otros_cerrado_categoria = 'Otros'
            turismo_otros_cerrado_t_1 = turismo_total_cerrado_t_1 - df['SUMA_TURISMO_T_1'].sum()
            turismo_otros_cerrado_t = turismo_total_cerrado_t - df['SUMA_TURISMO_T'].sum()
            turismo_otros_cerrado_diferencia_porcentual = calcular_diferencia_porcentual(turismo_otros_cerrado_t, turismo_otros_cerrado_t_1)
            # Crear DataFrame 
            otros_df = pd.DataFrame({
                primera_columna: [turismo_otros_cerrado_categoria],
                'SUMA_TURISMO_T_1': [turismo_otros_cerrado_t_1],
                'SUMA_TURISMO_T': [turismo_otros_cerrado_t],
                'DIFERENCIA_PORCENTUAL_T': [turismo_otros_cerrado_diferencia_porcentual]
            })
            # Agregar el DataFrame 'otros' a la lista
            otros_df_list.append(otros_df)
            # Crear DataFrame de totales
            totales_df = pd.DataFrame({
                primera_columna: [turismo_total_cerrado_categoria],
                'SUMA_TURISMO_T_1': [turismo_total_cerrado_t_1],
                'SUMA_TURISMO_T': [turismo_total_cerrado_t],
                'DIFERENCIA_PORCENTUAL_T': [turismo_total_cerrado_diferencia_porcentual]
            })
            # Agregar el DataFrame 'totales' a la lista
            totales_df_list.append(totales_df) 

        # Calcular participación
        # Loop a través de los DataFrames
        for i, (df, original_length) in enumerate(dataframes):
            if df is turismo_genero_cerrado:
                df_final = pd.concat([df, totales_df_list[i]])
            else:
                if original_length <= 5:
                    df_final = pd.concat([df, totales_df_list[i]])
                else:
                    df_final = pd.concat([df, otros_df_list[i], totales_df_list[i]])
            df_final = calcular_participacion_porcentual(df_final, 'SUMA_TURISMO_T', turismo_total_cerrado_t, 'PARTICIPACION_T')
            # Obtener el nombre de la primera columna del DataFrame actual
            primera_columna = df_final.columns[0]
            turismo_cerrado[primera_columna] = df_final

        # Agregar al diccionario de resumen los datos de interés
        # País
        if 'PAIS_RESIDENCIA' in turismo_cerrado:
            # Inicializar el diccionario para almacenar los resultados de turismo cerrado por países
            datos_resumen['TURISMO CERRADO PAISES'] = {}
            # Iterar a través de cada fila del DataFrame
            for index, row in turismo_cerrado['PAIS_RESIDENCIA'].iterrows():
                unidad = row['PAIS_RESIDENCIA']
                # Si la unidad no está en el diccionario, agregarla
                if unidad not in datos_resumen['TURISMO CERRADO PAISES']:
                    datos_resumen['TURISMO CERRADO PAISES'][unidad] = []
                
                # Crear una entrada con los datos relevantes
                entry = {
                    'sum_turismo_t_1': row['SUMA_TURISMO_T_1'],
                    'sum_turismo_t': row['SUMA_TURISMO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T'],
                    'participacion_t' : row['PARTICIPACION_T']
                }
                # Agregar la entrada al diccionario
                datos_resumen['TURISMO CERRADO PAISES'][unidad].append(entry)
        
        # Departamentos
        if 'DPTO_HOSPEDAJE' in turismo_cerrado:
        # Inicializar el diccionario para almacenar los resultados de turismo cerrado por departamentos
            datos_resumen['TURISMO CERRADO DEPARTAMENTOS'] = {}
            # Iterar a través de cada fila del DataFrame
            for index, row in turismo_cerrado['DPTO_HOSPEDAJE'].iterrows():
                unidad = row['DPTO_HOSPEDAJE']
                # Si la unidad no está en el diccionario, agregarla
                if unidad not in datos_resumen['TURISMO CERRADO DEPARTAMENTOS']:
                    datos_resumen['TURISMO CERRADO DEPARTAMENTOS'][unidad] = []
                
                # Crear una entrada con los datos relevantes
                entry = {
                    'sum_turismo_t_1': row['SUMA_TURISMO_T_1'],
                    'sum_turismo_t': row['SUMA_TURISMO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T'],
                    'participacion_t' : row['PARTICIPACION_T']
                }
                # Agregar la entrada al diccionario
                datos_resumen['TURISMO CERRADO DEPARTAMENTOS'][unidad].append(entry)

    # Ejecutar solo si hay datos año cerrado
    if dict_verificacion['turismo_corrido'] == 'CON DATOS DE TURISMO CORRIDO':
        # Tablas año corrido
        turismo_paises_corrido = pd.DataFrame(session.sql(query_paises_turismo_paises_corrido).collect())
        turismo_departamentos_corrido = pd.DataFrame(session.sql(query_paises_turismo_departamentos_corrido).collect())
        turismo_municipio_corrido = pd.DataFrame(session.sql(query_paises_turismo_municipio_corrido).collect())
        # Calcular tamaño de los df
        row_num_turismo_paises_corrido = turismo_paises_corrido.shape[0] # Para utilizar en el if para agregar o no otros de forma correcta
        row_num_turismo_departamentos_corrido = turismo_departamentos_corrido.shape[0] 
        row_num_turismo_municipio_corrido = turismo_municipio_corrido.shape[0]           
        # Calcular total
        turismo_total_corrido_categoria = 'TOTAL'
        turismo_total_corrido_t_1 = turismo_paises_corrido['SUMA_TURISMO_T_1'].sum()
        turismo_total_corrido_t = turismo_paises_corrido['SUMA_TURISMO_T'].sum()
        turismo_total_corrido_diferencia_porcentual = calcular_diferencia_porcentual(turismo_total_corrido_t, turismo_total_corrido_t_1)
        # Tomar top 5 en dataframes válidos
        turismo_paises_corrido = turismo_paises_corrido.head(5)
        turismo_departamentos_corrido = turismo_departamentos_corrido.head(5)
        turismo_municipio_corrido = turismo_municipio_corrido.head(5)
        # Calcular otros
        # Lista de DataFrames y sus tamaños originales
        dataframes_corrido = [
            (turismo_paises_corrido, row_num_turismo_paises_corrido),
            (turismo_departamentos_corrido, row_num_turismo_departamentos_corrido),
            (turismo_municipio_corrido, row_num_turismo_municipio_corrido)
        ]
        # Lista para almacenar los DataFrames 'otros'
        otros_corrido_df_list = []
        # Lista para almacenar los Dataframes 'totales'
        totales_corrido_df_list = []
        # Loop a través de los DataFrames
        for df, original_length in dataframes_corrido:
            # Obtener el nombre de la primera columna del DataFrame actual
            primera_columna = df.columns[0]

            turismo_otros_corrido_categoria = 'Otros'
            turismo_otros_corrido_t_1 = turismo_total_corrido_t_1 - df['SUMA_TURISMO_T_1'].sum()
            turismo_otros_corrido_t = turismo_total_corrido_t - df['SUMA_TURISMO_T'].sum()
            turismo_otros_corrido_diferencia_porcentual = calcular_diferencia_porcentual(turismo_otros_corrido_t, turismo_otros_corrido_t_1)
            # Crear DataFrame 
            otros_df = pd.DataFrame({
                primera_columna: [turismo_otros_corrido_categoria],
                'SUMA_TURISMO_T_1': [turismo_otros_corrido_t_1],
                'SUMA_TURISMO_T': [turismo_otros_corrido_t],
                'DIFERENCIA_PORCENTUAL': [turismo_otros_corrido_diferencia_porcentual]
            })
            # Agregar el DataFrame 'otros' a la lista
            otros_corrido_df_list.append(otros_df)
            # Crear DataFrame 
            totales_df = pd.DataFrame({
                primera_columna: [turismo_total_corrido_categoria],
                'SUMA_TURISMO_T_1': [turismo_total_corrido_t_1],
                'SUMA_TURISMO_T': [turismo_total_corrido_t],
                'DIFERENCIA_PORCENTUAL': [turismo_total_corrido_diferencia_porcentual]
            })
            # Agregar el DataFrame 'totales' a la lista
            totales_corrido_df_list.append(totales_df)

        # Calcular participación
        # Loop a través de los DataFrames
        for i, (df, original_length) in enumerate(dataframes_corrido):
            if original_length <= 5:
                df_final = pd.concat([df, totales_corrido_df_list[i]])
            else:
                df_final = pd.concat([df, otros_corrido_df_list[i], totales_corrido_df_list[i]])
            df_final = calcular_participacion_porcentual(df_final, 'SUMA_TURISMO_T', turismo_total_corrido_t, 'PARTICIPACION_T')
            # Obtener el nombre de la primera columna del DataFrame actual
            primera_columna = df_final.columns[0]
            turismo_corrido[primera_columna] = df_final

        # Agregar al diccionario de resumen los datos de interés
        # País
        if 'PAIS_RESIDENCIA' in turismo_corrido:
            # Inicializar el diccionario para almacenar los resultados de turismo corrido por países
            datos_resumen['TURISMO CORRIDO PAISES'] = {}
            # Iterar a través de cada fila del DataFrame
            for index, row in turismo_corrido['PAIS_RESIDENCIA'].iterrows():
                unidad = row['PAIS_RESIDENCIA']
                # Si la unidad no está en el diccionario, agregarla
                if unidad not in datos_resumen['TURISMO CORRIDO PAISES']:
                    datos_resumen['TURISMO CORRIDO PAISES'][unidad] = []
                
                # Crear una entrada con los datos relevantes
                entry = {
                    'sum_turismo_t_1': row['SUMA_TURISMO_T_1'],
                    'sum_turismo_t': row['SUMA_TURISMO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                    'participacion_t' : row['PARTICIPACION_T']
                }           
                # Agregar la entrada al diccionario
                datos_resumen['TURISMO CORRIDO PAISES'][unidad].append(entry)

        # Departamento
        if 'DPTO_HOSPEDAJE' in turismo_corrido:
            # Inicializar el diccionario para almacenar los resultados de turismo corrido por departamentos
            datos_resumen['TURISMO CORRIDO DEPARTAMENTOS'] = {}
            # Iterar a través de cada fila del DataFrame
            for index, row in turismo_corrido['DPTO_HOSPEDAJE'].iterrows():
                unidad = row['DPTO_HOSPEDAJE']
                # Si la unidad no está en el diccionario, agregarla
                if unidad not in datos_resumen['TURISMO CORRIDO DEPARTAMENTOS']:
                    datos_resumen['TURISMO CORRIDO DEPARTAMENTOS'][unidad] = []
                
                # Crear una entrada con los datos relevantes
                entry = {
                    'sum_turismo_t_1': row['SUMA_TURISMO_T_1'],
                    'sum_turismo_t': row['SUMA_TURISMO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                    'participacion_t' : row['PARTICIPACION_T']
                }           
                # Agregar la entrada al diccionario
                datos_resumen['TURISMO CORRIDO DEPARTAMENTOS'][unidad].append(entry)
    
    ##############
    # CONECTIVIDAD
    ##############
    conectividad = {}
    if AGRUPACION in ['DEPARTAMENTOS']:
        if (dict_verificacion['conectividad'] == "CON DATOS DE CONECTIVIDAD"):
        # Los datos de conectividad solo se usan en departamentos
            # Constuir consulta
            query_conectividad = """SELECT A.AEROLINEA AS "Aerolínea",
            A.CIUDAD_ORIGEN AS "Ciudad Origen",
            A.CIUDAD_DESTINO AS "Ciudad Destino",
            A.FRECUENCIAS AS "Frecuencias",
            A.SEMANA AS "Semana de análisis"
            FROM DOCUMENTOS_COLOMBIA.TURISMO.CONECTIVIDAD AS A
            WHERE 1 = 1 """
            # Agregar departamento
            query_conectividad += f" AND A.COD_DIVIPOLA_DEPARTAMENTO_DESTINO IN ({DEPARTAMENTOS_TURISMO_sql})"

            # Ejecutar consulta y agregar
            df_conectividad = pd.DataFrame(session.sql(query_conectividad).collect())

            # Agregar a un diccionario
            conectividad['CONECTIVIDAD'] = df_conectividad

    ###############
    # OPORTUNIDADES
    ###############

    oportunidades = {}
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'DEPARTAMENTOS', 'COLOMBIA']: 
        if (dict_verificacion['oportunidades_exportacion'] == "CON OPORTUNIDADES"):
            # Exportación
            query_oportunidades_exportacion = """
            SELECT DISTINCT A.CADENA,
                LOWER(A.SUBSECTOR) AS SUBSECTOR
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
            WHERE A.OPORTUNIDAD = 'Exportación' 
                AND A.CADENA NOT IN (('Turismo'))
            """
            # Países
            if AGRUPACION in ['PAISES']:
                query_oportunidades_exportacion += f" AND A.COD_PAIS IN ({PAISES_TURISMO_sql})"
            # Departamentos
            if AGRUPACION in ['DEPARTAMENTOS']:
                query_oportunidades_exportacion += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
            # Order
            query_oportunidades_exportacion += f" ORDER BY 1, 2 ASC"

            # Ejecutar consulta
            oportunidades_exportacion_df = pd.DataFrame(session.sql(query_oportunidades_exportacion).collect())
            # Agregarlas a los resultados
            oportunidades['EXPORTACIONES'] = oportunidades_exportacion_df

        if (dict_verificacion['oportunidades_inversion'] == "CON OPORTUNIDADES"):
            # Inversión
            query_oportunidades_ied = """
            SELECT DISTINCT A.CADENA,
                LOWER(A.SUBSECTOR) AS SUBSECTOR
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
            WHERE A.OPORTUNIDAD = 'IED'
            """
            # Países
            if AGRUPACION in ['PAISES']:
                query_oportunidades_ied += f" AND A.COD_PAIS IN ({PAISES_TURISMO_sql})"
            # Departamentos
            if AGRUPACION in ['DEPARTAMENTOS']:
                query_oportunidades_ied += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
            # Order
            query_oportunidades_ied += f" ORDER BY 1, 2 ASC"

            # Ejecutar consulta
            oportunidades_inversion_df = pd.DataFrame(session.sql(query_oportunidades_ied).collect())
            # Agregarlas a los resultados
            oportunidades['INVERSION'] = oportunidades_inversion_df

        if (dict_verificacion['oportunidades_turismo'] == "CON OPORTUNIDADES"):
            # Turismo
            query_oportunidades_turismo = """
            SELECT DISTINCT LOWER(A.SECTOR) AS SECTOR,
                LOWER(A.SUBSECTOR) AS SUBSECTOR
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
            WHERE A.CADENA IN ('Turismo')
            """
            # Países y agrupaciones de países
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                query_oportunidades_turismo += f" AND A.COD_PAIS IN ({PAISES_TURISMO_sql})"
            # Departamentos
            if AGRUPACION in ['DEPARTAMENTOS']:
                query_oportunidades_turismo += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
            # Order
            query_oportunidades_turismo += f" ORDER BY 1, 2 ASC"
            # Ejecutar consulta
            oportunidades_turismo_df = pd.DataFrame(session.sql(query_oportunidades_turismo).collect())
            # Agregarlas a los resultados
            oportunidades['TURISMO'] = oportunidades_turismo_df

    # 10. Retornar todos los resultados en un diccionario
    return {
        'TOTALES': totales,
        'TIPOS': tipos,
        'CATEGORIAS CERRADO': categorias_cerrado,
        'CATEGORIAS CORRIDO': categorias_corrido,
        'EMPRESAS': empresas,
        'CONTEO EMPRESAS': conteo,
        'TOTALES PESO': totales_peso,
        'TIPOS PESO': tipos_peso,
        'IED ACTIVIDADES COLOMBIA' : ied_colombia_actividades,
        'IED PAISES' : ied_paises,
        'IED TOTAL' : ied_total,
        'ICE PAISES' : ice_paises,
        'ICE TOTAL' : ice_total,
        'TURISMO CERRADO' : turismo_cerrado,
        'TURISMO CORRIDO' : turismo_corrido,
        'RESUMEN' : datos_resumen,
        'CONECTIVIDAD' : conectividad,
        'OPORTUNIDADES' : oportunidades,
        'MEDIOS PESO MINERO' : medios_peso_minero,
        'MEDIOS PESO NO MINERO' : medios_peso_no_minero
        }

def get_parameters_exportaciones(sesion):
    """
    Obtiene los parámetros de año cerrado y año corrido para exportaciones.

    Parámetros:
    - sesion: sesión de Snowflake.

    Retorna:
    Un diccionario con los parámetros T y T_1 para año cerrado y año corrido.
    """
    # Consulta de parámetros de año cerrado
    query_cerrado = """
    SELECT MAX(CASE WHEN B.PARAMETRO = 'Año cerrado (T-1)' THEN B.VALOR ELSE NULL END) AS T_1_YEAR,
           MAX(CASE WHEN B.PARAMETRO = 'Año cerrado (T)' THEN B.VALOR ELSE NULL END) AS T_YEAR
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS AS B
    WHERE B.EJE = 'Exportaciones'
      AND B.PARAMETRO IN ('Año cerrado (T-1)', 'Año cerrado (T)');
    """
    params_cerrado = pd.DataFrame(sesion.sql(query_cerrado).collect()).iloc[0]

    # Consulta de parámetros de año corrido
    query_corrido = """
    SELECT MAX(CASE WHEN B.PARAMETRO = 'Año corrido (T-1)' THEN B.VALOR ELSE NULL END) AS T_1_YEAR,
           MAX(CASE WHEN B.PARAMETRO = 'Año corrido (T)' THEN B.VALOR ELSE NULL END) AS T_YEAR,
           MAX(CASE WHEN B.PARAMETRO = 'Mes corrido texto (T)' THEN B.VALOR ELSE NULL END) AS MES_T
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS AS B
    WHERE B.EJE = 'Exportaciones'
      AND B.PARAMETRO IN ('Año corrido (T-1)', 'Año corrido (T)', 'Mes corrido texto (T)');
    """
    params_corrido = pd.DataFrame(sesion.sql(query_corrido).collect()).iloc[0]

    # Función para obtener el año del periodo corrido
    def get_year(year):
        return year.split('(')[0]

    return {
        'cerrado': {
            'T_1': params_cerrado['T_1_YEAR'],
            'T': params_cerrado['T_YEAR']
        },
        'corrido': {
            'T_1': params_corrido['T_1_YEAR'],
            'T': params_corrido['T_YEAR'],
            'MES_T' : params_corrido['MES_T'],
            'T_1_YEAR': get_year(params_corrido['T_1_YEAR']),
            'T_YEAR': get_year(params_corrido['T_YEAR'])
        }
    }


def get_parameters_inversion(sesion):
    """
    Obtiene los parámetros de año cerrado y año corrido para inversión.

    Parámetros:
    - sesion: sesión de Snowflake.

    Retorna:
    Un diccionario con los parámetros T y T_1 para año cerrado y año corrido.
    """
    # Consulta de parámetros de año cerrado
    query_cerrado = """
    SELECT MAX(CASE WHEN B.PARAMETRO = 'Año cerrado (T-1)' THEN B.VALOR ELSE NULL END) AS T_1_YEAR,
            MAX(CASE WHEN B.PARAMETRO = 'Año cerrado (T)' THEN B.VALOR ELSE NULL END) AS T_YEAR
        FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS AS B
        WHERE B.EJE = 'Inversión'
            AND B.PARAMETRO IN ('Año cerrado (T-3)', 'Año cerrado (T-2)', 'Año cerrado (T-1)', 'Año cerrado (T)');
    """
    params_cerrado = pd.DataFrame(sesion.sql(query_cerrado).collect()).iloc[0]

    # Consulta de parámetros de año corrido
    query_corrido = """
    SELECT MAX(CASE WHEN B.PARAMETRO = 'Año corrido (T-1)' THEN B.VALOR ELSE NULL END) AS T_1_YEAR,
        MAX(CASE WHEN B.PARAMETRO = 'Año corrido (T)' THEN B.VALOR ELSE NULL END) AS T_YEAR
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS AS B
    WHERE B.EJE = 'Inversión'
        AND B.PARAMETRO IN ('Año corrido (T-1)', 'Año corrido (T)');
    """
    params_corrido = pd.DataFrame(sesion.sql(query_corrido).collect()).iloc[0]

    # Función para obtener el nombre del trimestre
    def get_trimestre_name(year_quarter):
        quarter = year_quarter.split('-')[-1]
        if quarter == '1':
            return "primer"
        elif quarter == '2':
            return "segundo"
        elif quarter == '3':
            return "tercer"
        elif quarter == '4':
            return "cuarto"
        return ""

    # Función para obtener el año del trimestre
    def get_year(year_quarter):
        return year_quarter.split('-')[0]

    return {
        'cerrado': {
            'T_1': params_cerrado['T_1_YEAR'],
            'T': params_cerrado['T_YEAR']
        },
        'corrido': {
            'T_1': params_corrido['T_1_YEAR'],
            'T': params_corrido['T_YEAR'],
            'T_1_TRIMESTER_NUMBER': params_corrido['T_1_YEAR'].split('-')[-1],
            'T_TRIMESTER_NUMBER': params_corrido['T_YEAR'].split('-')[-1],
            'T_1_TRIMESTER_NAME': get_trimestre_name(params_corrido['T_1_YEAR']),
            'T_TRIMESTER_NAME': get_trimestre_name(params_corrido['T_YEAR']),
            'T_1_YEAR': get_year(params_corrido['T_1_YEAR']),
            'T_YEAR': get_year(params_corrido['T_YEAR'])
        }
    }


def get_parameters_turismo(sesion):
    """
    Obtiene los parámetros de año cerrado y mes corrido para turismo.

    Parámetros:
    - sesion: sesión de Snowflake.

    Retorna:
    Un diccionario con los parámetros T, T_1 para año cerrado y T_MONTH para mes corrido.
    """
    # Consulta de parámetros de turismo
    query_turismo = """
    SELECT MAX(CASE WHEN B.PARAMETRO = 'Año cerrado (T-1)' THEN B.VALOR ELSE NULL END) AS T_1_YEAR,
        MAX(CASE WHEN B.PARAMETRO = 'Año cerrado (T)' THEN B.VALOR ELSE NULL END) AS T_YEAR,
        MAX(CASE WHEN B.PARAMETRO = 'Año corrido (T-1)' THEN B.VALOR ELSE NULL END) AS T_1_YEAR_CORRIDO,
        MAX(CASE WHEN B.PARAMETRO = 'Año corrido (T)' THEN B.VALOR ELSE NULL END) AS T_YEAR_CORRIDO,
        MAX(CASE WHEN B.PARAMETRO = 'Mes corrido' THEN B.VALOR ELSE NULL END) AS T_MONTH_CORRIDO
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS AS B
        WHERE B.EJE = 'Turismo';
    """
    params_turismo = pd.DataFrame(sesion.sql(query_turismo).collect()).iloc[0]

    # Diccionario para las abreviaciones de los meses en español
    meses_abreviados = {
        1: "Ene",
        2: "Feb",
        3: "Mar",
        4: "Abr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Ago",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dic"
    }

    # Diccionario de los meses en español
    meses_es = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre"
    }

    # Convertir el valor de T_MONTH_CORRIDO a la abreviación del mes en español
    month_number = int(params_turismo['T_MONTH_CORRIDO'])
    month_abbr = meses_abreviados[month_number]

    # Convertir el valor de T_MONTH_CORRIDO al nombre del mes en español
    month_name = meses_es[month_number]        

    return {
        'cerrado': {
            'T_1': params_turismo['T_1_YEAR'],
            'T': params_turismo['T_YEAR']
        },
        'corrido': {
            'T_1': params_turismo['T_1_YEAR_CORRIDO'],
            'T': params_turismo['T_YEAR_CORRIDO'],
            'T_MONTH': params_turismo['T_MONTH_CORRIDO'],
            'T_MONTH_NAME': month_abbr,
            'T_MONTH_NAME_FULL' : month_name
        }
    }


def transform_year_column_name(col_name):
    """
    Transforma el nombre de una columna de año para un formato más amigable.
    
    Parámetros:
    col_name (str): Nombre de la columna que contiene el año y el período.

    Retorna:
    str: El nombre de la columna transformado si contiene paréntesis, de lo contrario, el nombre original.
    """
    if '(' in col_name and ')' in col_name:
        year, period = col_name.split('(')
        period = period.replace(')', '')
        return f'{period.strip()} {year.strip()}'
    return col_name


def format_columns_exportaciones(df):
    """Aplica el formato adecuado a las columnas de valor, peso, variación y participación"""
    # Formatear columnas de valor USD y peso
    for col in df.columns:        
        # Exportaciones
        if 'USD' in col or 'TONELADAS' in col:
            df[col] = df[col].apply(lambda x: f"{x:,.0f}".replace(',', 'X').replace('.', ',').replace('X', '.') if isinstance(x, (int, float)) else x)

    # Formatear columnas de variación y participación
    if 'Variación (%)' in df.columns:
        df['Variación (%)'] = df['Variación (%)'].apply(lambda x: f"{x:.1f}%".replace('.', ',') if isinstance(x, (int, float)) else x)

    # Formatear columnas de participación, buscando cualquier columna que comience con 'Participación (%)'
    for col in df.columns:
        if col.startswith('Participación (%)'):
            df[col] = df[col].apply(lambda x: f"{x:.1f}%".replace('.', ',') if isinstance(x, (int, float)) else x)
    
    # Devolver dataframe
    return df

def format_columns_exportaciones_excel(df):
    """Aplica el formato adecuado a las columnas de valor, peso, variación y participación"""
    # Formatear columnas de valor USD y peso
    for col in df.columns:
        if 'USD' in col or 'TONELADAS' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Formatear columnas de variación y participación
    if 'Variación (%)' in df.columns:
        df['Variación (%)'] = pd.to_numeric(df['Variación (%)'], errors='coerce').round(2)

    # Formatear columnas de participación, buscando cualquier columna que comience con 'Participación (%)'
    for col in df.columns:
        if col.startswith('Participación (%)'):
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Devolver dataframe
    return df


def format_columns_inversion(df):
    """Aplica el formato adecuado a las columnas de valor, peso, variación y participación"""

    # Capitalizar la primera columna para países
    if 'País' in df.columns[0]:
       first_col = df.columns[0]
       df[first_col] = df[first_col].apply(lambda x: ' '.join(word.capitalize() for word in x.split()))

    # Formatear columnas de valor USD
    for col in df.columns:
        if 'USD' in col:
            df[col] = df[col].apply(lambda x: f"{x:,.1f}".replace(',', 'X').replace('.', ',').replace('X', '.') if isinstance(x, (int, float)) else x)

    # Formatear columnas de participación, buscando cualquier columna que comience con 'Participación (%)'
    for col in df.columns:
        if col.startswith('Participación (%)'):
            df[col] = df[col].apply(lambda x: f"{x:.1f}%".replace('.', ',') if isinstance(x, (int, float)) else x)
        if col.startswith('Variación (%)'):
            df[col] = df[col].apply(lambda x: f"{x:.1f}%".replace('.', ',') if isinstance(x, (int, float)) else x)

    # Devolver dataframe
    return df

def format_columns_inversion_excel(df):
    """Aplica el formato adecuado a las columnas de valor, peso, variación y participación"""

    # Capitalizar la primera columna para países
    if 'País' in df.columns[0]:
        first_col = df.columns[0]
        df[first_col] = df[first_col].apply(lambda x: ' '.join(word.capitalize() for word in x.split()))

    # Formatear columnas de valor USD
    for col in df.columns:
        if 'USD' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Formatear columnas de participación y variación, buscando cualquier columna que comience con 'Participación (%)' o 'Variación (%)'
    for col in df.columns:
        if col.startswith('Participación (%)') or col.startswith('Variación (%)'):
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Devolver dataframe
    return df

def format_columns_turismo(df):
    """Aplica el formato adecuado a las columnas"""

    # Capitalizar la primera columna para países, departamentos, ciudades y motivos
    if df.columns[0] in ['País de residencia', 'Departamento de hospedaje', 'Ciudad de hospedaje', 'Motivo de viaje', 'Género']:
        first_col = df.columns[0]
        df[first_col] = df[first_col].apply(lambda x: ' '.join(word.capitalize() for word in x.split()) if isinstance(x, str) else x)

    # Convertir columnas de valor a números
    for col in df.columns:
        if col.startswith('20') or col.startswith('Ene'):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convertir columnas de participación y variación a números
    for col in df.columns:
        if col.startswith('Participación (%)') or col.startswith('Variación (%)') or col.startswith('Diferencia'):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Formatear columnas de valor
    for col in df.columns:
        if col.startswith('20') or col.startswith('Ene') or col.startswith('Diferencia'):
            df[col] = df[col].apply(lambda x: f"{x:,.0f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) else x)

    # Formatear columnas de participación y variación
    for col in df.columns:
        if col.startswith('Participación (%)') or col.startswith('Variación (%)'):
            df[col] = df[col].apply(lambda x: f"{x:.1f}%".replace('.', ',') if pd.notnull(x) else x)

    # Devolver dataframe
    return df

def format_columns_turismo_excel(df):
    """Aplica el formato adecuado a las columnas"""

    # Capitalizar la primera columna para países, departamentos, ciudades y motivos
    if df.columns[0] in ['País de residencia', 'Departamento de hospedaje', 'Ciudad de hospedaje', 'Motivo de viaje', 'Género']:
        first_col = df.columns[0]
        df[first_col] = df[first_col].apply(lambda x: ' '.join(word.capitalize() for word in x.split()) if isinstance(x, str) else x)

    # Convertir columnas de valor a números y redondear a dos decimales
    for col in df.columns:
        if col.startswith('20') or col.startswith('Ene'):
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Convertir columnas de participación y variación a números y redondear a dos decimales
    for col in df.columns:
        if col.startswith('Participación (%)') or col.startswith('Variación (%)') or col.startswith('Diferencia'):
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Devolver dataframe
    return df

def obtener_paises_correlativa(session):
    """
    Ejecuta la consulta en Snowflake y devuelve los resultados como un dataframe.

    Parámetros:
    session (snowflake.connector.SnowflakeConnection): La conexión a la sesión de Snowflake.

    Retorna:
    data: Una base de datos con los resultados de la consulta.
    """
    # Construir la consulta
    query = """
    SELECT A.CODIGO_DIAN,
           A.COUNTRY_OR_AREA_UNSD,
           A.PAIS_LLAVE_EXPORTACIONES,
           A.CONTINENTE_DANE_DIAN_EXPORTACIONES,
           A.PAIS_INVERSION_BANREP,
           A.PAIS_CODIGO_TURISMO,
           A.NOMBRE_PAIS_CODIGO_TURISMO
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.PAISES_CORRELATIVA AS A
    WHERE A.COUNTRY_OR_AREA_UNSD IS NOT NULL;
    """
    # Realizar esa consulta
    data = pd.DataFrame(session.sql(query).collect())
        
    return data

def obtener_departamentos_correlativa(session):
    """
    Ejecuta la consulta en Snowflake y devuelve los resultados como un dataframe.

    Parámetros:
    session (snowflake.connector.SnowflakeConnection): La conexión a la sesión de Snowflake.

    Retorna:
    data: Una base de datos con los resultados de la consulta.
    """
    # Construir la consulta
    query = """
    SELECT A.DEPARTAMENTO_DIAN,
           A.COD_DIAN_DEPARTAMENTO
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIAN_DEPARTAMENTOS AS A;
    """
    # Realizar la consulta
    data = pd.DataFrame(session.sql(query).collect())
        
    return data

def obtener_municipios_correlativa(session):
    """
    Ejecuta la consulta en Snowflake y devuelve los resultados como un dataframe.

    Parámetros:
    session (snowflake.connector.SnowflakeConnection): La conexión a la sesión de Snowflake.

    Retorna:
    data: Una base de datos con los resultados de la consulta.
    """
    # Construir la consulta
    query = """
    SELECT A.COD_DANE_MUNICIPIO,
           A.MUNICIPIO_DANE
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIVIPOLA_MUNICIPIOS AS A;
    """
    # Realizar la consulta
    data = pd.DataFrame(session.sql(query).collect())
        
    return data

def format_number(value):
    """Formatea un número con coma como separador decimal y punto como separador de miles."""
    return f"{value:,.1f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def format_number_no_decimal(value):
    """Formatea un número con coma como separador decimal y punto como separador de miles, sin decimales."""
    return f"{value:,.0f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def inversion_palabra(valor):
    """Devuelve 'positivo' si el valor es mayor que 0, 'negativo' en caso contrario."""
    return "positivos" if valor > 0 else "negativos"

def variacion_palabra(valor):
    """Devuelve 'mayor' si el valor es mayor que 0, 'menor' en caso contrario."""
    return "más" if valor > 0 else "menos"

def resumen_datos(data_dict, agrupacion, unidad, export_params, inversion_params, turismo_params, dict_verif):
    """
    Genera un resumen de datos de exportaciones, inversión y turismo.

    Parámetros:
    - data_dict: diccionario con los datos de exportaciones, inversión y turismo.
    - agrupacion: el nivel de agrupación para filtrar los datos.
    - unidad: nombre de la unidad de la agrupación para el resumen
    - export_params: parámetros para exportaciones.
    - inversion_params: parámetros para inversión.
    - turismo_params: parámetros para turismo.
    - dict_verif: diccionario con las marcas de verificación por los tres ejes

    Retorna:
    Un diccionario con las tablas de resumen y losz textos de resumen.
    """

    # Exportaciones:
    expo_columna_t_1_cerrado = f"{export_params['cerrado']['T_1']} (USD FOB millones)"
    expo_columna_t_cerrado = f"{export_params['cerrado']['T']} (USD FOB millones)"
    expo_variacion_cerrado = f"Variación (%) {transform_year_column_name(export_params['corrido']['T_1'])}"
    expo_columna_t_1_corrido = f"{transform_year_column_name(export_params['corrido']['T_1'])} (USD FOB millones)"
    expo_columna_t_corrido = f"{transform_year_column_name(export_params['corrido']['T'])} (USD FOB millones)"
    expo_variacion_corrido = f"Variación (%) {transform_year_column_name(export_params['corrido']['T'])}"


    # Crear la tabla con las variables y los datos del diccionario
    variables_expo = ['No Mineras', 'Mineras', 'Total']

    # Inicializar data_expo_cerrado y data_expo_corrido
    data_expo_cerrado = {}
    data_expo_corrido = {}

    # Verificar y construir la tabla de exportaciones cerradas
    if dict_verif['exportaciones_totales_cerrado'] == 'CON DATOS DE EXPORTACIONES TOTALES CERRADO':
        data_expo_cerrado = {
            'Tipo de exportación': variables_expo,
            expo_columna_t_1_cerrado: [data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO'].get(var, [{'sum_usd_t_1': 0}])[0]['sum_usd_t_1'] / 1e6 for var in variables_expo],
            expo_columna_t_cerrado: [data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO'].get(var, [{'sum_usd_t': 0}])[0]['sum_usd_t'] / 1e6 for var in variables_expo],
            expo_variacion_cerrado: [data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_expo]            
        }
    else:
        data_expo_cerrado = {
            'Tipo de exportación': variables_expo,
            expo_columna_t_1_cerrado: [0 for var in variables_expo],
            expo_columna_t_cerrado: [0 for var in variables_expo],
            expo_variacion_cerrado: [0 for var in variables_expo],
        }

    # Verificar y construir la tabla de exportaciones corridas
    if dict_verif['exportaciones_totales_corrido'] == 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO':
        data_expo_corrido = {
            expo_columna_t_1_corrido: [data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO'].get(var, [{'sum_usd_t_1': 0}])[0]['sum_usd_t_1'] / 1e6 for var in variables_expo],
            expo_columna_t_corrido: [data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO'].get(var, [{'sum_usd_t': 0}])[0]['sum_usd_t'] / 1e6 for var in variables_expo],
            expo_variacion_corrido: [data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_expo]
        }
    else:
        data_expo_corrido = {
            expo_columna_t_1_corrido: [0 for var in variables_expo],
            expo_columna_t_corrido: [0 for var in variables_expo],
            expo_variacion_corrido: [0 for var in variables_expo],
        }

    # Unir los datos cerrados y corridos
    data_expo = {**data_expo_cerrado, **data_expo_corrido}

    # Convertir el diccionario a DataFrame 
    tab_resumen_expo = pd.DataFrame(data_expo)
    tab_resumen_expo = format_columns_inversion(tab_resumen_expo)

    # Columnas para inversión
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        inv_columna_t_1_cerrado = f"{inversion_params['cerrado']['T_1']} (USD millones)"
        inv_columna_t_cerrado = f"{inversion_params['cerrado']['T']} (USD millones)"
        inv_variacion_cerrado = f"Variación (%) {transform_year_column_name(inversion_params['cerrado']['T'])}"
        inv_columna_t_1_corrido = f"{transform_year_column_name(inversion_params['corrido']['T_1'])} (USD millones)"
        inv_columna_t_corrido = f"{transform_year_column_name(inversion_params['corrido']['T'])} (USD millones)"
        inv_variacion_corrido = f"Variación (%) {transform_year_column_name(inversion_params['corrido']['T'])}"

        # Inicializar data_ied_cerrado, data_ied_corrido, data_ice_cerrado y data_ice_corrido
        data_ied_cerrado = {}
        data_ied_corrido = {}
        data_ice_cerrado = {}
        data_ice_corrido = {}

        # Inicializar variables_ied
        variables_ied = ['TOTAL'] if agrupacion != 'PAISES' else []

        # Crear la tabla con las variables y los datos del diccionario para IED
        # Verificar y construir la tabla de IED cerradas
        if dict_verif['ied_cerrado'] == 'CON DATOS DE IED CERRADO':
            if agrupacion == 'PAISES':
                variables_ied = list(data_dict['RESUMEN']['IED CERRADO PAISES'].keys())
            data_ied_cerrado = {
                'Tipo de inversión': 'IED',
                inv_columna_t_1_cerrado: [data_dict['RESUMEN']['IED CERRADO PAISES'].get(var, [{'sum_inversion_t_1': 0}])[0]['sum_inversion_t_1'] for var in variables_ied],
                inv_columna_t_cerrado: [data_dict['RESUMEN']['IED CERRADO PAISES'].get(var, [{'sum_inversion_t': 0}])[0]['sum_inversion_t'] for var in variables_ied],
                inv_variacion_cerrado: [data_dict['RESUMEN']['IED CERRADO PAISES'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_ied]
            }
        else:
            data_ied_cerrado = {
                'Tipo de inversión': 'IED',
                inv_columna_t_1_cerrado: [0 for var in variables_ied],
                inv_columna_t_cerrado: [0 for var in variables_ied],
                inv_variacion_cerrado: [0 for var in variables_ied]
            }

        # Verificar y construir la tabla de IED corridas
        if dict_verif['ied_corrido'] == 'CON DATOS DE IED CORRIDO':
            if agrupacion == 'PAISES' and not variables_ied:  # Para asegurarnos de que variables_ied esté llenado si no se llenó en el bloque anterior
                variables_ied = list(data_dict['RESUMEN']['IED CORRIDO PAISES'].keys())
            data_ied_corrido = {
                inv_columna_t_1_corrido: [data_dict['RESUMEN']['IED CORRIDO PAISES'].get(var, [{'sum_inversion_t_1': 0}])[0]['sum_inversion_t_1'] for var in variables_ied],
                inv_columna_t_corrido: [data_dict['RESUMEN']['IED CORRIDO PAISES'].get(var, [{'sum_inversion_t': 0}])[0]['sum_inversion_t'] for var in variables_ied],
                inv_variacion_corrido: [data_dict['RESUMEN']['IED CORRIDO PAISES'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_ied]
            }
        else:
            data_ied_corrido = {
                inv_columna_t_1_corrido: [0 for var in variables_ied],
                inv_columna_t_corrido: [0 for var in variables_ied],
                inv_variacion_corrido: [0 for var in variables_ied]
            }

        # Crear la tabla con las variables y los datos del diccionario para ICE
        # Inicializar variables_ice
        variables_ice = ['TOTAL'] if agrupacion != 'PAISES' else []

        # Verificar y construir la tabla de ICE cerradas
        if dict_verif['ice_cerrado'] == 'CON DATOS DE ICE CERRADO':
            if agrupacion == 'PAISES':
                variables_ice = list(data_dict['RESUMEN']['ICE CERRADO PAISES'].keys())
            data_ice_cerrado = {
                'Tipo de inversión': 'ICE',
                inv_columna_t_1_cerrado: [data_dict['RESUMEN']['ICE CERRADO PAISES'].get(var, [{'sum_inversion_t_1': 0}])[0]['sum_inversion_t_1'] for var in variables_ice],
                inv_columna_t_cerrado: [data_dict['RESUMEN']['ICE CERRADO PAISES'].get(var, [{'sum_inversion_t': 0}])[0]['sum_inversion_t'] for var in variables_ice],
                inv_variacion_cerrado: [data_dict['RESUMEN']['ICE CERRADO PAISES'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_ice]
            }
        else:
            data_ice_cerrado = {
                'Tipo de inversión': 'ICE',
                inv_columna_t_1_cerrado: [0 for var in variables_ice],
                inv_columna_t_cerrado: [0 for var in variables_ice],
                inv_variacion_cerrado: [0 for var in variables_ice]
            }

        # Verificar y construir la tabla de ICE corridas
        if dict_verif['ice_corrido'] == 'CON DATOS DE ICE CORRIDO':
            if agrupacion == 'PAISES' and not variables_ice:  # Para asegurarnos de que variables_ice esté llenado si no se llenó en el bloque anterior
                variables_ice = list(data_dict['RESUMEN']['ICE CORRIDO PAISES'].keys())
            data_ice_corrido = {
                inv_columna_t_1_corrido: [data_dict['RESUMEN']['ICE CORRIDO PAISES'].get(var, [{'sum_inversion_t_1': 0}])[0]['sum_inversion_t_1'] for var in variables_ice],
                inv_columna_t_corrido: [data_dict['RESUMEN']['ICE CORRIDO PAISES'].get(var, [{'sum_inversion_t': 0}])[0]['sum_inversion_t'] for var in variables_ice],
                inv_variacion_corrido: [data_dict['RESUMEN']['ICE CORRIDO PAISES'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_ice]
            }
        else:
            data_ice_corrido = {
                inv_columna_t_1_corrido: [0 for var in variables_ice],
                inv_columna_t_corrido: [0 for var in variables_ice],
                inv_variacion_corrido: [0 for var in variables_ice]
            }

        # Unir los datos de IED cerrados y corridos
        data_ied = {**data_ied_cerrado, **data_ied_corrido}

        # Convertir el diccionario a DataFrame 
        tab_resumen_ied = pd.DataFrame(data_ied)

        # Unir los datos de ICE cerrados y corridos
        data_ice = {**data_ice_cerrado, **data_ice_corrido}

        # Convertir el diccionario a DataFrame 
        tab_resumen_ice = pd.DataFrame(data_ice)

        # Concatenar los DataFrames de inversión
        inv_df = pd.concat([tab_resumen_ied, tab_resumen_ice])
        tab_resumen_inv = format_columns_inversion(inv_df)

    # Turismo
    tur_columna_t_1_cerrado = turismo_params['cerrado']['T_1']
    tur_columna_t_cerrado = turismo_params['cerrado']['T']
    tur_variacion_cerrado = f"Variación (%) {transform_year_column_name(turismo_params['corrido']['T_1'])}"
    tur_columna_t_1_corrido = f"Ene - {turismo_params['corrido']['T_MONTH_NAME']} {turismo_params['corrido']['T_1']}"
    tur_columna_t_corrido = f"Ene - {turismo_params['corrido']['T_MONTH_NAME']} {turismo_params['corrido']['T']}"
    tur_variacion_corrido = f"Variación (%) Ene - {turismo_params['corrido']['T_MONTH_NAME']} {turismo_params['corrido']['T']}"

    # Crear la tabla con las variables y los datos del diccionario para turismo
    variables_turismo = ['TOTAL']

    # Inicializar data_turismo_cerrado y data_turismo_corrido
    data_turismo_cerrado = {}
    data_turismo_corrido = {}

    # Verificar y construir la tabla de turismo cerradas
    if dict_verif['turismo_cerrado'] == 'CON DATOS DE TURISMO CERRADO':
        data_turismo_cerrado = {
            'Variable': 'Viajeros',
            tur_columna_t_1_cerrado: [data_dict['RESUMEN']['TURISMO CERRADO PAISES'][var][0]['sum_turismo_t_1'] for var in variables_turismo],
            tur_columna_t_cerrado: [data_dict['RESUMEN']['TURISMO CERRADO PAISES'][var][0]['sum_turismo_t'] for var in variables_turismo],
            tur_variacion_cerrado: [data_dict['RESUMEN']['TURISMO CERRADO PAISES'][var][0]['diferencia_porcentual'] for var in variables_turismo]
        }
    else:
        data_turismo_cerrado = {
            'Variable': 'Viajeros',
            tur_columna_t_1_cerrado: [0 for var in variables_turismo],
            tur_columna_t_cerrado: [0 for var in variables_turismo],
            tur_variacion_cerrado: [0 for var in variables_turismo]
        }

    # Verificar y construir la tabla de turismo corridas
    if dict_verif['turismo_corrido'] == 'CON DATOS DE TURISMO CORRIDO':
        data_turismo_corrido = {
            tur_columna_t_1_corrido: [data_dict['RESUMEN']['TURISMO CORRIDO PAISES'][var][0]['sum_turismo_t_1'] for var in variables_turismo],
            tur_columna_t_corrido: [data_dict['RESUMEN']['TURISMO CORRIDO PAISES'][var][0]['sum_turismo_t'] for var in variables_turismo],
            tur_variacion_corrido: [data_dict['RESUMEN']['TURISMO CORRIDO PAISES'][var][0]['diferencia_porcentual'] for var in variables_turismo]
        }
    else:
        data_turismo_corrido = {
            tur_columna_t_1_corrido: [0 for var in variables_turismo],
            tur_columna_t_corrido: [0 for var in variables_turismo],
            tur_variacion_corrido: [0 for var in variables_turismo]
        }

    # Unir los datos cerrados y corridos
    data_turismo = {**data_turismo_cerrado, **data_turismo_corrido}

    # Convertir el diccionario a DataFrame 
    tab_resumen_tur = pd.DataFrame(data_turismo)
    tab_resumen_tur = format_columns_turismo(tab_resumen_tur)
    
    # Crear el texto de exportaciones
    
    # Obtener datos cuando existen o llenarlos con cero
    # Cerrado total
    if dict_verif['exportaciones_totales_cerrado'] == 'CON DATOS DE EXPORTACIONES TOTALES CERRADO':
        exportaciones_totales_cerrado = data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO']['Total'][0]
        exportaciones_total_cerrado = exportaciones_totales_cerrado['sum_usd_t']
        exportaciones_variacion_total_cerrado = exportaciones_totales_cerrado['diferencia_porcentual']
    else:
        exportaciones_total_cerrado = 0
        exportaciones_variacion_total_cerrado = 0
    # Corrido
    if dict_verif['exportaciones_totales_corrido'] == 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO':
        exportaciones_totales_corrido = data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO']['Total'][0]
        exportaciones_total_corrido = exportaciones_totales_corrido['sum_usd_t']
        exportaciones_variacion_total_corrido = exportaciones_totales_corrido['diferencia_porcentual']
    else:
        exportaciones_total_corrido = 0
        exportaciones_variacion_total_corrido = 0   

    # Cerrado NME
    if dict_verif['exportaciones_nme_cerrado'] == 'CON DATOS DE EXPORTACIONES NME CERRADO':
        exportaciones_no_minero_cerrado = data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO']['No Mineras'][0]
        exportaciones_nme_cerrado = exportaciones_no_minero_cerrado['sum_usd_t']
        exportaciones_variacion_nme_cerrado = exportaciones_no_minero_cerrado['diferencia_porcentual']
    else:
        exportaciones_nme_cerrado = 0
        exportaciones_variacion_nme_cerrado = 0
    # Corrido NME
    if dict_verif['exportaciones_nme_corrido'] == 'CON DATOS DE EXPORTACIONES NME CORRIDO':
        exportaciones_no_minero_corrido = data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO']['No Mineras'][0]
        exportaciones_nme_corrido= exportaciones_no_minero_corrido['sum_usd_t']
        exportaciones_variacion_nme_corrido = exportaciones_no_minero_corrido['diferencia_porcentual']
    else:
        exportaciones_nme_corrido = 0
        exportaciones_variacion_nme_corrido = 0

    # Conteo cerrado
    if dict_verif['exportaciones_conteo_cerrado'] == 'CON DATOS DE CONTEO CERRADO':
        periodo_cerrado = list(data_dict['RESUMEN']['CONTEO']['CERRADO'].keys())[0]
        num_empresas_cerrado = data_dict['RESUMEN']['CONTEO']['CERRADO'][periodo_cerrado]
    else:
        num_empresas_cerrado = 0

    # Conteo corrido
    if dict_verif['exportaciones_conteo_corrido'] == 'CON DATOS DE CONTEO CORRIDO':
        periodo_corrido = list(data_dict['RESUMEN']['CONTEO']['CORRIDO'].keys())[0]
        num_empresas_corrido = data_dict['RESUMEN']['CONTEO']['CORRIDO'][periodo_corrido]
    else: 
        num_empresas_corrido = 0

    # Texto por bullets de expotaciones
    if agrupacion in ['COLOMBIA', 'DEPARTAMENTOS']:
        texto_exportaciones_b1_cerrado = f"""En {export_params['cerrado']['T']}, {unidad} exportó al Mundo USD {format_number(exportaciones_total_cerrado / 1e6)} millones, {format_number(abs(exportaciones_variacion_total_cerrado))}% {variacion_palabra(exportaciones_variacion_total_cerrado)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b1_corrido = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} las exportaciones totales de {unidad} al Mundo suman USD {format_number(exportaciones_total_corrido / 1e6)} millones, {format_number(abs(exportaciones_variacion_total_corrido))}% {variacion_palabra(exportaciones_variacion_total_corrido)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b2_cerrado = f"""Las exportaciones no minero-energéticas de {unidad} al Mundo en {export_params['cerrado']['T']} registraron USD {format_number(exportaciones_nme_cerrado / 1e6)} millones, {format_number(abs(exportaciones_variacion_nme_cerrado))}% {variacion_palabra(exportaciones_variacion_nme_cerrado)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b2_corrido = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} las exportaciones no minero-energéticas de {unidad} al Mundo suman USD {format_number(exportaciones_nme_corrido / 1e6)} millones, {format_number(abs(exportaciones_variacion_nme_corrido))}% {variacion_palabra(exportaciones_variacion_nme_corrido)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b3_cerrado = f"""Durante {export_params['cerrado']['T']}, {format_number_no_decimal(num_empresas_cerrado)} empresas colombianas exportaron productos no minero-energéticos por montos superiores a USD 10.000."""
        texto_exportaciones_b3_corrido = f"""Entre entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']}, {format_number_no_decimal(num_empresas_corrido)} empresas colombianas exportaron productos no minero-energéticos por montos superiores a USD 10.000"""
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        texto_exportaciones_b1_cerrado = f"""En {export_params['cerrado']['T']}, Colombia exportó a {unidad} USD {format_number(exportaciones_total_cerrado / 1e6)} millones, {format_number(abs(exportaciones_variacion_total_cerrado))}% {variacion_palabra(exportaciones_variacion_total_cerrado)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b1_corrido = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} las exportaciones totales a {unidad} suman USD {format_number(exportaciones_total_corrido / 1e6)} millones, {format_number(abs(exportaciones_variacion_total_corrido))}% {variacion_palabra(exportaciones_variacion_total_corrido)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b2_cerrado = f"""Las exportaciones no minero-energéticas de Colombia a {unidad} en {export_params['cerrado']['T']} registraron USD {format_number(exportaciones_nme_cerrado / 1e6)} millones, {format_number(abs(exportaciones_variacion_nme_cerrado))}% {variacion_palabra(exportaciones_variacion_nme_cerrado)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b2_corrido = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} las exportaciones no minero-energéticas de Colombia a {unidad} suman USD {format_number(exportaciones_nme_corrido / 1e6)} millones, {format_number(abs(exportaciones_variacion_nme_corrido))}% {variacion_palabra(exportaciones_variacion_nme_corrido)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b3_cerrado = f"""Durante {export_params['cerrado']['T']}, {format_number_no_decimal(num_empresas_cerrado)} empresas colombianas exportaron productos no minero-energéticos a {unidad} por montos superiores a USD 10.000."""
        texto_exportaciones_b3_corrido = f"""Entre entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']}, {format_number_no_decimal(num_empresas_corrido)} empresas colombianas exportaron productos no minero-energéticos a {unidad} por montos superiores a USD 10.000."""
   
    # Texto por bullets de inversión
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        # Cerrado inversión
        if dict_verif['ied_cerrado'] == 'CON DATOS DE IED CERRADO':
            # Obtener datos de IED cerrado por países o total
            if agrupacion == 'PAISES':
                inversion_total_cerrado = data_dict['RESUMEN']['IED CERRADO PAISES'][list(data_dict['RESUMEN']['IED CERRADO PAISES'].keys())[0]][0]
            else:
                inversion_total_cerrado = data_dict['RESUMEN']['IED CERRADO PAISES']['TOTAL'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            inversion_total_cerrado = {'sum_inversion_t': 0, 'diferencia_porcentual': 0}

        if dict_verif['ice_cerrado'] == 'CON DATOS DE ICE CERRADO':
            # Obtener datos de ICE cerrado por países o total
            if agrupacion == 'PAISES':
                ice_total_cerrado = data_dict['RESUMEN']['ICE CERRADO PAISES'][list(data_dict['RESUMEN']['ICE CERRADO PAISES'].keys())[0]][0]
            else:
                ice_total_cerrado = data_dict['RESUMEN']['ICE CERRADO PAISES']['TOTAL'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            ice_total_cerrado = {'sum_inversion_t': 0, 'diferencia_porcentual': 0}

        # Corrido inversión
        if dict_verif['ied_corrido'] == 'CON DATOS DE IED CORRIDO':
            # Obtener datos de IED corrido por países o total
            if agrupacion == 'PAISES':
                inversion_total_corrido = data_dict['RESUMEN']['IED CORRIDO PAISES'][list(data_dict['RESUMEN']['IED CORRIDO PAISES'].keys())[0]][0]
            else:
                inversion_total_corrido = data_dict['RESUMEN']['IED CORRIDO PAISES']['TOTAL'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            inversion_total_corrido = {'sum_inversion_t': 0, 'diferencia_porcentual': 0}

        if dict_verif['ice_corrido'] == 'CON DATOS DE ICE CORRIDO':
            # Obtener datos de ICE corrido por países o total
            if agrupacion == 'PAISES':
                ice_total_corrido = data_dict['RESUMEN']['ICE CORRIDO PAISES'][list(data_dict['RESUMEN']['ICE CORRIDO PAISES'].keys())[0]][0]
            else:
                ice_total_corrido = data_dict['RESUMEN']['ICE CORRIDO PAISES']['TOTAL'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            ice_total_corrido = {'sum_inversion_t': 0, 'diferencia_porcentual': 0}

        # Texto por bullets de inversión
        if agrupacion in ['COLOMBIA']:
            # Generar texto para agrupación por Colombia
            texto_inversion_b1_cerrado = f"""En {inversion_params['cerrado']['T']}, Colombia registró flujos {inversion_palabra(inversion_total_cerrado['sum_inversion_t'])} de inversión extranjera directa (IED) del Mundo por USD {format_number(inversion_total_cerrado['sum_inversion_t'])} millones, {format_number(abs(inversion_total_cerrado['diferencia_porcentual']))}% {variacion_palabra(inversion_total_cerrado['diferencia_porcentual'])} con respecto al {inversion_params['cerrado']['T_1']}."""
            texto_inversion_b1_corrido = f"""En el {inversion_params['corrido']['T_TRIMESTER_NAME']} trimestre de {inversion_params['corrido']['T_YEAR']}, Colombia registró flujos {inversion_palabra(inversion_total_corrido['sum_inversion_t'])} de IED del Mundo por USD {format_number(inversion_total_corrido['sum_inversion_t'])} millones, {format_number(abs(inversion_total_corrido['diferencia_porcentual']))}% {variacion_palabra(inversion_total_corrido['diferencia_porcentual'])} con respecto al mismo periodo de {inversion_params['corrido']['T_1_YEAR']}."""
            texto_inversion_b2_cerrado = f"""En {inversion_params['cerrado']['T']}, se registraron flujos {inversion_palabra(ice_total_cerrado['sum_inversion_t'])} de inversión directa de Colombia en el exterior (ICE) en el Mundo por USD {format_number(ice_total_cerrado['sum_inversion_t'])} millones, {format_number(abs(ice_total_cerrado['diferencia_porcentual']))}% {variacion_palabra(ice_total_cerrado['diferencia_porcentual'])} con respecto al {inversion_params['cerrado']['T_1']}."""
            texto_inversion_b2_corrido = f"""En el {inversion_params['corrido']['T_TRIMESTER_NAME']} trimestre de {inversion_params['corrido']['T_YEAR']}, Colombia registró flujos {inversion_palabra(ice_total_corrido['sum_inversion_t'])} de ICE en el Mundo por USD {format_number(ice_total_corrido['sum_inversion_t'])} millones, {format_number(abs(ice_total_corrido['diferencia_porcentual']))}% {variacion_palabra(ice_total_corrido['diferencia_porcentual'])} con respecto al mismo periodo de {inversion_params['corrido']['T_1_YEAR']}."""

        if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            # Generar texto para otras agrupaciones (continentes, hubs, TLCs, países)
            texto_inversion_b1_cerrado = f"""En {inversion_params['cerrado']['T']}, Colombia registró flujos {inversion_palabra(inversion_total_cerrado['sum_inversion_t'])} de inversión extranjera directa (IED) de {unidad} por USD {format_number(inversion_total_cerrado['sum_inversion_t'])} millones, {format_number(abs(inversion_total_cerrado['diferencia_porcentual']))}% {variacion_palabra(inversion_total_cerrado['diferencia_porcentual'])} con respecto al {inversion_params['cerrado']['T_1']}."""
            texto_inversion_b1_corrido = f"""En el {inversion_params['corrido']['T_TRIMESTER_NAME']} trimestre de {inversion_params['corrido']['T_YEAR']}, Colombia registró flujos {inversion_palabra(inversion_total_corrido['sum_inversion_t'])} de IED de {unidad} por USD {format_number(inversion_total_corrido['sum_inversion_t'])} millones, {format_number(abs(inversion_total_corrido['diferencia_porcentual']))}% {variacion_palabra(inversion_total_corrido['diferencia_porcentual'])} con respecto al mismo periodo de {inversion_params['corrido']['T_1_YEAR']}."""
            texto_inversion_b2_cerrado = f"""En {inversion_params['cerrado']['T']}, se registraron flujos {inversion_palabra(ice_total_cerrado['sum_inversion_t'])} de inversión directa de Colombia en el exterior (ICE) en {unidad} por USD {format_number(ice_total_cerrado['sum_inversion_t'])} millones, {format_number(abs(ice_total_cerrado['diferencia_porcentual']))}% {variacion_palabra(ice_total_cerrado['diferencia_porcentual'])} con respecto al {inversion_params['cerrado']['T_1']}."""
            texto_inversion_b2_corrido = f"""En el {inversion_params['corrido']['T_TRIMESTER_NAME']} trimestre de {inversion_params['corrido']['T_YEAR']}, Colombia registró flujos {inversion_palabra(ice_total_corrido['sum_inversion_t'])} de ICE en {unidad} por USD {format_number(ice_total_corrido['sum_inversion_t'])} millones, {format_number(abs(ice_total_corrido['diferencia_porcentual']))}% {variacion_palabra(ice_total_corrido['diferencia_porcentual'])} con respecto al mismo periodo de {inversion_params['corrido']['T_1_YEAR']}."""


    # Obtener datos cuando existen o llenarlos con cero
    # Cerrado turismo
    if dict_verif['turismo_cerrado'] == 'CON DATOS DE TURISMO CERRADO':
        turismo_cerrado = data_dict['RESUMEN']['TURISMO CERRADO PAISES']['TOTAL'][0]
        turismo_cerrado_sum = turismo_cerrado['sum_turismo_t']
        turismo_variacion_cerrado = turismo_cerrado['diferencia_porcentual']
    else:
        turismo_cerrado_sum = 0
        turismo_variacion_cerrado = 0

    # Corrido turismo
    if dict_verif['turismo_corrido'] == 'CON DATOS DE TURISMO CORRIDO':
        turismo_corrido = data_dict['RESUMEN']['TURISMO CORRIDO PAISES']['TOTAL'][0]
        turismo_corrido_sum = turismo_corrido['sum_turismo_t']
        turismo_variacion_corrido = turismo_corrido['diferencia_porcentual']
    else:
        turismo_corrido_sum = 0
        turismo_variacion_corrido = 0
    
    # Texto por bullets de turismo
    if agrupacion in ['COLOMBIA', 'DEPARTAMENTOS']:
        texto_turismo_b1_cerrado = f"""En {turismo_params['cerrado']['T']}, {unidad} registró {format_number_no_decimal(turismo_cerrado_sum)} llegadas de turistas extranjeros, {format_number(abs(turismo_variacion_cerrado))}% {variacion_palabra(turismo_variacion_cerrado)} con respecto a {turismo_params['cerrado']['T_1']}."""
        texto_turismo_b2_corrido = f"""Entre enero y {turismo_params['corrido']['T_MONTH_NAME_FULL'].lower()} de {turismo_params['corrido']['T']}, {unidad} registró {format_number_no_decimal(turismo_corrido_sum)} llegadas de turistas extranjeros, {format_number(abs(turismo_variacion_corrido))}% {variacion_palabra(turismo_variacion_corrido)} con respecto a {turismo_params['corrido']['T_1']}."""

    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        texto_turismo_b1_cerrado = f"""En {turismo_params['cerrado']['T']}, Colombia registró {format_number_no_decimal(turismo_cerrado_sum)} llegadas de turistas extranjeros provenientes de {unidad}, {format_number(abs(turismo_variacion_cerrado))}% {variacion_palabra(turismo_variacion_cerrado)} con respecto a {turismo_params['cerrado']['T_1']}."""
        texto_turismo_b2_corrido = f"""Entre enero y {turismo_params['corrido']['T_MONTH_NAME_FULL'].lower()} de {turismo_params['corrido']['T']}, {unidad} registró {format_number_no_decimal(turismo_corrido_sum)} llegadas de turistas extranjeros provenientes de {unidad}, {format_number(abs(turismo_variacion_corrido))}% {variacion_palabra(turismo_variacion_corrido)} con respecto a {turismo_params['corrido']['T_1']}."""
    
    # Resultados
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        return {
            'tab_resumen_expo': tab_resumen_expo,
            'tab_resumen_inv': tab_resumen_inv,
            'tab_resumen_tur': tab_resumen_tur,
            'texto_exportaciones_b1_cerrado' : texto_exportaciones_b1_cerrado,
            'texto_exportaciones_b1_corrido' : texto_exportaciones_b1_corrido,
            'texto_exportaciones_b2_cerrado' : texto_exportaciones_b2_cerrado,
            'texto_exportaciones_b2_corrido' : texto_exportaciones_b2_corrido,
            'texto_exportaciones_b3_cerrado' : texto_exportaciones_b3_cerrado,
            'texto_exportaciones_b3_corrido' : texto_exportaciones_b3_corrido,
            'texto_inversion_b1_cerrado' : texto_inversion_b1_cerrado,
            'texto_inversion_b1_corrido' : texto_inversion_b1_corrido,
            'texto_inversion_b2_cerrado' : texto_inversion_b2_cerrado,
            'texto_inversion_b2_corrido' : texto_inversion_b2_corrido,
            'texto_turismo_b1_cerrado' : texto_turismo_b1_cerrado,
            'texto_turismo_b2_corrido' : texto_turismo_b2_corrido
        }
    if agrupacion in ['DEPARTAMENTOS']:
         return {
             'tab_resumen_expo': tab_resumen_expo,
             'tab_resumen_tur': tab_resumen_tur,
             'texto_exportaciones_b1_cerrado' : texto_exportaciones_b1_cerrado,
             'texto_exportaciones_b1_corrido' : texto_exportaciones_b1_corrido,
             'texto_exportaciones_b2_cerrado' : texto_exportaciones_b2_cerrado,
             'texto_exportaciones_b2_corrido' : texto_exportaciones_b2_corrido,
             'texto_exportaciones_b3_cerrado' : texto_exportaciones_b3_cerrado,
             'texto_exportaciones_b3_corrido' : texto_exportaciones_b3_corrido,
             'texto_turismo_b1_cerrado' : texto_turismo_b1_cerrado,
             'texto_turismo_b2_corrido' : texto_turismo_b2_corrido
     }
    

def crear_diccionario_cadenas(data):
    """
    Crea un diccionario donde las llaves son los valores únicos de la columna 'CADENA'
    y los valores son strings de los 'SUBSECTORES' correspondientes, separados por comas y terminados con un punto.

    Parámetros:
    data (pd.DataFrame): DataFrame con las columnas 'CADENA' y 'SUBSECTOR'.

    Retorna:
    dict: Diccionario con las cadenas y sus respectivos subsectoras concatenados en una string.
    """
    # Crear un diccionario vacío para almacenar los resultados
    diccionario = {}
    
    # Iterar sobre las filas del DataFrame
    for cadena in data['CADENA'].unique():
        # Obtener los subsectoras correspondientes a la cadena
        subsectoras = data[data['CADENA'] == cadena]['SUBSECTOR']
        # Concatenar los subsectoras en una sola string, separados por comas y con un punto al final
        subsectoras_str = ', '.join(subsectoras) + '.'
        # Asignar la cadena y sus subsectoras al diccionario
        diccionario[cadena] = subsectoras_str
    
    return diccionario

def crear_diccionario_turismo(data):
    """
    Crea un diccionario con dos strings de SECTOR y SUBSECTOR concatenados, separados por comas y terminados con un punto.

    Parámetros:
    data (pd.DataFrame): DataFrame con las columnas 'SECTOR' y 'SUBSECTOR'.

    Retorna:
    dict: Diccionario con la concatenación de los datos.
    """
    # Crear un diccionario vacío para almacenar los resultados
    diccionario = {}
    
    # SECTOR
    # Obtener los sectores de turismo
    sectores = data['SECTOR'].dropna().unique()  # Evitar duplicados y valores nulos
    # Concatenar los sectores en una sola string, separados por comas y con un punto al final
    sectores_str = ', '.join(sectores) + '.'
    # Asignar al diccionario
    diccionario['SECTOR'] = sectores_str

    # SUBSECTOR
    # Obtener los subsectores de turismo
    subsectores = data['SUBSECTOR'].dropna().unique()  # Evitar duplicados y valores nulos
    # Concatenar los subsectores en una sola string, separados por comas y con un punto al final
    subsectores_str = ', '.join(subsectores) + '.'
    # Asignar al diccionario
    diccionario['SUBSECTOR'] = subsectores_str
    
    return diccionario


def process_data(session, agrupacion, continentes=None, paises=None, hubs=None, tlcs=None, departamentos=None, umbral=None):
    """
    Esta función extrae y organiza datos de exportaciones desde una base de datos en Snowflake, 
    luego cambia los nombres de las columnas según los requisitos especificados.

    Parámetros:
    - sesion: sesión de Snowflake.
    - agrupacion: el nivel de agrupación para filtrar los datos.
    - unidad: la unidad de medida para filtrar los datos.
    - umbral: valor USD exportado mínimo exportado para contar la empresa.

    Retorna:
    Un diccionario con los DataFrames procesados y las columnas renombradas.
    """
    # Geo Parámetros 
    geo_params = get_data_parametros(session, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)

    # Parámetros para los datos de exportaciones
    AGRUPACION = geo_params['AGRUPACION']
    UNIDAD = geo_params['UNIDAD'][0]

    #################################
    # INDICADOR DE PRESENCIA DE DATOS
    #################################

    dict_verificacion = verif_ejes(session, geo_params)
    
    # Obtener los parámetros T y T_1 para año cerrado y año corrido
    params = get_parameters_exportaciones(session)
    params_inversion = get_parameters_inversion(session)
    params_turismo = get_parameters_turismo(session)

    # Llamar a la función get_data() para obtener los datos de exportaciones
    data_dict = get_data(session, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)

    # Procesar las tablas relevantes y cambiar los nombres de las columnas según corresponda
    processed_data = {}

    # Obtener nombres en limpio de países
    df_paises = obtener_paises_correlativa(session)
    # Obtener nombres en limpio de departamentos
    df_departamentos = obtener_departamentos_correlativa(session)
    # Obtener nombres en limpio de municipios
    df_municipios = obtener_municipios_correlativa(session)

    ###############
    # Exportaciones
    ###############

    # Definir el diccionario para renombrar las columnas de categoría
    column_names_dict = {
        'TIPO': 'Tipo de exportación',
        'CONTINENTE': 'Continente',
        'DEPARTAMENTOS': 'Departamento de origen',
        'HUBs': 'HUB',
        'PAIS': 'País destino',
        'SECTORES': 'Sector',
        'SUBSECTORES': 'Subsector',
        'TLCS': 'Tratados de Libre Comercio'
    }

    # Renombrar las columnas de 'CATEGORIAS CERRADO' y 'CATEGORIAS CORRIDO'
    for key in ['CATEGORIAS CERRADO', 'CATEGORIAS CORRIDO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar los nombres de los países con el nombre real de la ONU
            if sub_key == 'PAIS':
                # Realizar la unión para reemplazar "Categoria" con "COUNTRY_OR_AREA_UNSD"
                df = df.merge(df_paises[['PAIS_LLAVE_EXPORTACIONES', 'COUNTRY_OR_AREA_UNSD']],
                              left_on='CATEGORIA',
                              right_on='PAIS_LLAVE_EXPORTACIONES',
                              how='left')
                # Reemplazar valores en "CATEGORIA"
                df['CATEGORIA'] = df.apply(
                    lambda row: row['COUNTRY_OR_AREA_UNSD'] if pd.notnull(row['COUNTRY_OR_AREA_UNSD']) else row['CATEGORIA'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['PAIS_LLAVE_EXPORTACIONES', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)
            # Renombrar la columna 'CATEGORIA' según el diccionario de nombres de columnas
            if 'CATEGORIA' in df.columns and sub_key in column_names_dict:
                df.rename(columns={'CATEGORIA': column_names_dict[sub_key]}, inplace=True)
            # Renombrar las columnas relacionadas con valores USD y participación
            if 'SUMA_USD_T_1' in df.columns and 'SUMA_USD_T' in df.columns:
                df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
                if key == 'CATEGORIAS CERRADO':
                    df.rename(columns={
                        'SUMA_USD_T_1': f"{params['cerrado']['T_1']} (USD FOB)",
                        'SUMA_USD_T': f"{params['cerrado']['T']} (USD FOB)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}"
                    }, inplace=True)
                elif key == 'CATEGORIAS CORRIDO':
                    df.rename(columns={
                        'SUMA_USD_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (USD FOB)",
                        'SUMA_USD_T': f"{transform_year_column_name(params['corrido']['T'])} (USD FOB)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                    }, inplace=True)
            format_columns_exportaciones(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict

    # Renombrar la columna 'CATEGORIA' y las columnas relacionadas con valores USD y peso en 'TIPOS' y 'TIPOS PESO'
    for key in ['TIPOS', 'TIPOS PESO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar la columna 'CATEGORIA' a 'Tipo de exportación'
            if 'CATEGORIA' in df.columns:
                df.rename(columns={'CATEGORIA': 'Tipo de exportación'}, inplace=True)
            # Renombrar las columnas relacionadas con valores USD
            if 'SUMA_USD_T_1' in df.columns and 'SUMA_USD_T' in df.columns:
                df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
                if sub_key == 'ST_CATEGORIAS_CERRADO':
                    df.rename(columns={
                        'SUMA_USD_T_1': f"{params['cerrado']['T_1']} (USD FOB)",
                        'SUMA_USD_T': f"{params['cerrado']['T']} (USD FOB)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}"
                    }, inplace=True)
                elif sub_key == 'ST_CATEGORIAS_CORRIDO':
                    df.rename(columns={
                        'SUMA_USD_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (USD FOB)",
                        'SUMA_USD_T': f"{transform_year_column_name(params['corrido']['T'])} (USD FOB)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                    }, inplace=True)
            # Renombrar las columnas relacionadas con peso y convertir los valores a toneladas
            if 'SUMA_PESO_T_1' in df.columns and 'SUMA_PESO_T' in df.columns:
                df['SUMA_PESO_T_1'] = df['SUMA_PESO_T_1'] / 1000  # Convertir a toneladas
                df['SUMA_PESO_T'] = df['SUMA_PESO_T'] / 1000  # Convertir a toneladas
                df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
                if sub_key == 'ST_CATEGORIAS_PESO_CERRADO':
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{params['cerrado']['T_1']} (TONELADAS)",
                        'SUMA_PESO_T': f"{params['cerrado']['T']} (TONELADAS)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}"
                    }, inplace=True)
                elif sub_key == 'ST_CATEGORIAS_PESO_CORRIDO':
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (TONELADAS)",
                        'SUMA_PESO_T': f"{transform_year_column_name(params['corrido']['T'])} (TONELADAS)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                    }, inplace=True)
            format_columns_exportaciones(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict

    # Renombrar la columna 'CATEGORIA' a 'NIT' en 'EMPRESAS' y las columnas relacionadas con valores USD
    sub_dict_empresas = data_dict['EMPRESAS']
    processed_sub_dict_empresas = {}
    for sub_key, df in sub_dict_empresas.items():
        if 'CATEGORIA' in df.columns:
            df.rename(columns={'CATEGORIA': 'NIT'}, inplace=True)
            df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
            df.rename(columns={'RAZON_SOCIAL': 'Razón Social'}, inplace=True)
            df.rename(columns={'SECTOR_ESTRELLA': 'Sector'}, inplace=True)
        if 'SUMA_USD_T_1' in df.columns and 'SUMA_USD_T' in df.columns:
            if sub_key == 'ST_NIT_CERRADO':
                df.rename(columns={
                    'SUMA_USD_T_1': f"{params['cerrado']['T_1']} (USD FOB)",
                    'SUMA_USD_T': f"{params['cerrado']['T']} (USD FOB)",
                    'PARTICIPACION_T' : f"Participación (%) {params['cerrado']['T']}"
                }, inplace=True)
            elif sub_key == 'ST_NIT_CORRIDO':
                df.rename(columns={
                    'SUMA_USD_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (USD FOB)",
                    'SUMA_USD_T': f"{transform_year_column_name(params['corrido']['T'])} (USD FOB)",
                    'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                }, inplace=True)
        format_columns_exportaciones(df)  # Aplicar formato
        processed_sub_dict_empresas[sub_key] = df
    processed_data['EMPRESAS'] = processed_sub_dict_empresas

    # Copiar 'CONTEO EMPRESAS' sin cambios
    processed_data['CONTEO EMPRESAS'] = data_dict['CONTEO EMPRESAS']

    ###########
    # Inversión
    ###########
    
    # Colombia

    # Definir el diccionario para renombrar las columnas de categoría
    column_names_dict_inversion = {
        'IED ACTIVIDADES COLOMBIA': 'Actividad económica',
        'IED PAISES': 'País',
        'ICE PAISES': 'País',
        'IED TOTAL' : 'Agrupación de países',
        'ICE TOTAL' : 'Agrupación de países'
    }
    # Actividades solo es válida para Colombia
    if agrupacion == 'COLOMBIA':
        for key in ['IED ACTIVIDADES COLOMBIA']:
            sub_dict = data_dict[key]
            processed_sub_dict = {}
            for sub_key, df in sub_dict.items():
                # Renombrar la columna 'UNIDAD' según el diccionario de nombres de columnas de inversión
                if 'UNIDAD' in df.columns and key in column_names_dict_inversion:
                    df.rename(columns={'UNIDAD': column_names_dict_inversion[key]}, inplace=True)  
                if sub_key in ['ied_cerrado', 'ice_cerrado']:
                    df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{params_inversion['cerrado']['T_1']} (USD millones)",
                            'SUMA_INVERSION_T': f"{params_inversion['cerrado']['T']} (USD millones)",
                            'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {params_inversion['cerrado']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) {params_inversion['cerrado']['T']}"
                        }, inplace=True)
                elif sub_key in ['ied_corrido', 'ice_corrido']:
                        df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{transform_year_column_name(params_inversion['corrido']['T_1'])} (USD millones)",
                            'SUMA_INVERSION_T': f"{transform_year_column_name(params_inversion['corrido']['T'])} (USD millones)",
                            'DIFERENCIA_PORCENTUAL': 'Variación (%)',
                            'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params_inversion['corrido']['T'])}"
                        }, inplace=True)
                format_columns_inversion(df)  # Aplicar formato
                processed_sub_dict[sub_key] = df
            processed_data[key] = processed_sub_dict
    

    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        for key in ['IED PAISES', 'ICE PAISES']:
            sub_dict = data_dict[key]
            processed_sub_dict = {}
            for sub_key, df in sub_dict.items():
                # Realizar la unión para reemplazar "UNIDAD" con "COUNTRY_OR_AREA_UNSD"
                df = df.merge(df_paises[['PAIS_INVERSION_BANREP', 'COUNTRY_OR_AREA_UNSD']],
                              left_on='UNIDAD',
                              right_on='PAIS_INVERSION_BANREP',
                              how='left')
                # Reemplazar valores en "UNIDAD"
                df['UNIDAD'] = df.apply(
                    lambda row: row['COUNTRY_OR_AREA_UNSD'] if pd.notnull(row['COUNTRY_OR_AREA_UNSD']) else row['UNIDAD'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['PAIS_INVERSION_BANREP', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)                              
                # Renombrar la columna 'UNIDAD' según el diccionario de nombres de columnas de inversión
                if 'UNIDAD' in df.columns and key in column_names_dict_inversion:
                    df.rename(columns={'UNIDAD': column_names_dict_inversion[key]}, inplace=True)  
                if sub_key in ['ied_cerrado', 'ice_cerrado']:
                    df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{params_inversion['cerrado']['T_1']} (USD millones)",
                            'SUMA_INVERSION_T': f"{params_inversion['cerrado']['T']} (USD millones)",
                            'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {params_inversion['cerrado']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) {params_inversion['cerrado']['T']}"
                        }, inplace=True)
                elif sub_key in ['ied_corrido', 'ice_corrido']:
                        df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{transform_year_column_name(params_inversion['corrido']['T_1'])} (USD millones)",
                            'SUMA_INVERSION_T': f"{transform_year_column_name(params_inversion['corrido']['T'])} (USD millones)",
                            'DIFERENCIA_PORCENTUAL': 'Variación (%)',
                            'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params_inversion['corrido']['T'])}"
                        }, inplace=True)
                format_columns_inversion(df)  # Aplicar formato
                processed_sub_dict[sub_key] = df
            processed_data[key] = processed_sub_dict

    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        for key in ['IED TOTAL', 'ICE TOTAL']:
            sub_dict = data_dict[key]
            processed_sub_dict = {}
            for sub_key, df in sub_dict.items():
                if key == 'IED TOTAL':
                    df.iloc[0, df.columns.get_loc('UNIDAD')] = f"Total IED de {UNIDAD} en Colombia"
                if key == 'ICE TOTAL':
                    df.iloc[0, df.columns.get_loc('UNIDAD')] = f"Total ICE de Colombia en {UNIDAD}"
                if 'UNIDAD' in df.columns and key in column_names_dict_inversion:
                    # Renombrar la columna 'UNIDAD' según el diccionario de nombres de columnas de inversión
                    df.rename(columns={'UNIDAD': column_names_dict_inversion[key]}, inplace=True)  
                if sub_key in ['ied_cerrado_total', 'ice_cerrado_total']:
                    df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{params_inversion['cerrado']['T_1']} (USD millones)",
                            'SUMA_INVERSION_T': f"{params_inversion['cerrado']['T']} (USD millones)",
                            'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {params_inversion['cerrado']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) {params_inversion['cerrado']['T']}"
                        }, inplace=True)
                elif sub_key in ['ied_corrido_total', 'ice_corrido_total']:
                        df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{transform_year_column_name(params_inversion['corrido']['T_1'])} (USD millones)",
                            'SUMA_INVERSION_T': f"{transform_year_column_name(params_inversion['corrido']['T'])} (USD millones)",
                            'DIFERENCIA_PORCENTUAL': 'Variación (%)',
                            'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params_inversion['corrido']['T'])}"
                        }, inplace=True)
                if agrupacion == 'PAISES':
                    df = df.iloc[[0], 1:]
                format_columns_inversion(df)  # Aplicar formato
                processed_sub_dict[sub_key] = df
            processed_data[key] = processed_sub_dict
    #########
    # Turismo
    #########
    
    # Definir el diccionario para renombrar las columnas de categoría
    column_names_dict_turismo = {
        'PAIS_RESIDENCIA' : 'País de residencia', 
        'DPTO_HOSPEDAJE' : 'Departamento de hospedaje', 
        'CIUDAD_HOSPEDAJE' : 'Ciudad de hospedaje', 
        'DESCRIPCION_GENERO' : 'Género', 
        'MOVC_NOMBRE' : 'Motivo de viaje'
    }
    # Datos de cerrado
    for key in ['TURISMO CERRADO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar la columna principal según el diccionario de nombres de columnas
            if sub_key in column_names_dict_turismo:
                first_column_name = df.columns[0]
                df.rename(columns={first_column_name: column_names_dict_turismo[sub_key]}, inplace=True)
                df.rename(columns={
                            'SUMA_TURISMO_T_1': f"{params_turismo['cerrado']['T_1']}",
                            'SUMA_TURISMO_T': f"{params_turismo['cerrado']['T']}",
                            'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {params_turismo['cerrado']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) {params_turismo['cerrado']['T']}"
                        }, inplace=True)
            # Usar nombres de países
            if sub_key == 'PAIS_RESIDENCIA':
                df = df.merge(df_paises[['PAIS_CODIGO_TURISMO', 'COUNTRY_OR_AREA_UNSD']],
                              left_on='País de residencia',
                              right_on='PAIS_CODIGO_TURISMO',
                              how='left')
                # Reemplazar valores
                df['País de residencia'] = df.apply(
                    lambda row: row['COUNTRY_OR_AREA_UNSD'] if pd.notnull(row['COUNTRY_OR_AREA_UNSD']) else row['País de residencia'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['PAIS_CODIGO_TURISMO', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)

            # Usar nombres de departamentos
            if sub_key == 'DPTO_HOSPEDAJE':
                df = df.merge(df_departamentos[['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN']],
                              left_on='Departamento de hospedaje',
                              right_on='COD_DIAN_DEPARTAMENTO',
                              how='left')
                # Reemplazar valores en "Departamento de hospedaje"
                df['Departamento de hospedaje'] = df.apply(
                    lambda row: row['DEPARTAMENTO_DIAN'] if pd.notnull(row['DEPARTAMENTO_DIAN']) else row['Departamento de hospedaje'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN'], axis=1, inplace=True)

            # Usar nombres de ciudades    
            if sub_key == 'CIUDAD_HOSPEDAJE':
                df = df.merge(df_municipios[['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE']],
                              left_on='Ciudad de hospedaje',
                              right_on='COD_DANE_MUNICIPIO',
                              how='left')
                # Reemplazar valores en "Ciudad de hospedaje"
                df['Ciudad de hospedaje'] = df.apply(
                    lambda row: row['MUNICIPIO_DANE'] if pd.notnull(row['MUNICIPIO_DANE']) else row['Ciudad de hospedaje'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE'], axis=1, inplace=True)
            format_columns_turismo(df)  # Aplicar formato    
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict
    # Datos de corrido
    for key in ['TURISMO CORRIDO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar la columna principal según el diccionario de nombres de columnas
            if sub_key in column_names_dict_turismo:
                first_column_name = df.columns[0]
                df.rename(columns={first_column_name: column_names_dict_turismo[sub_key]}, inplace=True)
                df.rename(columns={
                            'SUMA_TURISMO_T_1': f"Ene - {params_turismo['corrido']['T_MONTH_NAME']} {params_turismo['corrido']['T_1']}",
                            'SUMA_TURISMO_T': f"Ene - {params_turismo['corrido']['T_MONTH_NAME']} {params_turismo['corrido']['T']}",
                            'DIFERENCIA_ABSOLUTA' : 'Diferencia (turistas)',
                            'DIFERENCIA_PORCENTUAL': f"Variación (%) Ene - {params_turismo['corrido']['T_MONTH_NAME']} {params_turismo['corrido']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) Ene - {params_turismo['corrido']['T_MONTH_NAME']} {params_turismo['corrido']['T']}"
                        }, inplace=True)
            # Usar nombres de países
            if sub_key == 'PAIS_RESIDENCIA':
                # Realizar la unión para reemplazar "País de residencia" con "COUNTRY_OR_AREA_UNSD"
                df = df.merge(df_paises[['PAIS_CODIGO_TURISMO', 'COUNTRY_OR_AREA_UNSD']],
                              left_on='País de residencia',
                              right_on='PAIS_CODIGO_TURISMO',
                              how='left')
                # Reemplazar valores en "UNIDAD"
                df['País de residencia'] = df.apply(
                    lambda row: row['COUNTRY_OR_AREA_UNSD'] if pd.notnull(row['COUNTRY_OR_AREA_UNSD']) else row['País de residencia'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['PAIS_CODIGO_TURISMO', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)
            # Usar nombres de departamentos
            if sub_key == 'DPTO_HOSPEDAJE':
                df = df.merge(df_departamentos[['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN']],
                              left_on='Departamento de hospedaje',
                              right_on='COD_DIAN_DEPARTAMENTO',
                              how='left')
                # Reemplazar valores en "Departamento de hospedaje"
                df['Departamento de hospedaje'] = df.apply(
                    lambda row: row['DEPARTAMENTO_DIAN'] if pd.notnull(row['DEPARTAMENTO_DIAN']) else row['Departamento de hospedaje'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN'], axis=1, inplace=True)
            # Usar nombres de ciudades    
            if sub_key == 'CIUDAD_HOSPEDAJE':
                df = df.merge(df_municipios[['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE']],
                              left_on='Ciudad de hospedaje',
                              right_on='COD_DANE_MUNICIPIO',
                              how='left')
                # Reemplazar valores en "Ciudad de hospedaje"
                df['Ciudad de hospedaje'] = df.apply(
                    lambda row: row['MUNICIPIO_DANE'] if pd.notnull(row['MUNICIPIO_DANE']) else row['Ciudad de hospedaje'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE'], axis=1, inplace=True)
            format_columns_turismo(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict


    #################
    # Hoja de resumen
    #################
    if AGRUPACION == 'COLOMBIA':
        dict_resumen = resumen_datos(data_dict, agrupacion, 'Colombia', params, params_inversion, params_turismo, dict_verificacion)
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'DEPARTAMENTOS']:
        dict_resumen = resumen_datos(data_dict, agrupacion, UNIDAD, params, params_inversion, params_turismo, dict_verificacion)
    if AGRUPACION == 'PAISES':
        nombre = geo_params['NOMBRE PAIS'][0]
        dict_resumen = resumen_datos(data_dict, agrupacion, nombre, params, params_inversion, params_turismo, dict_verificacion)
      
    # Agregar al diccionario
    processed_data['RESUMEN'] = dict_resumen

    ##############
    # CONECTIVIDAD
    ##############
    if AGRUPACION in ['DEPARTAMENTOS']:
        if (dict_verificacion['conectividad'] == "CON DATOS DE CONECTIVIDAD"):
            processed_data['CONECTIVIDAD'] = data_dict['CONECTIVIDAD']

    ###############
    # OPORTUNIDADES
    ###############
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'DEPARTAMENTOS', 'COLOMBIA']:
        # Exportación  
        if (dict_verificacion['oportunidades_exportacion'] == "CON OPORTUNIDADES"):
            texto_exportaciones_oportunidades = crear_diccionario_cadenas(data_dict['OPORTUNIDADES']['EXPORTACIONES'])
            # Agregar al diccionario de resultados
            processed_data['OPORTUNIDADES_EXPORTACIONES'] = texto_exportaciones_oportunidades

        # Inversión
        if (dict_verificacion['oportunidades_inversion'] == "CON OPORTUNIDADES"):
            texto_inversion_oportunidades = crear_diccionario_cadenas(data_dict['OPORTUNIDADES']['INVERSION'])
            # Agregar al diccionario de resultados
            processed_data['OPORTUNIDADES_INVERSION'] = texto_inversion_oportunidades

        # Turismo
        if (dict_verificacion['oportunidades_turismo'] == "CON OPORTUNIDADES"):
            # Crear texto
            resultado = crear_diccionario_turismo(data_dict['OPORTUNIDADES']['TURISMO'])
            # Seleccionar resultados
            texto_turismo_principales = resultado['SECTOR']
            texto_turismo_nichos = resultado['SUBSECTOR']
            # Agregar al diccionario de resultados
            processed_data['TURISMO_PRINCIPAL'] = texto_turismo_principales
            processed_data['TURISMO_NICHOS'] = texto_turismo_nichos
    
    # PESO POR MEDIOS DE TRANSPORTE
    for key in ['MEDIOS PESO MINERO', 'MEDIOS PESO NO MINERO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar la columna 'CATEGORIA' a 'Medio de transporte'
            if 'CATEGORIA' in df.columns:
                df.rename(columns={'CATEGORIA': 'Medio de transporte'}, inplace=True)
            # Renombrar las columnas relacionadas con peso y convertir los valores a toneladas
            if 'SUMA_PESO_T_1' in df.columns and 'SUMA_PESO_T' in df.columns:
                df['SUMA_PESO_T_1'] = df['SUMA_PESO_T_1'] / 1000  # Convertir a toneladas
                df['SUMA_PESO_T'] = df['SUMA_PESO_T'] / 1000  # Convertir a toneladas
                if sub_key == 'ST_CATEGORIAS_PESO_CERRADO':
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{params['cerrado']['T_1']} (TONELADAS)",
                        'SUMA_PESO_T': f"{params['cerrado']['T']} (TONELADAS)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}",
                        'PARTICIPACION_T_1' : f"Participación (%) {transform_year_column_name(params['cerrado']['T_1'])}"
                    }, inplace=True)
                elif sub_key == 'ST_CATEGORIAS_PESO_CORRIDO':
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (TONELADAS)",
                        'SUMA_PESO_T': f"{transform_year_column_name(params['corrido']['T'])} (TONELADAS)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}",
                        'PARTICIPACION_T_1' : f"Participación (%) {transform_year_column_name(params['corrido']['T_1'])}"
                    }, inplace=True)
            format_columns_exportaciones(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict

    return processed_data
     

def process_data_excel(session, agrupacion, continentes=None, paises=None, hubs=None, tlcs=None, departamentos=None, umbral=None):
    """
    Esta función extrae y organiza datos de exportaciones desde una base de datos en Snowflake, 
    luego cambia los nombres de las columnas según los requisitos especificados.

    La función tiene como objetivo transformar los datos y dejarlos listos para se exportados a Excel

    Parámetros:
    - sesion: sesión de Snowflake.
    - agrupacion: el nivel de agrupación para filtrar los datos.
    - unidad: la unidad de medida para filtrar los datos.
    - umbral: valor USD exportado mínimo exportado para contar la empresa.

    Retorna:
    Un diccionario con los DataFrames procesados y las columnas renombradas.
    """
    # Geo Parámetros 
    geo_params = get_data_parametros(session, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)

    # Parámetros para los datos de exportaciones
    AGRUPACION = geo_params['AGRUPACION']
    UNIDAD = geo_params['UNIDAD'][0]

    #################################
    # INDICADOR DE PRESENCIA DE DATOS
    #################################

    dict_verificacion = verif_ejes(session, geo_params)
    
    # Obtener los parámetros T y T_1 para año cerrado y año corrido
    params = get_parameters_exportaciones(session)
    params_inversion = get_parameters_inversion(session)
    params_turismo = get_parameters_turismo(session)

    # Llamar a la función get_data() para obtener los datos de exportaciones
    data_dict = get_data(session, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)

    # Procesar las tablas relevantes y cambiar los nombres de las columnas según corresponda
    processed_data = {}

    # Obtener nombres en limpio de países
    df_paises = obtener_paises_correlativa(session)
    # Obtener nombres en limpio de departamentos
    df_departamentos = obtener_departamentos_correlativa(session)
    # Obtener nombres en limpio de municipios
    df_municipios = obtener_municipios_correlativa(session)

    ###############
    # Exportaciones
    ###############

    # Definir el diccionario para renombrar las columnas de categoría
    column_names_dict = {
        'TIPO': 'Tipo de exportación',
        'CONTINENTE': 'Continente',
        'DEPARTAMENTOS': 'Departamento de origen',
        'HUBs': 'HUB',
        'PAIS': 'País destino',
        'SECTORES': 'Sector',
        'SUBSECTORES': 'Subsector',
        'TLCS': 'Tratados de Libre Comercio'
    }

    # Renombrar las columnas de 'CATEGORIAS CERRADO' y 'CATEGORIAS CORRIDO'
    for key in ['CATEGORIAS CERRADO', 'CATEGORIAS CORRIDO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar los nombres de los países con el nombre real de la ONU
            if sub_key == 'PAIS':
                # Realizar la unión para reemplazar "Categoria" con "COUNTRY_OR_AREA_UNSD"
                df = df.merge(df_paises[['PAIS_LLAVE_EXPORTACIONES', 'COUNTRY_OR_AREA_UNSD']],
                              left_on='CATEGORIA',
                              right_on='PAIS_LLAVE_EXPORTACIONES',
                              how='left')
                # Reemplazar valores en "CATEGORIA"
                df['CATEGORIA'] = df.apply(
                    lambda row: row['COUNTRY_OR_AREA_UNSD'] if pd.notnull(row['COUNTRY_OR_AREA_UNSD']) else row['CATEGORIA'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['PAIS_LLAVE_EXPORTACIONES', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)
            # Renombrar la columna 'CATEGORIA' según el diccionario de nombres de columnas
            if 'CATEGORIA' in df.columns and sub_key in column_names_dict:
                df.rename(columns={'CATEGORIA': column_names_dict[sub_key]}, inplace=True)
            # Renombrar las columnas relacionadas con valores USD y participación
            if 'SUMA_USD_T_1' in df.columns and 'SUMA_USD_T' in df.columns:
                df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
                if key == 'CATEGORIAS CERRADO':
                    df.rename(columns={
                        'SUMA_USD_T_1': f"{params['cerrado']['T_1']} (USD FOB)",
                        'SUMA_USD_T': f"{params['cerrado']['T']} (USD FOB)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}"
                    }, inplace=True)
                elif key == 'CATEGORIAS CORRIDO':
                    df.rename(columns={
                        'SUMA_USD_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (USD FOB)",
                        'SUMA_USD_T': f"{transform_year_column_name(params['corrido']['T'])} (USD FOB)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                    }, inplace=True)
            format_columns_exportaciones_excel(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict

    # Renombrar la columna 'CATEGORIA' y las columnas relacionadas con valores USD y peso en 'TIPOS' y 'TIPOS PESO'
    for key in ['TIPOS', 'TIPOS PESO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar la columna 'CATEGORIA' a 'Tipo de exportación'
            if 'CATEGORIA' in df.columns:
                df.rename(columns={'CATEGORIA': 'Tipo de exportación'}, inplace=True)
            # Renombrar las columnas relacionadas con valores USD
            if 'SUMA_USD_T_1' in df.columns and 'SUMA_USD_T' in df.columns:
                df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
                if sub_key == 'ST_CATEGORIAS_CERRADO':
                    df.rename(columns={
                        'SUMA_USD_T_1': f"{params['cerrado']['T_1']} (USD FOB)",
                        'SUMA_USD_T': f"{params['cerrado']['T']} (USD FOB)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}"
                    }, inplace=True)
                elif sub_key == 'ST_CATEGORIAS_CORRIDO':
                    df.rename(columns={
                        'SUMA_USD_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (USD FOB)",
                        'SUMA_USD_T': f"{transform_year_column_name(params['corrido']['T'])} (USD FOB)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                    }, inplace=True)
            # Renombrar las columnas relacionadas con peso y convertir los valores a toneladas
            if 'SUMA_PESO_T_1' in df.columns and 'SUMA_PESO_T' in df.columns:
                df['SUMA_PESO_T_1'] = df['SUMA_PESO_T_1'] / 1000  # Convertir a toneladas
                df['SUMA_PESO_T'] = df['SUMA_PESO_T'] / 1000  # Convertir a toneladas
                df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
                if sub_key == 'ST_CATEGORIAS_PESO_CERRADO':
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{params['cerrado']['T_1']} (TONELADAS)",
                        'SUMA_PESO_T': f"{params['cerrado']['T']} (TONELADAS)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}"
                    }, inplace=True)
                elif sub_key == 'ST_CATEGORIAS_PESO_CORRIDO':
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (TONELADAS)",
                        'SUMA_PESO_T': f"{transform_year_column_name(params['corrido']['T'])} (TONELADAS)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                    }, inplace=True)
            format_columns_exportaciones_excel(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict

    # Renombrar la columna 'CATEGORIA' a 'NIT' en 'EMPRESAS' y las columnas relacionadas con valores USD
    sub_dict_empresas = data_dict['EMPRESAS']
    processed_sub_dict_empresas = {}
    for sub_key, df in sub_dict_empresas.items():
        if 'CATEGORIA' in df.columns:
            df.rename(columns={'CATEGORIA': 'NIT'}, inplace=True)
            df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
            df.rename(columns={'RAZON_SOCIAL': 'Razón Social'}, inplace=True)
            df.rename(columns={'SECTOR_ESTRELLA': 'Sector'}, inplace=True)
        if 'SUMA_USD_T_1' in df.columns and 'SUMA_USD_T' in df.columns:
            if sub_key == 'ST_NIT_CERRADO':
                df.rename(columns={
                    'SUMA_USD_T_1': f"{params['cerrado']['T_1']} (USD FOB)",
                    'SUMA_USD_T': f"{params['cerrado']['T']} (USD FOB)",
                    'PARTICIPACION_T' : f"Participación (%) {params['cerrado']['T']}"
                }, inplace=True)
            elif sub_key == 'ST_NIT_CORRIDO':
                df.rename(columns={
                    'SUMA_USD_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (USD FOB)",
                    'SUMA_USD_T': f"{transform_year_column_name(params['corrido']['T'])} (USD FOB)",
                    'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                }, inplace=True)
        format_columns_exportaciones_excel(df)  # Aplicar formato
        processed_sub_dict_empresas[sub_key] = df
    processed_data['EMPRESAS'] = processed_sub_dict_empresas

    # Copiar 'CONTEO EMPRESAS' sin cambios
    processed_data['CONTEO EMPRESAS'] = data_dict['CONTEO EMPRESAS']

    ###########
    # Inversión
    ###########
    
    # Colombia

    # Definir el diccionario para renombrar las columnas de categoría
    column_names_dict_inversion = {
        'IED ACTIVIDADES COLOMBIA': 'Actividad económica',
        'IED PAISES': 'País',
        'ICE PAISES': 'País',
        'IED TOTAL' : 'Agrupación de países',
        'ICE TOTAL' : 'Agrupación de países'
    }
    # Actividades solo es válida para Colombia
    if agrupacion == 'COLOMBIA':
        for key in ['IED ACTIVIDADES COLOMBIA']:
            sub_dict = data_dict[key]
            processed_sub_dict = {}
            for sub_key, df in sub_dict.items():
                # Renombrar la columna 'UNIDAD' según el diccionario de nombres de columnas de inversión
                if 'UNIDAD' in df.columns and key in column_names_dict_inversion:
                    df.rename(columns={'UNIDAD': column_names_dict_inversion[key]}, inplace=True)  
                if sub_key in ['ied_cerrado', 'ice_cerrado']:
                    df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{params_inversion['cerrado']['T_1']} (USD millones)",
                            'SUMA_INVERSION_T': f"{params_inversion['cerrado']['T']} (USD millones)",
                            'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {params_inversion['cerrado']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) {params_inversion['cerrado']['T']}"
                        }, inplace=True)
                elif sub_key in ['ied_corrido', 'ice_corrido']:
                        df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{transform_year_column_name(params_inversion['corrido']['T_1'])} (USD millones)",
                            'SUMA_INVERSION_T': f"{transform_year_column_name(params_inversion['corrido']['T'])} (USD millones)",
                            'DIFERENCIA_PORCENTUAL': 'Variación (%)',
                            'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params_inversion['corrido']['T'])}"
                        }, inplace=True)
                format_columns_inversion_excel(df)  # Aplicar formato
                processed_sub_dict[sub_key] = df
            processed_data[key] = processed_sub_dict
    

    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        for key in ['IED PAISES', 'ICE PAISES']:
            sub_dict = data_dict[key]
            processed_sub_dict = {}
            for sub_key, df in sub_dict.items():
                # Realizar la unión para reemplazar "UNIDAD" con "COUNTRY_OR_AREA_UNSD"
                df = df.merge(df_paises[['PAIS_INVERSION_BANREP', 'COUNTRY_OR_AREA_UNSD']],
                              left_on='UNIDAD',
                              right_on='PAIS_INVERSION_BANREP',
                              how='left')
                # Reemplazar valores en "UNIDAD"
                df['UNIDAD'] = df.apply(
                    lambda row: row['COUNTRY_OR_AREA_UNSD'] if pd.notnull(row['COUNTRY_OR_AREA_UNSD']) else row['UNIDAD'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['PAIS_INVERSION_BANREP', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)                              
                # Renombrar la columna 'UNIDAD' según el diccionario de nombres de columnas de inversión
                if 'UNIDAD' in df.columns and key in column_names_dict_inversion:
                    df.rename(columns={'UNIDAD': column_names_dict_inversion[key]}, inplace=True)  
                if sub_key in ['ied_cerrado', 'ice_cerrado']:
                    df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{params_inversion['cerrado']['T_1']} (USD millones)",
                            'SUMA_INVERSION_T': f"{params_inversion['cerrado']['T']} (USD millones)",
                            'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {params_inversion['cerrado']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) {params_inversion['cerrado']['T']}"
                        }, inplace=True)
                elif sub_key in ['ied_corrido', 'ice_corrido']:
                        df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{transform_year_column_name(params_inversion['corrido']['T_1'])} (USD millones)",
                            'SUMA_INVERSION_T': f"{transform_year_column_name(params_inversion['corrido']['T'])} (USD millones)",
                            'DIFERENCIA_PORCENTUAL': 'Variación (%)',
                            'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params_inversion['corrido']['T'])}"
                        }, inplace=True)
                format_columns_inversion_excel(df)  # Aplicar formato
                processed_sub_dict[sub_key] = df
            processed_data[key] = processed_sub_dict

    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        for key in ['IED TOTAL', 'ICE TOTAL']:
            sub_dict = data_dict[key]
            processed_sub_dict = {}
            for sub_key, df in sub_dict.items():
                if key == 'IED TOTAL':
                    df.iloc[0, df.columns.get_loc('UNIDAD')] = f"Total IED de {UNIDAD} en Colombia"
                if key == 'ICE TOTAL':
                    df.iloc[0, df.columns.get_loc('UNIDAD')] = f"Total ICE de Colombia en {UNIDAD}"
                if 'UNIDAD' in df.columns and key in column_names_dict_inversion:
                    # Renombrar la columna 'UNIDAD' según el diccionario de nombres de columnas de inversión
                    df.rename(columns={'UNIDAD': column_names_dict_inversion[key]}, inplace=True)  
                if sub_key in ['ied_cerrado_total', 'ice_cerrado_total']:
                    df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{params_inversion['cerrado']['T_1']} (USD millones)",
                            'SUMA_INVERSION_T': f"{params_inversion['cerrado']['T']} (USD millones)",
                            'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {params_inversion['cerrado']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) {params_inversion['cerrado']['T']}"
                        }, inplace=True)
                elif sub_key in ['ied_corrido_total', 'ice_corrido_total']:
                        df.rename(columns={
                            'SUMA_INVERSION_T_1': f"{transform_year_column_name(params_inversion['corrido']['T_1'])} (USD millones)",
                            'SUMA_INVERSION_T': f"{transform_year_column_name(params_inversion['corrido']['T'])} (USD millones)",
                            'DIFERENCIA_PORCENTUAL': 'Variación (%)',
                            'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params_inversion['corrido']['T'])}"
                        }, inplace=True)
                if agrupacion == 'PAISES':
                    df = df.iloc[[0], 1:]
                format_columns_inversion_excel(df)  # Aplicar formato
                processed_sub_dict[sub_key] = df
            processed_data[key] = processed_sub_dict
    #########
    # Turismo
    #########
    
    # Definir el diccionario para renombrar las columnas de categoría
    column_names_dict_turismo = {
        'PAIS_RESIDENCIA' : 'País de residencia', 
        'DPTO_HOSPEDAJE' : 'Departamento de hospedaje', 
        'CIUDAD_HOSPEDAJE' : 'Ciudad de hospedaje', 
        'DESCRIPCION_GENERO' : 'Género', 
        'MOVC_NOMBRE' : 'Motivo de viaje'
    }
    # Datos de cerrado
    for key in ['TURISMO CERRADO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar la columna principal según el diccionario de nombres de columnas
            if sub_key in column_names_dict_turismo:
                first_column_name = df.columns[0]
                df.rename(columns={first_column_name: column_names_dict_turismo[sub_key]}, inplace=True)
                df.rename(columns={
                            'SUMA_TURISMO_T_1': f"{params_turismo['cerrado']['T_1']}",
                            'SUMA_TURISMO_T': f"{params_turismo['cerrado']['T']}",
                            'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {params_turismo['cerrado']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) {params_turismo['cerrado']['T']}"
                        }, inplace=True)
            # Usar nombres de países
            if sub_key == 'PAIS_RESIDENCIA':
                df = df.merge(df_paises[['PAIS_CODIGO_TURISMO', 'COUNTRY_OR_AREA_UNSD']],
                              left_on='País de residencia',
                              right_on='PAIS_CODIGO_TURISMO',
                              how='left')
                # Reemplazar valores
                df['País de residencia'] = df.apply(
                    lambda row: row['COUNTRY_OR_AREA_UNSD'] if pd.notnull(row['COUNTRY_OR_AREA_UNSD']) else row['País de residencia'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['PAIS_CODIGO_TURISMO', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)

            # Usar nombres de departamentos
            if sub_key == 'DPTO_HOSPEDAJE':
                df = df.merge(df_departamentos[['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN']],
                              left_on='Departamento de hospedaje',
                              right_on='COD_DIAN_DEPARTAMENTO',
                              how='left')
                # Reemplazar valores en "Departamento de hospedaje"
                df['Departamento de hospedaje'] = df.apply(
                    lambda row: row['DEPARTAMENTO_DIAN'] if pd.notnull(row['DEPARTAMENTO_DIAN']) else row['Departamento de hospedaje'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN'], axis=1, inplace=True)

            # Usar nombres de ciudades    
            if sub_key == 'CIUDAD_HOSPEDAJE':
                df = df.merge(df_municipios[['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE']],
                              left_on='Ciudad de hospedaje',
                              right_on='COD_DANE_MUNICIPIO',
                              how='left')
                # Reemplazar valores en "Ciudad de hospedaje"
                df['Ciudad de hospedaje'] = df.apply(
                    lambda row: row['MUNICIPIO_DANE'] if pd.notnull(row['MUNICIPIO_DANE']) else row['Ciudad de hospedaje'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE'], axis=1, inplace=True)    
            # Cambiar numérico
            df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')
            format_columns_turismo_excel(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict
    # Datos de corrido
    for key in ['TURISMO CORRIDO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar la columna principal según el diccionario de nombres de columnas
            if sub_key in column_names_dict_turismo:
                first_column_name = df.columns[0]
                df.rename(columns={first_column_name: column_names_dict_turismo[sub_key]}, inplace=True)
                df.rename(columns={
                            'SUMA_TURISMO_T_1': f"Ene - {params_turismo['corrido']['T_MONTH_NAME']} {params_turismo['corrido']['T_1']}",
                            'SUMA_TURISMO_T': f"Ene - {params_turismo['corrido']['T_MONTH_NAME']} {params_turismo['corrido']['T']}",
                            'DIFERENCIA_ABSOLUTA' : 'Diferencia (turistas)',
                            'DIFERENCIA_PORCENTUAL': f"Variación (%) Ene - {params_turismo['corrido']['T_MONTH_NAME']} {params_turismo['corrido']['T']}",
                            'PARTICIPACION_T' : f"Participación (%) Ene - {params_turismo['corrido']['T_MONTH_NAME']} {params_turismo['corrido']['T']}"
                        }, inplace=True)
            # Usar nombres de países
            if sub_key == 'PAIS_RESIDENCIA':
                # Realizar la unión para reemplazar "País de residencia" con "COUNTRY_OR_AREA_UNSD"
                df = df.merge(df_paises[['PAIS_CODIGO_TURISMO', 'COUNTRY_OR_AREA_UNSD']],
                              left_on='País de residencia',
                              right_on='PAIS_CODIGO_TURISMO',
                              how='left')
                # Reemplazar valores en "UNIDAD"
                df['País de residencia'] = df.apply(
                    lambda row: row['COUNTRY_OR_AREA_UNSD'] if pd.notnull(row['COUNTRY_OR_AREA_UNSD']) else row['País de residencia'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['PAIS_CODIGO_TURISMO', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)
            # Usar nombres de departamentos
            if sub_key == 'DPTO_HOSPEDAJE':
                df = df.merge(df_departamentos[['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN']],
                              left_on='Departamento de hospedaje',
                              right_on='COD_DIAN_DEPARTAMENTO',
                              how='left')
                # Reemplazar valores en "Departamento de hospedaje"
                df['Departamento de hospedaje'] = df.apply(
                    lambda row: row['DEPARTAMENTO_DIAN'] if pd.notnull(row['DEPARTAMENTO_DIAN']) else row['Departamento de hospedaje'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN'], axis=1, inplace=True)
            # Usar nombres de ciudades    
            if sub_key == 'CIUDAD_HOSPEDAJE':
                df = df.merge(df_municipios[['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE']],
                              left_on='Ciudad de hospedaje',
                              right_on='COD_DANE_MUNICIPIO',
                              how='left')
                # Reemplazar valores en "Ciudad de hospedaje"
                df['Ciudad de hospedaje'] = df.apply(
                    lambda row: row['MUNICIPIO_DANE'] if pd.notnull(row['MUNICIPIO_DANE']) else row['Ciudad de hospedaje'], axis=1)
                # Eliminar columnas innecesarias después de la unión
                df.drop(['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE'], axis=1, inplace=True)
            # Cambiar numérico
            df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')
            format_columns_turismo_excel(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict
    
    # PESO POR MEDIOS DE TRANSPORTE
    for key in ['MEDIOS PESO MINERO', 'MEDIOS PESO NO MINERO']:
        sub_dict = data_dict[key]
        processed_sub_dict = {}
        for sub_key, df in sub_dict.items():
            # Renombrar la columna 'CATEGORIA' a 'Medio de transporte'
            if 'CATEGORIA' in df.columns:
                df.rename(columns={'CATEGORIA': 'Medio de transporte'}, inplace=True)
            # Renombrar las columnas relacionadas con peso y convertir los valores a toneladas
            if 'SUMA_PESO_T_1' in df.columns and 'SUMA_PESO_T' in df.columns:
                df['SUMA_PESO_T_1'] = df['SUMA_PESO_T_1'] / 1000  # Convertir a toneladas
                df['SUMA_PESO_T'] = df['SUMA_PESO_T'] / 1000  # Convertir a toneladas
                if sub_key == 'ST_CATEGORIAS_PESO_CERRADO':
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{params['cerrado']['T_1']} (TONELADAS)",
                        'SUMA_PESO_T': f"{params['cerrado']['T']} (TONELADAS)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}",
                        'PARTICIPACION_T_1' : f"Participación (%) {transform_year_column_name(params['cerrado']['T_1'])}"
                    }, inplace=True)
                elif sub_key == 'ST_CATEGORIAS_PESO_CORRIDO':
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (TONELADAS)",
                        'SUMA_PESO_T': f"{transform_year_column_name(params['corrido']['T'])} (TONELADAS)",
                        'PARTICIPACION_T' : f"Participación (%) {transform_year_column_name(params['corrido']['T'])}",
                        'PARTICIPACION_T_1' : f"Participación (%) {transform_year_column_name(params['corrido']['T_1'])}"
                    }, inplace=True)
            format_columns_exportaciones_excel(df)  # Aplicar formato
            processed_sub_dict[sub_key] = df
        processed_data[key] = processed_sub_dict

    return processed_data


def guardar_tablas_en_excel(session, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral, file_path):
    """
    Guarda todas las tablas obtenidas de la función get_data en un archivo de Excel, 
    con cada tabla en una pestaña separada, usando un mapeo para nombres de pestañas específicos.

    Parameters:
    session (object): Sesión de base de datos.
    agrupacion (str): Agrupación para los datos.
    continentes (list): Lista de continentes.
    paises (list): Lista de países.
    hubs (list): Lista de hubs.
    tlcs (list): Lista de TLCs.
    departamentos (list): Lista de departamentos.
    umbral (list): Umbral para los datos.
    file_path (str): Ruta del archivo de Excel donde se guardarán las tablas.
    """
    # Obtener los datos usando la función get_data
    data_dict = process_data_excel(session, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)
    
    # Diccionario de mapeo para nombres de pestañas específicos
    sheet_name_mapping = {
        # EXPORTACIONES TOTALES
        ('TOTALES', 'ST_CATEGORIAS_CERRADO'): 'EXPO_TOTAL_CERRADO',
        ('TOTALES', 'ST_CATEGORIAS_CORRIDO'): 'EXPO_TOTAL_CORRIDO',
        # EXPORTACIONES TOTALES POR TIPO
        ('TIPOS', 'ST_CATEGORIAS_CERRADO'): 'EXPO_TIPOS_CERRADO',
        ('TIPOS', 'ST_CATEGORIAS_CORRIDO'): 'EXPO_TIPOS_CORRIDO',
        # EXPORTACIONES NME POR VARIABLE AÑO CERRADO
        ('CATEGORIAS CERRADO', 'CONTINENTE'): 'EXPO_CONTINENTE_CERRADO',
        ('CATEGORIAS CERRADO', 'DEPARTAMENTOS'): 'EXPO_DPTO_CERRADO',
        ('CATEGORIAS CERRADO', 'HUBS'): 'EXPO_HUB_CERRADO',
        ('CATEGORIAS CERRADO', 'PAIS'): 'EXPO_PAIS_CERRADO',
        ('CATEGORIAS CERRADO', 'SECTORES'): 'EXPO_SECTOR_CERRADO',
        ('CATEGORIAS CERRADO', 'SUBSECTORES'): 'EXPO_SUBSECTOR_CERRADO',
        ('CATEGORIAS CERRADO', 'TLCS'): 'EXPO_TLC_CERRADO',
        # EXPORTACIONES NME POR VARIABLE AÑO CORRIDO
        ('CATEGORIAS CORRIDO', 'CONTINENTE'): 'EXPO_CONTINENTE_CORRIDO',
        ('CATEGORIAS CORRIDO', 'DEPARTAMENTOS'): 'EXPO_DPTO_CORRIDO',
        ('CATEGORIAS CORRIDO', 'HUBS'): 'EXPO_HUB_CORRIDO',
        ('CATEGORIAS CORRIDO', 'PAIS'): 'EXPO_PAIS_CORRIDO',
        ('CATEGORIAS CORRIDO', 'SECTORES'): 'EXPO_SECTOR_CORRIDO',
        ('CATEGORIAS CORRIDO', 'SUBSECTORES'): 'EXPO_SUBSECTOR_CORRIDO',
        ('CATEGORIAS CORRIDO', 'TLCS'): 'EXPO_TLC_CORRIDO',
        # DATOS DE EMPRESAS NME POR AÑO CERRADO
        ('EMPRESAS', 'ST_NIT_CERRADO'): 'NIT_CERRADO',
        # DATOS DE EMPRESAS NME POR AÑO CORRIDO
        ('EMPRESAS', 'ST_NIT_CORRIDO'): 'NIT_CORRIDO',
        # CONTEO DE EMPRESAS AÑO CERRADO
        ('CONTEO EMPRESAS', 'CERRADO'): 'CONTEO_CERRADO',
        # CONTEO DE EMPRESAS AÑO CORRIDO
        ('CONTEO EMPRESAS', 'CORRIDO'): 'CONTEO_CORRIDO',
        # EXPORTACIONES TOTALES POR PESO AÑO CERRADO
        ('TOTALES PESO', 'ST_CATEGORIAS_PESO_CERRADO'): 'EXPO_PESO_CERRADO',
        # EXPORTACIONES TOTALES POR PESO AÑO CORRIDO
        ('TOTALES PESO', 'ST_CATEGORIAS_PESO_CORRIDO'): 'EXPO_PESO_CORRIDO',
        # EXPORTACIONES POR TIPO POR PESO AÑO CERRADO
        ('TIPOS PESO', 'ST_CATEGORIAS_PESO_CERRADO'): 'EXPO_TIPOS_PESO_CERRADO',
        # EXPORTACIONES POR TIPO POR PESO AÑO CORRIDO
        ('TIPOS PESO', 'ST_CATEGORIAS_PESO_CORRIDO'): 'EXPO_TIPOS_PESO_CORRIDO',
        # IED ACTIVIDADES AÑO CERRADO
        ('IED ACTIVIDADES COLOMBIA', 'ied_cerrado'): 'IED_ACTIVIDADES_CERRADO',
        # IED ACTIVIDADES AÑO CORRIDO
        ('IED ACTIVIDADES COLOMBIA', 'ied_corrido'): 'IED_ACTIVIDADES_CORRIDO',
        # IED PAISES AÑO CERRADO
        ('IED PAISES', 'ied_cerrado'): 'IED_PAISES_CERRADO',
        # IED PAISES AÑO CORRIDO
        ('IED PAISES', 'ied_corrido'): 'IED_PAISES_CORRIDO',
        # IED TOTAL AÑO CERRADO
        ('IED TOTAL', ''): 'IED_TOTAL',
        # ICE PAISES AÑO CERRADO
        ('ICE PAISES', 'ice_cerrado'): 'ICE_PAISES_CERRADO',
        # ICE PAISES AÑO CORRIDO
        ('ICE PAISES', 'ice_corrido'): 'ICE_PAISES_CERRADO',
        # ICE TOTAL
        ('ICE TOTAL', ''): 'ICE_TOTAL',
        # TURISMO PAISES AÑO CERRADO
        ('TURISMO CERRADO', 'PAIS_RESIDENCIA'): 'TURISMO_PAIS_CERRADO',
        # TURISMO DEPARTAMENTOS AÑO CERRADO
        ('TURISMO CERRADO', 'DPTO_HOSPEDAJE'): 'TURISMO_DPTO_CERRADO',
        # TURISMO CIUDAD AÑO CERRADO
        ('TURISMO CERRADO', 'CIUDAD_HOSPEDAJE'): 'TURISMO_MUN_CERRADO',
        # TURISMO GENERO AÑO CERRADO
        ('TURISMO CERRADO', 'DESCRIPCION_GENERO'): 'TURISMO_GEN_CERRADO',
        # TURISMO MOTIVO AÑO CERRADO
        ('TURISMO CERRADO', 'MOVC_NOMBRE'): 'TURISMO_MOV_CERRADO',
        # TURISMO PAISES AÑO CORRIDO
        ('TURISMO CORRIDO', 'PAIS_RESIDENCIA'): 'TURISMO_PAIS_CORRIDO',
        # TURISMO DEPARTAMENTOS AÑO DEPARTAMENTO
        ('TURISMO CORRIDO', 'DPTO_HOSPEDAJE'): 'TURISMO_DPTO_CORRIDO',
        # TURISMO CIUDAD AÑO CORRIDO
        ('TURISMO CORRIDO', 'CIUDAD_HOSPEDAJE'): 'TURISMO_MUN_CORRIDO',
        # TURISMO GENERO AÑO CORRIDO
        ('TURISMO CORRIDO', 'DESCRIPCION_GENERO'): 'TURISMO_GEN_CORRIDO',
        # TURISMO MOTIVO AÑO CORRIDO
        ('TURISMO CORRIDO', 'MOVC_NOMBRE'): 'TURISMO_MOV_CORRIDO',
        # EXPORTACIONES TOTALES POR PESO POR MEDIO DE TRANSPORTE AÑO CERRADO
        ('MEDIOS PESO MINERO', 'ST_CATEGORIAS_PESO_CERRADO'): 'EXPO_MEDIOS_MINERO_PESO_CERRADO',
        ('MEDIOS PESO NO MINERO', 'ST_CATEGORIAS_PESO_CERRADO'): 'EXPO_MEDIOS_NME_PESO_CERRADO',
        # EXPORTACIONES TOTALES POR PESO POR MEDIO DE TRANSPORTE AÑO CORRIDO
        ('MEDIOS PESO MINERO', 'ST_CATEGORIAS_PESO_CORRIDO'): 'EXPO_MEDIOS_MINERO_PESO_CORRIDO',
        ('MEDIOS PESO NO MINERO', 'ST_CATEGORIAS_PESO_CORRIDO'): 'EXPO_MEDIOS_MINERO_PESO_CORRIDO',
    }
    # Definir el orden deseado para las llaves y subllaves
    orden_deseado = [
        # VALORES TOTALES DE EXPORTACIONES
        ('TOTALES', 'ST_CATEGORIAS_CERRADO'),
        ('TOTALES', 'ST_CATEGORIAS_CORRIDO'),
        ('TIPOS', 'ST_CATEGORIAS_CERRADO'),
        ('TIPOS', 'ST_CATEGORIAS_CORRIDO'),
        ('TOTALES PESO', 'ST_CATEGORIAS_PESO_CERRADO'),
        ('TOTALES PESO', 'ST_CATEGORIAS_PESO_CORRIDO'),
        ('TIPOS PESO', 'ST_CATEGORIAS_PESO_CERRADO'),
        ('TIPOS PESO', 'ST_CATEGORIAS_PESO_CORRIDO'),
        ('MEDIOS PESO MINERO', 'ST_CATEGORIAS_PESO_CERRADO'),
        ('MEDIOS PESO NO MINERO', 'ST_CATEGORIAS_PESO_CERRADO'),
        ('MEDIOS PESO MINERO', 'ST_CATEGORIAS_PESO_CORRIDO'),
        ('MEDIOS PESO NO MINERO', 'ST_CATEGORIAS_PESO_CORRIDO'),
        # CATEGORIAS CERRADO
        ('CATEGORIAS CERRADO', 'CONTINENTE'),
        ('CATEGORIAS CERRADO', 'DEPARTAMENTOS'),
        ('CATEGORIAS CERRADO', 'HUBS'),
        ('CATEGORIAS CERRADO', 'PAIS'),
        ('CATEGORIAS CERRADO', 'SECTORES'),
        ('CATEGORIAS CERRADO', 'SUBSECTORES'),
        ('CATEGORIAS CERRADO', 'TLCS'),
        # CATEGORIAS CORRIDO
        ('CATEGORIAS CORRIDO', 'CONTINENTE'),
        ('CATEGORIAS CORRIDO', 'DEPARTAMENTOS'),
        ('CATEGORIAS CORRIDO', 'HUBS'),
        ('CATEGORIAS CORRIDO', 'PAIS'),
        ('CATEGORIAS CORRIDO', 'SECTORES'),
        ('CATEGORIAS CORRIDO', 'SUBSECTORES'),
        ('CATEGORIAS CORRIDO', 'TLCS'),
        # DATOS DE EMPRESAS
        ('EMPRESAS', 'ST_NIT_CERRADO'),
        ('EMPRESAS', 'ST_NIT_CORRIDO'),
        # CONTEO DE EMPRESAS
        ('CONTEO EMPRESAS', 'CERRADO'),
        ('CONTEO EMPRESAS', 'CORRIDO'),
        # IED TOTAL
        ('IED TOTAL', ''),
        # IED ACTIVIDADES
        ('IED ACTIVIDADES COLOMBIA', 'ied_cerrado'),
        ('IED ACTIVIDADES COLOMBIA', 'ied_corrido'),
        # IED PAISES
        ('IED PAISES', 'ied_cerrado'),
        ('IED PAISES', 'ied_corrido'),
        # ICE TOTAL
        ('ICE TOTAL', ''),
        ('ICE PAISES', 'ice_cerrado'),
        ('ICE PAISES', 'ice_corrido'),
        # TURISMO CERRADO
        ('TURISMO CERRADO', 'PAIS_RESIDENCIA'),
        ('TURISMO CERRADO', 'DPTO_HOSPEDAJE'),
        ('TURISMO CERRADO', 'CIUDAD_HOSPEDAJE'),
        ('TURISMO CERRADO', 'DESCRIPCION_GENERO'),
        ('TURISMO CERRADO', 'MOVC_NOMBRE'),
        # TURISMO CORRIDO
        ('TURISMO CORRIDO', 'PAIS_RESIDENCIA'),
        ('TURISMO CORRIDO', 'DPTO_HOSPEDAJE'),
        ('TURISMO CORRIDO', 'CIUDAD_HOSPEDAJE'),
        ('TURISMO CORRIDO', 'DESCRIPCION_GENERO'),
        ('TURISMO CORRIDO', 'MOVC_NOMBRE')
    ]

    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        for key, sub_key in orden_deseado:
            if key in ['RESUMEN', 'OPORTUNIDADES', 'CONECTIVIDAD']:
                continue
            if key in data_dict and sub_key in data_dict[key]:
                df = data_dict[key][sub_key]
                # Asegurarse de que el DataFrame no esté vacío
                if not df.empty:
                    # Buscar el nombre de la pestaña en el diccionario de mapeo
                    sheet_name = sheet_name_mapping.get((key, sub_key), f"{key}_{sub_key}")
                    # Limitar el nombre de la pestaña a 31 caracteres (límite de Excel)
                    sheet_name = sheet_name[:31]
                    # Guardar el DataFrame en la pestaña correspondiente
                    df.to_excel(writer, sheet_name=sheet_name, index=False)



