# Librerias
# Datos
import datos as dat
# Documentos 
import documentos as doc
# Conversión
import io
import base64
# Streamlit
import streamlit as st


# Función para insertar datos en la tabla de seguimiento
def registrar_evento(sesion_activa, tipo_evento, detalle_evento, unidad):
    """
    Registra un evento en la base de datos Snowflake.

    Args:
    - sesion_activa: Sesión activa de conexión a la base de datos.
    - tipo_evento (str): Tipo de evento ('selección' o 'descarga').
    - detalle_evento (str): Detalle de evento ('selección continente', 'selección país', etc)
    - unidad (str): Unidad específica del evento (e.g., 'América', 'Colombia').
    """
    # Crear objeto de conexión
    conn = sesion_activa.connection
    try:
        # Crear consulta para el insert
        query_insert = f"""
        INSERT INTO DOCUMENTOS_COLOMBIA.SEGUIMIENTO.SEGUIMIENTO_EVENTOS (TIPO_EVENTO, DETALLE_EVENTO, UNIDAD, FECHA_HORA) 
        VALUES ('{tipo_evento}', '{detalle_evento}', '{unidad}', CONVERT_TIMEZONE('America/Los_Angeles', 'America/Bogota', CURRENT_TIMESTAMP));
        """
        # Crear un cursor para ejecutar la consulta
        cur = conn.cursor()
        try:
            # Ejecutar la consulta SQL con los valores
            cur.execute(query_insert)
        finally:
            # Cerrar el cursor
            cur.close()
    # Error
    except Exception as e:
        st.write(f"Error al registrar evento: {e}")

    
# Función para generar archivos sin generar botón de descarga
@st.cache_data(show_spinner=False)
def generar_documentos(agrupacion, _sesion_activa, continentes=None, paises=None, hubs=None, tlcs=None, departamentos=None, umbral=[10000], header_image_left=None, footer_image=None):
    
    """
    Genera documentos Word y Excel para la agrupación seleccionada y los pone disponibles para descarga.

    Args:
    - agrupacion (str): Tipo de agrupación para el informe (e.g., 'CONTINENTES', 'PAISES', 'HUBS', 'TLCS', 'DEPARTAMENTOS', 'COLOMBIA').
    - _sesion_activa: Sesión activa de conexión a la base de datos (OBJETO NO HASHEABLE POR ELLO SE PONE _ AL INICIO).
    - continentes (tuple, optional): Tupla de continentes seleccionados. Default es None.
    - paises (tuple, optional): Tupla de países seleccionados. Default es None.
    - hubs (tuple, optional): Tupla de HUBs seleccionados. Default es None.
    - tlcs (tuple, optional): Tupla de TLCs seleccionados. Default es None.
    - departamentos (tuple, optional): Tupla de departamentos seleccionados. Default es None.
    - umbral (tuple, optional): Tupla de umbrales de valores. Default es (10000,).
    - header_image_left (str, optional): Ruta a la imagen del encabezado izquierdo. Default es None.
    - header_image_right (str, optional): Ruta a la imagen del encabezado derecho. Default es None.
    - footer_image (str, optional): Ruta a la imagen del pie de página. Default es None.
    """

    # Convertir tuplas a listas, o definir como None si no se proporcionan valores
    continentes = list(continentes) if continentes else None
    paises = list(paises) if paises else None
    hubs = list(hubs) if hubs else None
    tlcs = list(tlcs) if tlcs else None
    departamentos = list(departamentos) if departamentos else None
    umbral = list(umbral) if umbral else None

    # Mostrar barra de progreso y spinner
    progress_bar = st.progress(0)
    with st.spinner('Generando el documento, por favor espere...'):
        try:
            # Obtener parámetros de datos
            geo_params = dat.get_data_parametros(_sesion_activa, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)
            # Actualizar progreso
            progress_bar.progress(5, text="Parámetros identificados correctamente.")
            
            # Procesar datos
            tables = dat.process_data(_sesion_activa, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)
            progress_bar.progress(50, text="Datos extraidos y transformados correctamente.")

            # Determinar los nombres de los archivos
            if agrupacion == 'COLOMBIA':
                file_name_suffix = 'Colombia'
            else:
                entity_name = (continentes[0] if continentes else
                               paises[0] if paises else
                               hubs[0] if hubs else
                               tlcs[0] if tlcs else
                               departamentos[0])
                file_name_suffix = f"{agrupacion} - {entity_name}"

            # Filepath para generar archivos
            file_path_docx = f"output/Tres Ejes {file_name_suffix}.docx"
            file_path_xlsx = f"output/Tres Ejes {file_name_suffix}.xlsx"

            # Generar el documento Word y registrar evento de selección en la base de datos
            if agrupacion == 'CONTINENTES':
                doc.create_document_continentes(tablas=tables, file_path=file_path_docx, titulo=continentes[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de continente', unidad=continentes[0])
            elif agrupacion == 'PAISES':
                doc.create_document_paises(tablas=tables, file_path=file_path_docx, titulo=paises[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de país', unidad=paises[0])
            elif agrupacion == 'HUBS':
                doc.create_document_hubs(tablas=tables, file_path=file_path_docx, titulo=hubs[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de HUB', unidad=hubs[0])   
            elif agrupacion == 'TLCS':
                doc.create_document_tlcs(tablas=tables, file_path=file_path_docx, titulo=tlcs[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de TLC', unidad=tlcs[0])
            elif agrupacion == 'DEPARTAMENTOS':
                doc.create_document_departamentos(tablas=tables, file_path=file_path_docx, titulo=departamentos[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de departamento', unidad=departamentos[0])
            elif agrupacion == 'COLOMBIA':
                doc.create_document_colombia(tablas=tables, file_path=file_path_docx, header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de Colombia', unidad='Colombia')
            else:
                raise ValueError("Agrupación no reconocida")

            # Crear el archivo Excel utilizando la función original
            dat.guardar_tablas_en_excel(session=_sesion_activa, agrupacion=agrupacion, continentes=continentes, paises=paises, hubs=hubs, tlcs=tlcs, departamentos=departamentos, umbral=umbral, file_path=file_path_xlsx)
            progress_bar.progress(75, text="Documento creado con exito.")

            # Preparar los archivos para descarga
            # DOCX
            with open(file_path_docx, 'rb') as f:
                doc_bytes = io.BytesIO(f.read())
            # EXCEL
            with open(file_path_xlsx, 'rb') as f:
                excel_bytes = io.BytesIO(f.read())

            # Codificar los archivos en base64
            # DOCX
            b64_docx = base64.b64encode(doc_bytes.getvalue()).decode()
            # EXCEL
            b64_xlsx = base64.b64encode(excel_bytes.getvalue()).decode()

            # Modificar nombres y dejarlos sin output/ para un nombre más corto
            file_path_docx = f"Tres Ejes {file_name_suffix}.docx"
            file_path_xlsx = f"Tres Ejes {file_name_suffix}.xlsx"

            # Actualizar progreso al 100%
            progress_bar.progress(100, text="Proceso terminado")
            
            # Mostrar mensaje de éxito cuando los documentos están listos
            st.success("El documento ha sido generado exitosamente. Puede descargarlo a continuación:")
            
        except Exception as e:
            # Mostrar mensaje de error en caso de excepción
            st.error(f"Se produjo un error durante la generación del documento: {e}")
        finally:
            # Finalizar barra de progreso
            progress_bar.empty()
    
    # Return
    return b64_docx, b64_xlsx, file_path_docx, file_path_xlsx



# Función para crear los botones de descarga
def botones_descarga_word_xlsx(b64_docx, b64_xlsx, file_path_docx, file_path_xlsx, agrupacion, _sesion_activa, unidad):

    """
    Genera botones de descarga para documentos en formatos Word y Excel, con eventos de registro.

    Args:
    - b64_docx (str): Documento en formato Word codificado en base64.
    - b64_xlsx (str): Documento en formato Excel codificado en base64.
    - file_path_docx (str): Nombre del archivo Word para la descarga.
    - file_path_xlsx (str): Nombre del archivo Excel para la descarga.
    - agrupacion (str): Tipo de agrupación para el informe (e.g., 'CONTINENTES', 'PAISES', 'HUBS', 'TLCS', 'DEPARTAMENTOS', 'COLOMBIA').
    - _sesion_activa: Sesión activa de conexión a la base de datos.
    - unidad (tuple or list): Unidad seleccionada para el evento (e.g., continente, país, HUB).

    Raises:
    - ValueError: Si la agrupación no es reconocida.

    Returns:
    - None: La función genera botones de descarga directamente en la interfaz de usuario de Streamlit.
    """

    # Creación de detalles de eventos:
    # Parte común 
    descripcion_evento_word = 'Descarga Word de '
    descripcion_evento_excel = 'Descarga Excel de '
    # Agregar final según agrupación
    if agrupacion == 'CONTINENTES':
        descripcion_evento_word += 'continente'
        descripcion_evento_excel += 'continente'
    elif agrupacion == 'PAISES':
        descripcion_evento_word += 'país'
        descripcion_evento_excel += 'país'
    elif agrupacion == 'HUBS':
        descripcion_evento_word += 'HUB'
        descripcion_evento_excel += 'HUB'
    elif agrupacion == 'TLCS':
        descripcion_evento_word += 'TLC'
        descripcion_evento_excel += 'TLC'
    elif agrupacion == 'DEPARTAMENTOS':
        descripcion_evento_word += 'departamento'
        descripcion_evento_excel += 'departamento'
    elif agrupacion == 'COLOMBIA':
        descripcion_evento_word += 'Colombia'
        descripcion_evento_excel += 'Colombia'
    else:
        raise ValueError("Agrupación no reconocida")
    
    # Agregar botones de descarga con widgets básicos de streamlit
    # Convertir tuplas a listas
    unidad_evento = str(unidad[0]) if isinstance(unidad, tuple) else str(unidad) if unidad else None
    # WORD
    st.download_button(label='Descargar el documento en Microsoft Word', data=base64.b64decode(b64_docx), 
                    file_name=file_path_docx, help='Presione el botón para descargar el archivo Word', 
                    mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document', 
                    on_click=lambda: registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Descarga', detalle_evento=descripcion_evento_word, unidad=unidad_evento),
                    type='secondary',
                    use_container_width=True)
    # EXCEL
    st.download_button(label='Presione el botón para descargar el archivo Excel', data=base64.b64decode(b64_xlsx), 
                    file_name=file_path_xlsx, help='Presione el botón para descargar el archivo Excel', 
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
                    on_click=lambda: registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Descarga', detalle_evento=descripcion_evento_excel, unidad=unidad_evento),
                    type='secondary',
                    use_container_width=True)


